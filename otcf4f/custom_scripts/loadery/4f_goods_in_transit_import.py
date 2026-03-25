"""
================================================================================
SKRYPT: Import i integracja towaru w drodze (Expected Delivery) 4F do Occubee
Autor:  Patryk Bubula / Occubee Data Engineering
================================================================================

OPIS
----
Skrypt integruje dwa niezależne źródła zasilające strukturę "Expected Delivery"
w platformie Occubee: otwarte zamówienia zakupowe (open_orders) oraz towar
faktycznie będący w transporcie (goods_in_transit). Oba zbiory mapowane są na
jednolity schemat, łączone i filtrowane przed zapisem do kontenera bigdata.

METODA IMPORTU: CALOSCIOWO (overwrite, NIE delta!)
Oba pliki źródłowe są NADPISYWANE codziennie przez OTCF — każdy import zawiera
pełny obraz sytuacji, nie przyrostowe zmiany. Occubee traktuje każdy import
jako pełny snapshot — poprzednie dane o towarze w drodze są zastępowane nowymi.

CHARAKTERYSTYKA PLIKÓW ŹRÓDŁOWYCH
-----------------------------------
  goods_in_transit.csv — towar FIZYCZNIE w transporcie z ostatnich ~30 dni.
    Zawiera pozycje, które zostały już wysłane i są w drodze do magazynów/sklepów.
    OTCF nadpisuje ten plik codziennie pełnym snapshotem.

  open_orders.csv — PLANOWANE przyszłe zamówienia zakupowe.
    Zawiera pozycje, które zostały zamówione ale jeszcze nie wysłane.
    OTCF nadpisuje ten plik codziennie pełnym snapshotem.

Oba pliki razem dają kompletny obraz oczekiwanych dostaw: to co jest w drodze
+ to co jest zamówione (ale jeszcze nie wysłane).

ŹRÓDŁO DANYCH (INPUT)
---------------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net

  open_orders.csv  (sep=|)
    Otwarte zamówienia zakupowe. Kolumny używane: destination_code,
    product_code, quantity_ordered, estimated_arrival_date.

  goods_in_transit.csv  (sep=|)
    Towar w drodze (fizycznie w transporcie). Kolumny używane:
    destination_code, product_code, quantity, estimated_arrival_date.

LOGIKA BIZNESOWA
----------------
1. Walidacja danych wejściowych:
   V1: Pusty plik (dowolne źródło) → CRITICAL ERROR, przerwanie skryptu.
   V2: Brak wymaganych kolumn → CRITICAL ERROR, przerwanie skryptu.
   V3: NULL w destination_code lub product_code → WARN + odrzucenie wierszy.

2. Mapowanie na wspólną strukturę Occubee (Expected Delivery):
   Oba źródła mają różne nazwy kolumn ilościowych (quantity_ordered vs
   quantity) — mapowane do wspólnego pola `amount`. Stała wartość
   unit_of_measure_code = "ST" dodawana do obu zbiorów.

3. Łączenie źródeł (unionByName):
   Po zmapowaniu kolumn oba zbiory łączone są w jeden dataset przy użyciu
   unionByName, co zapewnia spójność schematów niezależnie od kolejności kolumn.

4. Twardy filtr — odrzucenie rekordów bez daty dostawy:
   Rekordy z NULL w polu estimated_arrival_date są odrzucane — Occubee
   nie może zaplanować dostaw bez daty. Jest to decyzja biznesowa: rekordy
   zostaną załadowane przy kolejnym zrzucie danych, gdy data zostanie uzupełniona.

5. Audyt wolumenowy:
   Po filtrowaniu do logu trafiają: łączna liczba wierszy, liczba odrzuconych
   (brak daty) oraz liczba wierszy gotowych do zapisu. Ostrzeżenie (level=warn)
   jeśli liczba odrzuconych > 0.

6. Wymuszanie typów (Casting):
   amount → Decimal(15, 3), delivery_date → DateType.

WYJŚCIE (OUTPUT)
----------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net
  TRANSFORMED_DATA/TOWAR_W_DRODZE/goods_in_transit.csv         — pojedynczy plik CSV
  TRANSFORMED_DATA/TOWAR_W_DRODZE/goods_in_transit_SAMPLE.csv — próbka 1000 wierszy
Format: CSV, sep=;, encoding=UTF-8, quoteAll=false, dateFormat=yyyy-MM-dd

URUCHOMIENIE
------------
/opt/spark3/bin/spark-submit 4f_goods_in_transit_import.py \\
  -s otcf4fprodmainsa.dfs.core.windows.net \\
  -k <OCCUBEE_KEY> -c bigdata \\
  -p <process_instance_id> \\
  -v spark_script_configs/<config>.json
================================================================================
"""

import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from pyspark.sql import DataFrame

from occubee.common import (GatherArguments, Configuration, LibHelpersFactory,
                            SparkWrapper, AzureHelper, SparkHelper, Logger)
from lib.parameters import Parameters
from lib.spark_profiles import apply_profile


class GoodsInTransitImport:
    # ==========================================================================
    # TRYB ZAPISU: "TEST" (próbka 1000 wierszy), "FULL" (całość), "BOTH" (oba)
    # ==========================================================================
    OUTPUT_MODE = "FULL"

    ORDER = [
        "point_of_sale_code", "product_code", "amount",
        "unit_of_measure_code", "delivery_date"
    ]

    CASTS = {
        "point_of_sale_code": StringType(),
        "product_code": StringType(),
        "amount": DecimalType(15, 3),
        "unit_of_measure_code": StringType(),
        "delivery_date": DateType()
    }

    def __init__(self, spark_helper: SparkHelper, azure_helper: AzureHelper, client_azure_helper: AzureHelper,
                 script_cfg: Configuration, logger: Logger):
        self.__spark_helper = spark_helper
        self.__azure_helper = azure_helper
        self.__client_azure_helper = client_azure_helper
        self.__script_cfg = script_cfg
        self.__logger = logger
        self.spark = spark_helper.spark

    def _read_csv(self, path: str) -> DataFrame:
        """Wczytuje plik CSV z separatorem '|' i standaryzuje nazwy kolumn."""
        df = self.spark.read.option("header", "true").option("sep", "|").csv(path)
        cleaned_columns = [c.strip().lower().encode('ascii', 'ignore').decode('ascii') for c in df.columns]
        return df.toDF(*cleaned_columns)

    REQUIRED_COLS_ORDERS = ["destination_code", "product_code", "quantity_ordered", "estimated_arrival_date"]
    REQUIRED_COLS_TRANSIT = ["destination_code", "product_code", "quantity", "estimated_arrival_date"]

    def load_raw_data(self) -> tuple:
        """Krok 1: Wczytywanie obu źródeł danych z Azure."""
        self.__logger.log("Wczytywanie pliku open_orders.csv...")
        path_orders = self.__client_azure_helper.create_azure_path(
            folder_path=self.__script_cfg['input']['raw_open_orders']
        )
        df_orders = self._read_csv(path_orders)

        self.__logger.log("Wczytywanie pliku goods_in_transit.csv...")
        path_transit = self.__client_azure_helper.create_azure_path(
            folder_path=self.__script_cfg['input']['raw_goods_in_transit']
        )
        df_transit = self._read_csv(path_transit)

        return df_orders, df_transit

    def validate_source(self, df: DataFrame, source_name: str, required_cols: list):
        """Walidacja pojedynczego pliku zrodlowego: V1 pusty plik, V2 brak kolumn."""
        # --- V1: Pusty plik ---
        row_count = df.count()
        self.__logger.log(f"Plik {source_name}: {row_count:,} wierszy.")
        if row_count == 0:
            self.__logger.log(
                f"CRITICAL: Plik {source_name} jest PUSTY (0 wierszy). Przerywanie importu.",
                level="error"
            )
            sys.exit(1)

        # --- V2: Brak wymaganych kolumn ---
        missing_cols = set(required_cols) - set(df.columns)
        if missing_cols:
            self.__logger.log(
                f"CRITICAL: Plik {source_name} — brak wymaganych kolumn: {sorted(missing_cols)}. "
                f"Dostepne kolumny: {sorted(df.columns)}. Przerywanie importu.",
                level="error"
            )
            sys.exit(1)

    def transform_and_union(self, df_orders: DataFrame, df_transit: DataFrame) -> DataFrame:
        """Krok 2: Mapowanie obu źródeł, union i filtrowanie braków daty."""
        self.__logger.log("Mapowanie kolumn open_orders na strukturę Occubee...")
        df_orders_mapped = df_orders.select(
            F.trim(F.col("destination_code")).alias("point_of_sale_code"),
            F.trim(F.col("product_code")).cast("string").alias("product_code"),
            F.regexp_replace(F.col("quantity_ordered"), ",", ".").cast("double").alias("amount"),
            F.lit("ST").alias("unit_of_measure_code"),
            F.to_date(F.col("estimated_arrival_date")).alias("delivery_date")
        )

        self.__logger.log("Mapowanie kolumn goods_in_transit na strukturę Occubee...")
        df_transit_mapped = df_transit.select(
            F.trim(F.col("destination_code")).alias("point_of_sale_code"),
            F.trim(F.col("product_code")).cast("string").alias("product_code"),
            F.regexp_replace(F.col("quantity"), ",", ".").cast("double").alias("amount"),
            F.lit("ST").alias("unit_of_measure_code"),
            F.to_date(F.col("estimated_arrival_date")).alias("delivery_date")
        )

        self.__logger.log("Łączenie zbiorów i audyt wolumenowy...")
        df_combined_all = df_orders_mapped.unionByName(df_transit_mapped)
        df_combined_all.cache()
        total_raw_rows = df_combined_all.count()
        self.__logger.log(f"Wiersze po polaczeniu (union): {total_raw_rows:,}")

        # --- V3: NULL w destination_code lub product_code → WARN + drop ---
        null_key_count = df_combined_all.filter(
            F.col("point_of_sale_code").isNull() | (F.trim(F.col("point_of_sale_code")) == "") |
            F.col("product_code").isNull() | (F.trim(F.col("product_code")) == "")
        ).count()

        if null_key_count > 0:
            self.__logger.log(
                f"WARN: {null_key_count:,} wierszy z pustym destination_code lub product_code — odrzucone.",
                level="warn"
            )
            df_combined_all = df_combined_all.filter(
                F.col("point_of_sale_code").isNotNull() & (F.trim(F.col("point_of_sale_code")) != "") &
                F.col("product_code").isNotNull() & (F.trim(F.col("product_code")) != "")
            )

        # --- Filtr: brak daty dostawy ---
        df_combined = df_combined_all.filter(F.col("delivery_date").isNotNull())
        df_combined.cache()
        valid_rows = df_combined.count()
        dropped_no_date = total_raw_rows - null_key_count - valid_rows

        self.__logger.log(f"Odrzucone wiersze (brak klucza): {null_key_count:,}")
        self.__logger.log(f"Odrzucone wiersze (brak daty dostawy): {dropped_no_date:,}")
        self.__logger.log(f"Wiersze gotowe do zapisu: {valid_rows:,}")

        if dropped_no_date > 0:
            self.__logger.log(
                f"UWAGA: Pominięto {dropped_no_date:,} rekordów bez uzupełnionej daty. "
                f"Zostaną załadowane, gdy system źródłowy uzupełni dane w kolejnych zrzutach.",
                level="warn"
            )

        df_combined_all.unpersist()
        return df_combined

    def map_columns(self, df: DataFrame) -> DataFrame:
        """Krok 3: Wymuszanie typów docelowych Occubee."""
        self.__logger.log("Rzutowanie kolumn na typy docelowe Occubee...")
        final_cols = []
        for target in self.ORDER:
            expr = F.col(target) if target in df.columns else F.lit(None)
            if target in self.CASTS:
                expr = expr.cast(self.CASTS[target])
            final_cols.append(expr.alias(target))
        return df.select(*final_cols)

    def _write_single_file(self, df: DataFrame, base_path: str, file_name: str, write_opts: dict):
        """Zapisuje DF jako pojedynczy plik CSV o czystej nazwie (Hadoop rename)."""
        from py4j.java_gateway import java_import
        java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        FileSystem = self.spark._jvm.org.apache.hadoop.fs.FileSystem

        temp_dir = self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/_tmp_transit")
        df.repartition(1).write.mode("overwrite").options(**write_opts).csv(temp_dir)

        fs = FileSystem.get(self.spark._jvm.java.net.URI(temp_dir), hadoop_conf)
        temp_path = self.spark._jvm.org.apache.hadoop.fs.Path(temp_dir)
        part_files = [f.getPath() for f in fs.listStatus(temp_path) if f.getPath().getName().startswith("part-")]

        if part_files:
            target = self.spark._jvm.org.apache.hadoop.fs.Path(
                self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/{file_name}")
            )
            if fs.exists(target):
                fs.delete(target, False)
            fs.rename(part_files[0], target)

        fs.delete(temp_path, True)

    def save_results(self, df_final: DataFrame):
        """Krok 4: Zapis wyników na Azure Data Lake (kontener klienta: data@4fprodmainsa)."""
        write_opts = {
            "header": "true", "sep": ";", "encoding": "UTF-8",
            "quoteAll": "false", "emptyValue": "", "dateFormat": "yyyy-MM-dd"
        }

        if self.OUTPUT_MODE in ("FULL", "BOTH"):
            base_path = self.__script_cfg['output']['result_full_path']
            file_name = self.__script_cfg['output']['result_full_filename']
            self.__logger.log(f"Zapisywanie FULL: {base_path}/{file_name}")
            self._write_single_file(df_final, base_path, file_name, write_opts)
            self.__logger.log("Sukces! Zapisano FULL.")

        if self.OUTPUT_MODE in ("TEST", "BOTH"):
            base_path = self.__script_cfg['output']['result_sample_path']
            file_name = self.__script_cfg['output']['result_sample_filename']
            self.__logger.log(f"Zapisywanie SAMPLE: {base_path}/{file_name}")
            self._write_single_file(df_final.limit(1000), base_path, file_name, write_opts)
            self.__logger.log("Sukces! Zapisano SAMPLE.")


if __name__ == "__main__":
    script_cfg = Configuration()
    cmd_args = GatherArguments(script_cfg["std_cmd_args"]["std_cmd_args"])

    apply_profile(script_cfg, "light")
    lib_helpers_factory = LibHelpersFactory(config=script_cfg, cmd_args=cmd_args)
    app_name = "Import Towaru w Drodze 4F"

    spark_wrapper = lib_helpers_factory.make_spark_wrapper(spark_application_name=app_name)

    with spark_wrapper as sc:
        spark_wrapper.spark.sparkContext.setLogLevel("ERROR")
        azure_helper = lib_helpers_factory.make_azure_helper()
        spark_helper = lib_helpers_factory.make_spark_helper()
        logger = Logger(script_cfg['logging']['log_on_the_screen'], app_name)

        dictionary_helper = lib_helpers_factory.make_dictionary_helper()
        parameters = Parameters(dictionary_helper)
        spark_wrapper.spark.sparkContext._jsc.hadoopConfiguration().set(
            f"fs.azure.account.key.{parameters.client_storage_account_name}",
            parameters.client_storage_account_key
        )
        client_azure_helper = AzureHelper(
            container_name=parameters.client_container_name,
            storage_account_name=parameters.client_storage_account_name,
            spark=spark_wrapper.spark,
        )

        importer = GoodsInTransitImport(spark_helper, azure_helper, client_azure_helper, script_cfg, logger)

        logger.printSection("ETAP 1: Wczytywanie Danych")
        df_orders, df_transit = importer.load_raw_data()

        logger.printSection("ETAP 2: Walidacja Danych Zrodlowych")
        importer.validate_source(df_orders, "open_orders.csv", importer.REQUIRED_COLS_ORDERS)
        importer.validate_source(df_transit, "goods_in_transit.csv", importer.REQUIRED_COLS_TRANSIT)

        logger.printSection("ETAP 3: Transformacja i Łączenie")
        df_combined = importer.transform_and_union(df_orders, df_transit)

        logger.printSection("ETAP 4: Mapowanie i Rzutowanie Typów")
        df_mapped = importer.map_columns(df_combined)

        logger.printSection("ETAP 5: Zapis Danych na Azure Data Lake")
        importer.save_results(df_mapped)

        df_combined.unpersist()
