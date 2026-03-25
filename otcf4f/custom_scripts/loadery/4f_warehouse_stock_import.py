"""
================================================================================
SKRYPT: Import stanów magazynowych (Warehouse Stock) 4F do Occubee
Autor:  Patryk Bubula / Occubee Data Engineering
================================================================================

OPIS
----
Skrypt pobiera codzienne stany magazynów centralnych (DC) sieci 4F z pliku
inventory_warehouse.csv i generuje plik CSV zgodny ze strukturą importu
stanów magazynowych (Warehouse Stock Level) platformy Occubee.

W odróżnieniu od store_stock_import, dane dotyczą magazynów hurtowych (DC),
a nie stanów w poszczególnych sklepach.

CHARAKTERYSTYKA PLIKU ŹRÓDŁOWEGO
---------------------------------
Plik inventory_warehouse.csv jest MAŁY (rzędu kilku-kilkunastu tysięcy wierszy)
i jest NADPISYWANY codziennie przez OTCF — zawiera SNAPSHOT stanów magazynowych
z jednego dnia. Nie jest to delta — to pełen obraz stanów na dany dzień.

Kluczowa zasada biznesowa: BRAK WIERSZA = STOCK 0.
Jeśli dany produkt nie pojawia się w pliku, oznacza to że jego stan magazynowy
wynosi zero. Occubee interpretuje to w ten sam sposób — import jest CALOSCIOWY
(nadpisuje poprzedni stan).

METODA IMPORTU: CALOSCIOWO (overwrite)
Occubee traktuje każdy import jako pełny snapshot — poprzednie stany są
zastępowane nowymi.

ŹRÓDŁO DANYCH (INPUT)
---------------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net

  inventory_warehouse.csv  (sep=|)
    Stany magazynów centralnych. Kolumny: warehouse_code, product_code,
    inventory, inventory_warehouse_date.
    Separator '|' (różni się od store_stock, który używa ';').
    Może zawierać BOM UTF-8 i białe znaki w nagłówkach — czyszczone przy odczycie.

LOGIKA BIZNESOWA
----------------
1. Normalizacja liczb:
   Kolumna inventory może zawierać europejski zapis dziesiętny z przecinkiem.
   Zamiana przecinków na kropki przed konwersją do double.

2. Formatowanie dat:
   Kolumna inventory_warehouse_date rzutowana na DateType (format yyyy-MM-dd).

3. Jednostka miary:
   Occubee wymaga pola warehouse_unit_of_measure_code. Dla 4F stałą wartością
   jest "ST" (sztuka) — dodawana jako kolumna wyliczana (calc_uom).

4. Walidacja danych (przed zapisem):
   V1: Pusty plik → CRITICAL ERROR, przerwanie skryptu (sys.exit(1)).
   V2: Brak wymaganych kolumn → CRITICAL ERROR, przerwanie skryptu.
   V3: NULL/puste wartości w kolumnach wymaganych → WARN z liczbą braków.
       Plik z brakami zostanie zapisany, ale może zostać odrzucony przez
       walidator Occubee — walidacja pozwala szybko zidentyfikować problem.

5. Wymuszanie typów (Casting):
   amount → Decimal(15, 3), daty → DateType, kody → StringType.

WYJŚCIE (OUTPUT)
----------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net
  TRANSFORMED_DATA/STANY_MAGAZYNOWE/warehouse_stock.csv         — pojedynczy plik CSV
  TRANSFORMED_DATA/STANY_MAGAZYNOWE/warehouse_stock_SAMPLE.csv — próbka 1000 wierszy
Format: CSV, sep=;, encoding=UTF-8, quoteAll=false, dateFormat=yyyy-MM-dd

URUCHOMIENIE
------------
/opt/spark3/bin/spark-submit 4f_warehouse_stock_import.py \\
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


class WarehouseStockImport:
    # ==========================================================================
    # TRYB ZAPISU: "TEST" (próbka 1000 wierszy), "FULL" (całość), "BOTH" (oba)
    # ==========================================================================
    OUTPUT_MODE = "FULL"

    ORDER = [
        "date_stock_level", "warehouse_code", "product_code",
        "amount", "warehouse_unit_of_measure_code"
    ]

    REQUIRED_COLS = [
        "date_stock_level", "warehouse_code", "product_code",
        "amount", "warehouse_unit_of_measure_code"
    ]

    MAP = {
        "date_stock_level": "inventory_warehouse_date",
        "warehouse_code": "warehouse_code",
        "product_code": "product_code",
        "amount": "inventory",
        "warehouse_unit_of_measure_code": "calc_uom"
    }

    CASTS = {
        "date_stock_level": DateType(),
        "warehouse_code": StringType(),
        "product_code": StringType(),
        "amount": DecimalType(15, 3),
        "warehouse_unit_of_measure_code": StringType()
    }

    def __init__(self, spark_helper: SparkHelper, azure_helper: AzureHelper, client_azure_helper: AzureHelper,
                 script_cfg: Configuration, logger: Logger):
        self.__spark_helper = spark_helper
        self.__azure_helper = azure_helper
        self.__client_azure_helper = client_azure_helper
        self.__script_cfg = script_cfg
        self.__logger = logger
        self.spark = spark_helper.spark

    def load_raw_data(self) -> DataFrame:
        """Krok 1: Wczytywanie surowych danych z Azure."""
        self.__logger.log("Wczytywanie inventory_warehouse.csv...")
        path = self.__client_azure_helper.create_azure_path(
            folder_path=self.__script_cfg['input']['raw_warehouse_inventory']
        )
        self.__logger.log(f"Sciezka zrodlowa: {path}")

        df = self.spark.read.option("header", "true").option("sep", "|").csv(path)
        cleaned_columns = [c.strip().lower().encode('ascii', 'ignore').decode('ascii') for c in df.columns]
        return df.toDF(*cleaned_columns)

    def clean_and_transform(self, df: DataFrame) -> DataFrame:
        """Krok 2: Czyszczenie kwot, formatowanie dat, dodanie jednostki miary."""
        self.__logger.log("Czyszczenie danych (formatowanie kwot i dat)...")

        if "inventory" in df.columns:
            df = df.withColumn("inventory", F.regexp_replace(F.col("inventory"), ",", ".").cast("double"))

        if "inventory_warehouse_date" in df.columns:
            df = df.withColumn("inventory_warehouse_date", F.to_date(F.col("inventory_warehouse_date")))

        return df.withColumn("calc_uom", F.lit("ST"))

    def validate(self, df: DataFrame) -> bool:
        """Krok 3: Walidacja danych — V1 pusty plik, V2 brak kolumn, V3 NULL w wymaganych."""
        self.__logger.log("Walidacja danych wyjsciowych...")

        # --- V1: Pusty plik ---
        df.cache()
        total_rows = df.count()
        self.__logger.log(f"Liczba wierszy do zapisu: {total_rows:,}")
        if total_rows == 0:
            self.__logger.log(
                "CRITICAL: Plik zrodlowy jest PUSTY (0 wierszy). Przerywanie importu.",
                level="error"
            )
            sys.exit(1)

        # --- V2: Brak wymaganych kolumn zrodlowych ---
        required_source_cols = set(self.MAP.values())
        # calc_uom jest kolumna wyliczana — nie sprawdzamy jej w surowych danych
        cols_to_check = required_source_cols - {"calc_uom"}
        missing_cols = cols_to_check - set(df.columns)
        if missing_cols:
            self.__logger.log(
                f"CRITICAL: Brak wymaganych kolumn w danych: {sorted(missing_cols)}. "
                f"Dostepne kolumny: {sorted(df.columns)}. Przerywanie importu.",
                level="error"
            )
            sys.exit(1)

        # --- V3: NULL / puste wartosci w kolumnach wymaganych ---
        has_warnings = False
        for target in self.REQUIRED_COLS:
            src = self.MAP.get(target, target)
            if src in df.columns:
                null_count = df.filter(
                    F.col(src).isNull() | (F.col(src).cast("string") == "")
                ).count()
                if null_count > 0:
                    self.__logger.log(
                        f"WARN: Kolumna '{src}' (-> {target}) posiada {null_count:,} pustych wartosci "
                        f"({null_count * 100.0 / total_rows:.1f}% z {total_rows:,}).",
                        level="warn"
                    )
                    has_warnings = True
                else:
                    self.__logger.log(f"OK: Kolumna '{src}' jest w 100% wypelniona.")

        if has_warnings:
            self.__logger.log(
                "UWAGA: Wykryto braki danych. Plik moze zostac odrzucony przez Occubee.",
                level="warn"
            )

        return has_warnings

    def map_columns(self, df: DataFrame) -> DataFrame:
        """Krok 4: Mapowanie na docelową strukturę i wymuszanie typów."""
        self.__logger.log("Rzutowanie kolumn na typy docelowe Occubee...")
        final_cols = []
        for target in self.ORDER:
            src = self.MAP.get(target, target)
            expr = F.col(src) if src in df.columns else F.lit(None)
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

        temp_dir = self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/_tmp_warehouse")
        df.repartition(1).write.mode("overwrite").options(**write_opts).csv(temp_dir)

        fs = FileSystem.get(self.spark._jvm.java.net.URI(temp_dir), hadoop_conf)
        temp_path = self.spark._jvm.org.apache.hadoop.fs.Path(temp_dir)
        part_files = [f.getPath() for f in fs.listStatus(temp_path) if f.getPath().getName().startswith("part-")]

        if part_files:
            target = self.spark._jvm.org.apache.hadoop.fs.Path(
                self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/{file_name}")
            )
            fs.rename(part_files[0], target)

        fs.delete(temp_path, True)

    def save_results(self, df_final: DataFrame):
        """Krok 5: Zapis wyników na Azure Data Lake (kontener klienta: data@4fprodmainsa)."""
        write_opts = {
            "header": "true", "sep": ";", "encoding": "UTF-8",
            "quoteAll": "false", "emptyValue": "", "dateFormat": "yyyy-MM-dd"
        }

        if self.OUTPUT_MODE in ("FULL", "BOTH"):
            base_path = self.__script_cfg['output']['result_full_path']
            file_name = self.__script_cfg['output']['result_full_filename']
            self.__logger.log(f"Zapisywanie FULL: {base_path}/{file_name}")
            self._write_single_file(df_final, base_path, file_name, write_opts)
            self.__logger.log("Sukces! Zapisano pelne dane magazynowe.")

        if self.OUTPUT_MODE in ("TEST", "BOTH"):
            base_path = self.__script_cfg['output']['result_sample_path']
            file_name = self.__script_cfg['output']['result_sample_filename']
            self.__logger.log(f"Zapisywanie probki: {base_path}/{file_name}")
            self._write_single_file(df_final.limit(1000), base_path, file_name, write_opts)
            self.__logger.log("Sukces! Wygenerowano probke.")


if __name__ == "__main__":
    script_cfg = Configuration()
    cmd_args = GatherArguments(script_cfg["std_cmd_args"]["std_cmd_args"])

    apply_profile(script_cfg, "light")
    lib_helpers_factory = LibHelpersFactory(config=script_cfg, cmd_args=cmd_args)
    app_name = "Import Stanow Magazynowych 4F"

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

        importer = WarehouseStockImport(spark_helper, azure_helper, client_azure_helper, script_cfg, logger)

        logger.printSection("ETAP 1: Wczytywanie Danych")
        df_raw = importer.load_raw_data()

        logger.printSection("ETAP 2: Czyszczenie i Logika Biznesowa")
        df_clean = importer.clean_and_transform(df_raw)

        logger.printSection("ETAP 3: Walidacja Danych")
        importer.validate(df_clean)

        logger.printSection("ETAP 4: Mapowanie i Rzutowanie Typow")
        df_mapped = importer.map_columns(df_clean)

        logger.printSection("ETAP 5: Zapis Danych na Azure Data Lake")
        importer.save_results(df_mapped)

        df_clean.unpersist()
