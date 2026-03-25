"""
================================================================================
SKRYPT: Import stanow sklepowych (Store Stock / Inventory) 4F do Occubee
Autor:  Patryk Bubula / Occubee Data Engineering
Wersja: 2.0  (2026-03-17)
================================================================================

OPIS
----
Skrypt pobiera codzienne stany magazynowe sklepow sieci 4F z plikow inventory
dostarczonych przez klienta i generuje plik CSV zgodny ze struktura importu
stanow sklepowych (Store Stock Level) platformy Occubee.

W wersji 2.0 dodano mechanizm FILE_MODE (automatyczny wybor najnowszego pliku),
framework walidacyjny oraz zmieniono domyslny tryb zapisu na FULL.

KONWENCJA: "BRAK WIERSZA = STOCK 0"
------------------------------------
Occubee interpretuje brak wiersza dla danej kombinacji (sklep, produkt, data)
jako stan zerowy. Dlatego w pliku NIE trzeba generowac wierszy z inventory=0
dla produktow, ktorych sklep nie posiada. Import zawiera TYLKO pozycje
z niezerowym stanem magazynowym.

KONFIGURACJA UZYTKOWNIKA (zmienne klasowe InventoryImport)
-----------------------------------------------------------
FILE_MODE : str
    Tryb wyboru pliku wejsciowego:

    "LATEST" (domyslny)
        Skrypt listuje wszystkie pliki w folderze DATA_IN/STANY_SKLEPOWE/
        na Azure, sortuje je alfabetycznie po nazwie i wybiera OSTATNI.
        Pliki maja nazwy inventory_yyyy_mm_dd.csv, wiec sortowanie
        alfabetyczne = sortowanie chronologiczne. Idealne do codziennego
        automatycznego importu — zawsze przetwarza najnowszy plik.

    "MANUAL"
        Skrypt uzywa pliku wskazanego przez zmienna MANUAL_FILE_NAME.
        Idealne do recznego backfill lub powtorzenia importu konkretnego dnia.

MANUAL_FILE_NAME : str
    Nazwa pliku do przetworzenia (bez sciezki folderu), np.:
    "inventory_2026_03_15.csv"
    Uzywane TYLKO gdy FILE_MODE = "MANUAL". Ignorowane w trybie "LATEST".

OUTPUT_MODE : str
    Tryb zapisu wynikow:

    "FULL"
        Zapisuje pelny wynik w 10 plikach CSV (repartition=10).
        Sciezka: TRANSFORMED_DATA/STANY_SKLEPOWE/store_stock_FULL/

    "TEST"
        Zapisuje probke 1000 wierszy w 1 pliku CSV (coalesce=1).
        Sciezka: TRANSFORMED_DATA/STANY_SKLEPOWE/store_stock_SAMPLE/

    "BOTH"
        Zapisuje oba warianty (FULL + TEST).

WALIDACJA DANYCH
-----------------
Tylko sprawdzenie schematu (obecnosc wymaganych kolumn) — zero akcji Spark.
Dane OTCF sa stabilne, brak pliku wynikowego jest wystarczajacym sygnalem problemu.

LOGIKA BIZNESOWA
----------------
1. Normalizacja nazw kolumn: strip + lowercase.

2. Czyszczenie wartosci numerycznych:
   Plik zrodlowy 4F eksportuje liczby otoczone cudzyslowami (np. "1 234,56").
   Cudzyslowy sa usuwane, przecinki zamieniane na kropki, konwersja do double.
   Dotyczy kolumn: inventory (ilosc) oraz stock_cost (wartosc zapasow).

3. Formatowanie dat:
   Kolumna inventory_date rzutowana na DateType (format yyyy-MM-dd).

4. Jednostka miary:
   Occubee wymaga pola unit_of_measure_code. Dla 4F stala wartoscia jest "ST"
   (sztuka) — dodawana jako kolumna wyliczana (calc_uom).

5. Pominiecie kolumn technicznych:
   Kolumny point_of_sale_section_code i point_of_sale_shelf_code sa zdefiniowane
   w ORDER, ale wymienione w EXCLUDE — zostana pominiete w finalnym SELECT.

6. Wymuszanie typow (Casting):
   amount -> Decimal(15, 3), stock_cost -> Decimal(15, 2), daty -> DateType.

ZRODLO DANYCH (INPUT)
---------------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net

  DATA_IN/STANY_SKLEPOWE/inventory_yyyy_mm_dd.csv  (sep=;)
    Stany sklepowe. Kolumny: store_code, product_code, inventory,
    inventory_date, stock_cost, currency.

WYJSCIE (OUTPUT)
----------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net
  TRANSFORMED_DATA/STANY_SKLEPOWE/store_stock_FULL/   — 10 plikow CSV
  TRANSFORMED_DATA/STANY_SKLEPOWE/store_stock_SAMPLE/ — 1 plik CSV, 1 000 wierszy
Format: CSV, sep=;, encoding=UTF-8, quoteAll=false, dateFormat=yyyy-MM-dd

JAK URUCHAMIAC
--------------
Codzienny import (automatyczny, najnowszy plik):
    Ustaw FILE_MODE = "LATEST", OUTPUT_MODE = "FULL".
    /opt/spark3/bin/spark-submit 4f_store_stock_import.py \\
      -s otcf4fprodmainsa.dfs.core.windows.net \\
      -k <OCCUBEE_KEY> -c bigdata \\
      -p <process_instance_id> \\
      -v spark_script_configs/<config>.json

Backfill (reczny, konkretny plik):
    Ustaw FILE_MODE = "MANUAL",
    MANUAL_FILE_NAME = "inventory_2026_03_15.csv",
    OUTPUT_MODE = "FULL".
    Uruchom jak wyzej.

Testowy (probka):
    Ustaw OUTPUT_MODE = "TEST" (lub "BOTH" aby miec oba).
    Uruchom jak wyzej.
================================================================================
"""


import sys
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType
from pyspark.sql import DataFrame

# Importy z biblioteki Occubee
from occubee.common import (GatherArguments, Configuration, LibHelpersFactory,
                            SparkWrapper, AzureHelper, SparkHelper, Logger)
from lib.parameters import Parameters
from lib.spark_profiles import apply_profile


class InventoryImport:
    # ==========================================================================
    # KONFIGURACJA UZYTKOWNIKA — zmien ponizsze wartosci przed uruchomieniem
    # ==========================================================================

    # Tryb wyboru pliku: "LATEST" (najnowszy z folderu) | "MANUAL" (wskazany)
    FILE_MODE = "LATEST"

    # Nazwa pliku do przetworzenia (tylko gdy FILE_MODE = "MANUAL")
    MANUAL_FILE_NAME = "inventory_2026_03_15.csv"

    # Tryb zapisu: "FULL" (calosc) | "TEST" (probka 1000) | "BOTH" (oba)
    OUTPUT_MODE = "FULL"

    # ==========================================================================
    # SLOWNIKI I MAPOWANIA
    # ==========================================================================
    ORDER = [
        "date_point_of_sale_level", "point_of_sale_code", "product_code", "amount",
        "unit_of_measure_code", "point_of_sale_section_code", "point_of_sale_shelf_code",
        "stock_cost", "currency"
    ]

    EXCLUDE = ["point_of_sale_section_code", "point_of_sale_shelf_code"]

    MAP = {
        "date_point_of_sale_level": "inventory_date", "point_of_sale_code": "store_code",
        "product_code": "product_code", "amount": "inventory", "unit_of_measure_code": "calc_uom",
        "stock_cost": "stock_cost", "currency": "currency"
    }

    CASTS = {
        "date_point_of_sale_level": DateType(), "point_of_sale_code": StringType(), "product_code": StringType(),
        "amount": DecimalType(15, 3), "unit_of_measure_code": StringType(), "stock_cost": DecimalType(15, 2),
        "currency": StringType()
    }

    REQUIRED_COLUMNS = [
        "store_code", "product_code", "inventory", "inventory_date", "stock_cost", "currency"
    ]

    def __init__(self, spark_helper: SparkHelper, azure_helper: AzureHelper, client_azure_helper: AzureHelper,
                 script_cfg: Configuration, logger: Logger):
        self.__spark_helper = spark_helper
        self.__azure_helper = azure_helper
        self.__client_azure_helper = client_azure_helper
        self.__script_cfg = script_cfg
        self.__logger = logger
        self.spark = spark_helper.spark

    # ==========================================================================
    # ETAP 1: WCZYTYWANIE DANYCH
    # ==========================================================================
    def _resolve_inventory_file_path(self) -> str:
        """Wyznacza pelna sciezke Azure do pliku inventory na podstawie FILE_MODE."""
        inventory_folder = self.__script_cfg['input']['raw_inventory_folder']

        if self.FILE_MODE == "MANUAL":
            file_name = self.MANUAL_FILE_NAME
            self.__logger.log(f"FILE_MODE=MANUAL -> uzywam pliku: {file_name}")

        elif self.FILE_MODE == "LATEST":
            self.__logger.log(f"FILE_MODE=LATEST -> listowanie plikow w: {inventory_folder}")
            folder_azure_path = self.__client_azure_helper.create_azure_path(folder_path=inventory_folder)

            # Listowanie plikow w folderze — Hadoop FileSystem API
            hadoop_conf = self.spark.sparkContext._jsc.hadoopConfiguration()
            fs = self.spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                self.spark._jvm.java.net.URI(folder_azure_path), hadoop_conf
            )
            status_list = fs.listStatus(self.spark._jvm.org.apache.hadoop.fs.Path(folder_azure_path))

            csv_files = []
            for status in status_list:
                name = status.getPath().getName()
                if name.startswith("inventory_") and name.endswith(".csv"):
                    csv_files.append(name)

            if not csv_files:
                raise RuntimeError(f"CRITICAL: Brak plikow inventory_*.csv w folderze {inventory_folder}")

            csv_files.sort()
            file_name = csv_files[-1]
            self.__logger.log(f"Znaleziono {len(csv_files)} plikow. Najnowszy: {file_name}")

        else:
            raise ValueError(f"Nieznany FILE_MODE: '{self.FILE_MODE}'. Dozwolone: LATEST, MANUAL.")

        full_path = f"{inventory_folder}/{file_name}"
        self.__logger.log(f"Sciezka pliku do przetworzenia: {full_path}")
        return full_path

    def load_raw_data(self) -> DataFrame:
        """Wczytuje surowy plik inventory z kontenera 'data' klienta."""
        self.__logger.log("Wczytywanie surowego pliku inventory...")

        inventory_file_path = self._resolve_inventory_file_path()
        path_inv = self.__client_azure_helper.create_azure_path(folder_path=inventory_file_path)

        opts = {
            "header": "true", "sep": ";", "inferSchema": "false",
            "ignoreLeadingWhiteSpace": "true", "ignoreTrailingWhiteSpace": "true"
        }

        df = self.spark.read.options(**opts).csv(path_inv)

        # Standaryzacja nazw kolumn
        return df.toDF(*[c.strip().lower() for c in df.columns])

    # ==========================================================================
    # ETAP 2: WALIDACJA
    # ==========================================================================
    def validate(self, df: DataFrame) -> DataFrame:
        """Walidacja schematu — zero akcji Spark, tylko metadane."""
        actual_cols = set(df.columns)
        missing = [c for c in self.REQUIRED_COLUMNS if c not in actual_cols]
        if missing:
            raise RuntimeError(
                f"CRITICAL: Brakujace kolumny: {missing}. "
                f"Dostepne: {sorted(actual_cols)}. Przerwanie importu."
            )
        self.__logger.log(f"Schemat OK — {len(actual_cols)} kolumn, wszystkie wymagane obecne.")
        return df

    # ==========================================================================
    # ETAP 3: CZYSZCZENIE I TRANSFORMACJA
    # ==========================================================================
    def clean_and_transform(self, df: DataFrame) -> DataFrame:
        """Czyszczenie cudzyslowow, formatowanie kwot i dat, dodanie calc_uom."""
        self.__logger.log("Czyszczenie danych (usuwanie cudzyslowow, formatowanie kwot i dat)...")

        for c in ["inventory", "stock_cost"]:
            if c in df.columns:
                df = df.withColumn(c, F.regexp_replace(F.col(c), "\"", ""))
                df = df.withColumn(c, F.regexp_replace(F.col(c), ",", ".").cast("double"))

        if "inventory_date" in df.columns:
            df = df.withColumn("inventory_date", F.to_date(F.col("inventory_date")))

        return df.withColumn("calc_uom", F.lit("ST"))

    # ==========================================================================
    # ETAP 4: MAPOWANIE I RZUTOWANIE TYPOW
    # ==========================================================================
    def map_columns(self, df: DataFrame) -> DataFrame:
        """Mapowanie na docelowa strukture i wymuszanie typow."""
        self.__logger.log("Rzutowanie kolumn na typy docelowe Occubee...")
        final_cols = []

        for target in self.ORDER:
            if target in self.EXCLUDE:
                continue

            src = self.MAP.get(target, target)
            expr = F.col(src) if src in df.columns else F.lit(None)

            if target in self.CASTS:
                expr = expr.cast(self.CASTS[target])
            final_cols.append(expr.alias(target))

        return df.select(*final_cols)

    # ==========================================================================
    # ETAP 5: CACHE I ZAPIS
    # ==========================================================================
    def _write_single_file(self, df: DataFrame, base_path: str, file_name: str, write_opts: dict):
        """Zapisuje DF jako pojedynczy plik CSV o czystej nazwie (Hadoop rename)."""
        from py4j.java_gateway import java_import
        java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        FileSystem = self.spark._jvm.org.apache.hadoop.fs.FileSystem

        temp_dir = self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/_tmp_stock")
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
        """Zapis na Azure (kontener klienta: data@4fprodmainsa) zalezenie od OUTPUT_MODE."""
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

    apply_profile(script_cfg, "medium")
    lib_helpers_factory = LibHelpersFactory(config=script_cfg, cmd_args=cmd_args)
    app_name = "Import Zapasow Magazynowych 4F"

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

        importer = InventoryImport(spark_helper, azure_helper, client_azure_helper, script_cfg, logger)

        logger.printSection("ETAP 1: Wczytywanie Danych")
        df_raw = importer.load_raw_data()

        logger.printSection("ETAP 2: Walidacja Danych")
        df_raw = importer.validate(df_raw)

        logger.printSection("ETAP 3: Czyszczenie i Transformacja")
        df_clean = importer.clean_and_transform(df_raw)

        logger.printSection("ETAP 4: Mapowanie i Rzutowanie Typow")
        df_mapped = importer.map_columns(df_clean)

        logger.printSection("ETAP 5: Cache i Zapis")
        importer.save_results(df_mapped)

        logger.log("PROCES ZAKONCZONY SUKCESEM.")
