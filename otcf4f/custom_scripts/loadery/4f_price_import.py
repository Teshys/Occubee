"""
================================================================================
SKRYPT: Import i transformacja cenników 4F do struktur Occubee
Autor:  Patryk Bubula / Occubee Data Engineering
================================================================================

OPIS
----
Skrypt przetwarza dwa cenniki 4F (bazowy i promocyjny), grupuje sklepy na strefy
cenowe i generuje pliki CSV zgodne ze strukturą importu cenników (Price Lists)
platformy Occubee.

CHARAKTER DANYCH ŹRÓDŁOWYCH
----------------------------
Pliki price.csv i retail_price.csv to SNAPSHOTY aktualnego stanu cennika,
NIE historia zmian cen. Każdy produkt w danej strefie ma dokładnie 1 wiersz
(1 cenę). Pole price_date oznacza datę ostatniej aktualizacji ceny danego
produktu, nie serię czasową.

Konsekwencje:
  - Filtr min_date w .cfg odcina produkty ze starszą ceną — zmniejsza pokrycie
    asortymentu zamiast ograniczać historię. Zalecenie: nie filtrować (puste).
  - Sekcja end_date w skrypcie (lead/lag) jest przygotowana na historię,
    ale przy 1 wpisie per produkt end_date zawsze = NULL.
  - Aby budować historię cenową pod modele ML, konieczne jest cykliczne
    uruchamianie importu (np. co tydzień) z akumulacją w Occubee (tryb DELTA).

ŹRÓDŁO DANYCH (INPUT)
---------------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net

  price.csv  (sep=|)  ~21M wierszy, ~800k SKU, 36 stref
    Cennik bazowy (first_price). Kolumny: price_date,
    product_agg_level_code, store_agg_level_code, price_with_tax, currency.

  retail_price.csv  (sep=|)  ~11M wierszy, ~700k SKU, 34 strefy
    Cennik promocyjny (actual_price). Identyczna struktura jak price.csv.

  store.csv  (sep=;)
    Słownik sklepów z przypisaniem do stref cenowych.
    Kolumny: store_code, first_price (strefa bazowa),
             actual_price (strefa promocyjna).

Kontener: bigdata @ otcf4fprodmainsa.dfs.core.windows.net

  dictionaries/product  (parquet)
    Słownik produktów Occubee — walidacja SKU (INNER JOIN).

LOGIKA BIZNESOWA
----------------
1. Podział na strefy cenowe (~37 stref):
   Osobna encja cennikowa dla każdej strefy (np. "Cennik_bazowy_PLREG").
   Strefy jako rynki (market_code) — bez method_of_sale_code.

2. Kalkulacja end_date (przygotowana na historię):
   end_date = start_date następnego wiersza minus 1 dzień (window lead()).
   Przy 1 wpisie per produkt: end_date = NULL (bezterminowy).

3. Dedup: Lowest Price Wins — jeśli produkt ma kilka cen na tę samą datę
   w tej samej strefie, zostaje najniższa.

4. Deterministyczne priorytety stref: ranking alfabetyczny po name.

5. Walidacja produktów: INNER JOIN ze słownikiem Occubee.

6. Oba cenniki w jednym przejściu (price.csv + retail_price.csv).

7. Tryb DELTA (import przyrostowy):
   Porównuje z dictionaries/price (bigdata). Eksportuje tylko nowe/zmienione.
   Klucz: name + product_code + start_date.

WYJŚCIE (OUTPUT)
----------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net
  TRANSFORMED_DATA/CENNIKI/price_FULL/    — CSV (OUTPUT_MODE=FULL)
  TRANSFORMED_DATA/CENNIKI/price_SAMPLE/  — próbka (OUTPUT_MODE=TEST)
Format: CSV, sep=;, encoding=UTF-8, quoteAll=false, timestamps yyyy-MM-dd HH:mm:ss

URUCHOMIENIE
------------
/opt/spark3/bin/spark-submit 4f_price_import.py \\
  -s otcf4fprodmainsa.dfs.core.windows.net \\
  -k <OCCUBEE_KEY> -c bigdata \\
  -p <process_instance_id> \\
  -v spark_script_configs/<config>.json
================================================================================
"""


import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from pyspark.sql import DataFrame

# Importy z biblioteki Occubee
from occubee.common import (GatherArguments, Configuration, LibHelpersFactory,
                            SparkWrapper, AzureHelper, SparkHelper, Logger)
from lib.parameters import Parameters
from lib.spark_profiles import apply_profile

class PriceImport:
    # ==========================================================================
    # TRYB ZAPISU: "TEST" (próbka 1000 wierszy), "FULL" (całość), "BOTH" (oba)
    # ==========================================================================
    OUTPUT_MODE = "FULL"

    # ==========================================================================
    # FILTR STREF CENOWYCH: lista nazw stref do wygenerowania.
    # Pusta lista [] = generuj WSZYSTKIE strefy.
    # Przykład: ["PLREGA", "PLREGF"] = generuj tylko te dwa cenniki.
    # ==========================================================================
    SELECTED_ZONES = ["PLREGA", "PLREGF"]

    # ==========================================================================
    # TRYB IMPORTU:
    #   "FULL"  — eksportuje WSZYSTKIE ceny (pełna historia)
    #   "DELTA" — eksportuje TYLKO nowe/zmienione wpisy vs dictionaries/price
    #             Wymaga włączonego parametru "Cenniki ładowane przyrostowo" w Occubee.
    # ==========================================================================
    IMPORT_MODE = "FULL"

    REQUIRED_COLUMNS_PRICE = [
        "store_agg_level_code", "product_agg_level_code",
        "price_date", "price_with_tax"
    ]

    def __init__(self, spark_helper: SparkHelper, azure_helper: AzureHelper, client_azure_helper: AzureHelper,
                 script_cfg: Configuration, logger: Logger):
        self.__spark_helper = spark_helper
        self.__azure_helper = azure_helper
        self.__client_azure_helper = client_azure_helper
        self.__script_cfg = script_cfg
        self.__logger = logger
        self.spark = spark_helper.spark

    def _validate_schema(self, df: DataFrame, required_cols: list, label: str) -> DataFrame:
        """Walidacja schematu — zero akcji Spark, tylko metadane."""
        actual_cols = set(df.columns)
        missing = [c for c in required_cols if c not in actual_cols]
        if missing:
            raise RuntimeError(f"CRITICAL {label}: Brakujące kolumny: {missing}. Dostępne: {sorted(actual_cols)}.")
        self.__logger.log(f"{label}: OK — schemat zgodny.")
        return df

    # ==========================================================================
    # ETAP 1: DANE REFERENCYJNE
    # ==========================================================================
    def load_product_master(self) -> DataFrame:
        """Pobiera słownik produktów z kontenera 'bigdata'."""
        self.__logger.log("Pobieranie produktów (bigdata) do walidacji SKU...")
        sa_name = self.__azure_helper.storage_account_name
        product_path = f"abfs://bigdata@{sa_name}/{self.__script_cfg['input']['product_dict']}"

        df_master = self.spark.read.option("recursiveFileLookup", "true").parquet(product_path)
        code_col = "product_code" if "product_code" in df_master.columns else "code"

        # dropDuplicates: recursiveFileLookup wczytuje wiele snapshotów parqueta,
        # więc ten sam product_code może wystąpić N razy. Bez tego INNER JOIN
        # z cenami daje efekt mnożnikowy (1 wiersz ceny × N duplikatów = N wierszy).
        df_master_clean = df_master.select(
            F.trim(F.col(code_col)).alias("join_key_sku")
        ).dropDuplicates()

        count = df_master_clean.count()
        self.__logger.log(f"Słownik produktów: {count:,} unikalnych SKU.")
        return F.broadcast(df_master_clean)

    def prepare_stores(self) -> tuple:
        """Pobiera i grupuje sklepy na strefy (base i retail)."""
        self.__logger.log("Pobieranie i grupowanie sklepów na strefy cenowe...")
        store_path = self.__client_azure_helper.create_azure_path(folder_path=self.__script_cfg['input']['raw_store'])

        df_store = self.spark.read.option("header", "true").option("sep", ";").csv(store_path)
        df_store = df_store.toDF(*[c.strip().lower() for c in df_store.columns])
        self._validate_schema(df_store, ["first_price", "actual_price"], "store.csv")

        if "class" in df_store.columns:
            df_store = df_store.filter(~F.col("class").eqNullSafe("ZAMKNIETE"))

        store_code_col = "store_code" if "store_code" in df_store.columns else "code"

        df_store_clean = df_store.select(
            F.trim(F.col(store_code_col)).alias("mos_code"),
            F.trim(F.col("first_price")).alias("first_price"),
            F.trim(F.col("actual_price")).alias("actual_price")
        )

        def get_zoned_stores(df, group_col):
            return df.filter(F.col(group_col).isNotNull()) \
                     .groupBy(group_col) \
                     .agg(F.collect_set("mos_code").alias("mos_list")) \
                     .withColumnRenamed(group_col, "join_zone_code")

        df_store_base = get_zoned_stores(df_store_clean, "first_price")
        df_store_retail = get_zoned_stores(df_store_clean, "actual_price")

        if self.SELECTED_ZONES:
            self.__logger.log(f"FILTR STREF: generowanie tylko dla {self.SELECTED_ZONES}")
            df_store_base = df_store_base.filter(F.col("join_zone_code").isin(self.SELECTED_ZONES))
            df_store_retail = df_store_retail.filter(F.col("join_zone_code").isin(self.SELECTED_ZONES))
        else:
            self.__logger.log("FILTR STREF: brak filtra — generowanie WSZYSTKICH stref.")

        return F.broadcast(df_store_base), F.broadcast(df_store_retail)

    # ==========================================================================
    # ETAP 2: WCZYTANIE I PRZETWORZENIE OBU CENNIKÓW W JEDNYM PRZEJŚCIU
    # ==========================================================================
    def process_all_prices(self, df_master: DataFrame, df_store_base: DataFrame, df_store_retail: DataFrame) -> DataFrame:
        """Wczytuje oba cenniki (bazowy + promocyjny), przetwarza w jednym pipeline."""
        self.__logger.log("Wczytywanie obu cenników (bazowy + promocyjny)...")

        csv_opts = {"header": "true", "sep": "|"}
        path_base = self.__client_azure_helper.create_azure_path(folder_path=self.__script_cfg['input']['raw_price_base'])
        path_retail = self.__client_azure_helper.create_azure_path(folder_path=self.__script_cfg['input']['raw_price_retail'])

        # Jedno wczytanie obu plików — Spark czyta równolegle
        df_base_raw = self.spark.read.options(**csv_opts).csv(path_base)
        df_retail_raw = self.spark.read.options(**csv_opts).csv(path_retail)

        self._validate_schema(df_base_raw, self.REQUIRED_COLUMNS_PRICE, "price.csv (bazowy)")
        self._validate_schema(df_retail_raw, self.REQUIRED_COLUMNS_PRICE, "retail_price.csv (promocyjny)")

        # Dodajemy kolumnę source i łączymy od razu
        df_base_raw = df_base_raw.withColumn("_source", F.lit("base"))
        df_retail_raw = df_retail_raw.withColumn("_source", F.lit("retail"))
        df_all = df_base_raw.unionByName(df_retail_raw)

        # Transformacja — jedno przejście po danych
        df_prep = df_all.select(
            F.trim(F.col("product_agg_level_code")).alias("join_key_sku"),
            F.trim(F.col("store_agg_level_code")).alias("clean_zone_code"),
            F.round(F.regexp_replace("price_with_tax", ",", ".").cast("double"), 2).alias("value_double"),
            F.coalesce(
                F.to_date("price_date", "yyyy-MM-dd"),
                F.to_date("price_date", "yyyy-MM-dd HH:mm:ss"),
                F.to_date("price_date", "dd.MM.yyyy")
            ).alias("date_obj"),
            F.col("_source")
        ).filter(
            (F.col("value_double") > 0) & F.col("date_obj").isNotNull()
        )

        # Filtr dat z .cfg (opcjonalny)
        min_date = self.__script_cfg['business'].get('min_date', '').strip()
        if min_date:
            self.__logger.log(f"Filtr dat: tylko ceny od {min_date}")
            df_prep = df_prep.filter(F.col("date_obj") >= min_date)
        else:
            self.__logger.log("Filtr dat: BRAK — pełna historia.")

        # Join z produktami — jedno złączenie zamiast dwóch
        self.__logger.log("Join z produktami Occubee...")
        df_with_products = df_prep.join(df_master, "join_key_sku", "inner")

        # Rozdzielamy na base i retail — join ze sklepami wymaga osobnych DF stref
        self.__logger.log("Join ze strefami cenowymi (bazowy + promocyjny)...")
        # market_zone = prefiks strefy bez F/A (np. PLREGF -> PLREG)
        df_base_joined = df_with_products.filter(F.col("_source") == "base") \
            .join(df_store_base, F.col("clean_zone_code") == df_store_base.join_zone_code, "inner") \
            .withColumn("market_zone", F.expr("substring(clean_zone_code, 1, length(clean_zone_code) - 1)")) \
            .withColumn("name", F.concat(F.lit("Cennik_bazowy_"), F.col("market_zone"))) \
            .withColumn("price_type_id", F.lit("100"))

        df_retail_joined = df_with_products.filter(F.col("_source") == "retail") \
            .join(df_store_retail, F.col("clean_zone_code") == df_store_retail.join_zone_code, "inner") \
            .withColumn("market_zone", F.expr("substring(clean_zone_code, 1, length(clean_zone_code) - 1)")) \
            .withColumn("name", F.concat(F.lit("Cennik_promocyjny_"), F.col("market_zone"))) \
            .withColumn("price_type_id", F.lit("200"))

        df_combined = df_base_joined.unionByName(df_retail_joined)

        # Deduplikacja dzienna (Lowest Price Wins):
        # Jeśli dla tego samego produktu+strefy+daty istnieje wiele cen, zachowaj najniższą.
        # Bez deduplikacji lead() w kalkulacji end_date trafia na wiersz z tą samą datą
        # i daje end_date = start_date - 1 dzień (błąd walidacji).
        self.__logger.log("Deduplikacja cenników (produkt + strefa + data)...")
        dedup_window = Window.partitionBy("name", "join_key_sku", "date_obj").orderBy(F.col("value_double").asc())
        df_combined = df_combined.withColumn("_rn_dedup", F.row_number().over(dedup_window)) \
                                 .filter(F.col("_rn_dedup") == 1).drop("_rn_dedup")

        # end_date — jedyny window function w całym pipeline
        self.__logger.log("Kalkulacja end_date (window function)...")
        window_timeline = Window.partitionBy("name", "join_key_sku").orderBy(F.col("date_obj").asc())
        df_with_end = df_combined.withColumn(
            "end_date",
            F.date_format(F.date_sub(F.lead("date_obj").over(window_timeline), 1), "yyyy-MM-dd HH:mm:ss")
        )

        # Format finalny (bez priority — nadany w assign_global_priorities)
        # market_code = prefiks strefy cenowej (np. PLREG), bez explode per sklep
        return df_with_end.select(
            "name",
            F.col("name").alias("external_id"),
            "price_type_id",
            F.col("join_key_sku").alias("product_code"),
            F.col("market_zone").alias("market_code"),
            F.lit("PLN").alias("currency_code"),
            F.col("value_double").cast("string").alias("value"),
            F.date_format(F.col("date_obj"), "yyyy-MM-dd HH:mm:ss").alias("start_date"),
            "end_date"
        )

    def assign_global_priorities(self, df_combined: DataFrame) -> DataFrame:
        """Nadaje globalnie unikalne priorytety — ranking alfabetyczny po name."""
        self.__logger.log("Nadawanie globalnych priorytetów...")
        df_names = df_combined.select("name").distinct().orderBy("name")
        df_names = df_names.withColumn("priority", F.row_number().over(Window.orderBy("name")).cast(StringType()))
        df_names = F.broadcast(df_names)
        df_with_priority = df_combined.join(df_names, "name", "inner")

        # Kolejność kolumn zgodna z matcher_config Occubee (NiFi)
        return df_with_priority.select(
            "name", "external_id", "price_type_id", "currency_code",
            "market_code", "product_code", "value",
            "start_date", "end_date", "priority"
        )

    # ==========================================================================
    # DELTA: porównanie z dictionaries/price (bigdata)
    # ==========================================================================
    def load_existing_prices(self) -> DataFrame:
        """Wczytuje cenniki już załadowane do Occubee (dictionaries/price z bigdata)."""
        sa_name = self.__azure_helper.storage_account_name
        price_path = f"abfs://bigdata@{sa_name}/{self.__script_cfg['input']['price_dict']}"
        self.__logger.log(f"DELTA: Wczytywanie istniejących cenników z {price_path}...")

        try:
            df_existing = self.spark.read.option("recursiveFileLookup", "true").parquet(price_path)

            # Normalizacja nazw kolumn — dictionaries mogą mieć różne schematy
            available = set(df_existing.columns)
            name_col = "name" if "name" in available else "external_id"
            product_col = "product_code" if "product_code" in available else "code"
            start_col = "start_date" if "start_date" in available else "valid_from"
            end_col = "end_date" if "end_date" in available else "valid_to"
            value_col = "value" if "value" in available else "price"

            df_norm = df_existing.select(
                F.col(name_col).alias("name"),
                F.trim(F.col(product_col)).alias("product_code"),
                F.col(start_col).cast("string").alias("start_date"),
                F.col(end_col).cast("string").alias("end_date"),
                F.col(value_col).cast("string").alias("value")
            )

            count = df_norm.count()
            self.__logger.log(f"DELTA: Wczytano {count:,} istniejących wpisów cennikowych z Occubee.")
            return df_norm

        except Exception as e:
            self.__logger.log(f"DELTA: Nie udało się odczytać dictionaries/price ({e.__class__.__name__}). "
                              "Pierwszy import? Przełączam na FULL.")
            return None

    def apply_delta(self, df_full: DataFrame) -> DataFrame:
        """Porównuje przetworzony pełny cennik z tym co już jest w Occubee.
        Zwraca tylko:
          - NOWE wiersze (nie istnieją w Occubee)
          - ZAKTUALIZOWANE wiersze (end_date się zmieniło, np. z NULL na datę)
        Klucz porównania: name + product_code + start_date"""

        df_existing = self.load_existing_prices()

        if df_existing is None:
            self.__logger.log("DELTA: Brak danych referencyjnych — eksport FULL.")
            return df_full

        # Filtruj istniejące do tylko tych cenników które przetwarzamy
        if self.SELECTED_ZONES:
            zone_names = [row["name"] for row in df_full.select("name").distinct().collect()]
            df_existing = df_existing.filter(F.col("name").isin(zone_names))

        # Klucz porównania
        join_key = ["name", "product_code", "start_date"]

        # 1. NOWE wiersze — nie istnieją w Occubee
        df_new = df_full.join(df_existing.select(*join_key), on=join_key, how="left_anti")
        new_count = df_new.count()
        self.__logger.log(f"DELTA: Nowe wpisy cennikowe: {new_count:,}")

        # 2. ZAKTUALIZOWANE — istnieją, ale end_date się zmieniło
        df_matched = df_full.alias("new").join(
            df_existing.alias("old"),
            on=join_key,
            how="inner"
        ).filter(
            ~F.col("new.end_date").eqNullSafe(F.col("old.end_date"))
        ).select("new.*")

        updated_count = df_matched.count()
        self.__logger.log(f"DELTA: Zaktualizowane wpisy (zmiana end_date): {updated_count:,}")

        # 3. Union nowe + zaktualizowane
        df_delta = df_new.unionByName(df_matched)
        total = new_count + updated_count
        self.__logger.log(f"DELTA: Łącznie do eksportu: {total:,} wierszy")

        return df_delta

    def _write_zone_file(self, df_zone: DataFrame, base_path: str, zone_name: str, write_opts: dict):
        """Zapisuje DF jednej strefy jako pojedynczy plik CSV o czystej nazwie."""
        from py4j.java_gateway import java_import
        java_import(self.spark._jvm, "org.apache.hadoop.fs.Path")
        hadoop_conf = self.spark._jsc.hadoopConfiguration()
        FileSystem = self.spark._jvm.org.apache.hadoop.fs.FileSystem

        temp_dir = self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/_tmp_{zone_name}")
        df_zone.coalesce(1).write.mode("overwrite").options(**write_opts).csv(temp_dir)

        fs = FileSystem.get(self.spark._jvm.java.net.URI(temp_dir), hadoop_conf)
        temp_path = self.spark._jvm.org.apache.hadoop.fs.Path(temp_dir)
        part_files = [f.getPath() for f in fs.listStatus(temp_path) if f.getPath().getName().startswith("part-")]

        if part_files:
            target = self.spark._jvm.org.apache.hadoop.fs.Path(
                self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/{zone_name}.csv")
            )
            if fs.exists(target):
                fs.delete(target, False)
            fs.rename(part_files[0], target)

        fs.delete(temp_path, True)

    def save_results(self, df_final: DataFrame, full_key: str, sample_key: str):
        """Zapisuje wersję FULL i/lub SAMPLE zależnie od OUTPUT_MODE."""
        write_opts = {"header": "true", "sep": ";", "encoding": "UTF-8", "quoteAll": "false", "emptyValue": ""}

        zone_names = [row["name"] for row in df_final.select("name").distinct().orderBy("name").collect()]
        self.__logger.log(f"Zapisywanie {len(zone_names)} cenników: {zone_names}")

        if self.OUTPUT_MODE in ("FULL", "BOTH"):
            base_path_full = self.__script_cfg['output'][full_key]
            full_dir = self.__client_azure_helper.create_azure_path(folder_path=base_path_full)
            self.__logger.log(f"Zapisywanie FULL do: {full_dir}")
            if self.__client_azure_helper.exists(full_dir): self.__client_azure_helper.delete(full_dir, True)

            for zone in zone_names:
                df_zone = df_final.filter(F.col("name") == zone)
                self._write_zone_file(df_zone, base_path_full, zone, write_opts)

            self.__logger.log(f"Sukces! Zapisano {len(zone_names)} plików FULL.")

        if self.OUTPUT_MODE in ("TEST", "BOTH"):
            base_path_sample = self.__script_cfg['output'][sample_key]
            sample_dir = self.__client_azure_helper.create_azure_path(folder_path=base_path_sample)
            self.__logger.log("Zapisywanie próbki reprezentatywnej ze stref...")
            if self.__client_azure_helper.exists(sample_dir): self.__client_azure_helper.delete(sample_dir, True)

            window_sample = Window.partitionBy("name").orderBy(F.rand())
            df_sampled = df_final.withColumn("rn_samp", F.row_number().over(window_sample)) \
                                 .filter(F.col("rn_samp") <= 1000).drop("rn_samp")

            for zone in zone_names:
                df_zone = df_sampled.filter(F.col("name") == zone)
                self._write_zone_file(df_zone, base_path_sample, zone, write_opts)

            self.__logger.log(f"Sukces! Zapisano {len(zone_names)} plików SAMPLE.")


if __name__ == "__main__":
    script_cfg = Configuration()
    cmd_args = GatherArguments(script_cfg["std_cmd_args"]["std_cmd_args"])

    apply_profile(script_cfg, "medium")
    lib_helpers_factory = LibHelpersFactory(config=script_cfg, cmd_args=cmd_args)
    app_name = "Import Cennikow 4F"

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

        importer = PriceImport(spark_helper, azure_helper, client_azure_helper, script_cfg, logger)

        logger.printSection("ETAP 1: Dane referencyjne")
        df_master = importer.load_product_master()
        df_store_base, df_store_retail = importer.prepare_stores()

        logger.printSection("ETAP 2: Przetwarzanie cenników (bazowy + promocyjny)")
        df_combined = importer.process_all_prices(df_master, df_store_base, df_store_retail)

        if importer.IMPORT_MODE == "DELTA":
            logger.printSection("ETAP 3: DELTA — porównanie z Occubee")
            df_combined = importer.apply_delta(df_combined)

        logger.printSection("ETAP 4: Priorytety i zapis")
        df_final = importer.assign_global_priorities(df_combined)
        df_final = df_final.cache()

        row_count = df_final.count()
        zone_count = df_final.select("name").distinct().count()
        logger.log(f"Łącznie {row_count:,} wierszy, {zone_count} encji cennikowych, priorytety 1-{zone_count}")

        importer.save_results(df_final, "out_full", "out_sample")
        df_final.unpersist()

        logger.log("PROCES ZAKOŃCZONY SUKCESEM.")
