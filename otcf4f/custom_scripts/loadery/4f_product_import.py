"""
================================================================================
SKRYPT: Import i transformacja produktow 4F do struktur Occubee
Autor:  Patryk Bubula / Occubee Data Engineering
Wersja: 2.1 (2026-03-19)
================================================================================

OPIS
----
Skrypt przetwarza katalog produktow 4F wraz z atrybutami UDA (User Defined
Attributes), laczy go z informacja o aktywnosci produktow i generuje plik CSV
gotowy do importu slownika produktow w Occubee.

ZMIANY WZGLEDEM WERSJI 1.x
---------------------------
Wersja 2.0 wprowadza nowa logike aktywnosci, walidacje danych wejsciowych
oraz alerty sezonowe:

  1. NOWA LOGIKA AKTYWNOSCI (get_active_products):
     Poprzednia wersja skanowala pelna historie sprzedazy (sales_base) oraz
     stanow sklepowych (store_stock_level) — dwa duze datasety parquet
     z rekursywnym odczytem, co trwalo kilka minut.

     Nowa wersja korzysta z dwoch lzejszych zrodel:
       a) inventory_warehouse.csv — plik CSV z kontenera klienta 'data',
          zawierajacy aktualne stany magazynowe. Jest maly (kilka tysiecy
          wierszy), wiec stosujemy broadcast join.
       b) dictionaries/product — parquet z kontenera Occubee 'bigdata',
          zawierajacy produkty juz zaladowane do systemu.

     Produkt uznawany jest za AKTYWNY, jesli:
       - jego product_code wystepuje w inventory_warehouse.csv (ma stan
         magazynowy → moze byc sprzedawany w przyszlosci), LUB
       - jego product_code juz istnieje w Occubee (dictionaries/product)
         (zostal zaladowany wczesniej → musi byc utrzymany w slowniku).

     Efekt: szybszy import (broadcast na malym pliku), prostsze zrodla,
     a jednoczesnie gwarancja ze istniejace produkty nie znikna z systemu.

  2. ALERTY SEZONOWE (validate_season):
     Po wyznaczeniu zbioru NOWYCH produktow (aktywnych, ale jeszcze
     nieobecnych w Occubee), skrypt analizuje ich kolumne 'season'.

     Format sezonu: "SS26", "AW25", "MM", itp.
       - SS = Spring/Summer, AW = Autumn/Winter — sezon z rokiem (2 cyfry)
       - MM / inne / puste → Basic/NOOS (zawsze akceptowane)

     Parametr ACTIVE_SEASONS_YEARS_BACK (domyslnie 1) okresla ile lat wstecz
     od biezacego roku uznajemy sezon za "aktualny". Przy wartosci 1 i roku
     2026 aktywne sa sezony z 2025 i 2026. Jesli wsrod nowych produktow
     znajduja sie produkty ze starszym sezonem (np. SS22), skrypt loguje
     ostrzezenie WARN. Produkty NIE sa usuwane — to alert informacyjny.

     Przyklad logu:
       "Nowe produkty: 150 | SS26: 120, AW25: 25, SS22: 5 (UWAGA stary sezon!)"

  3. WALIDACJE DANYCH WEJSCIOWYCH (validate_input):
     Zestaw walidacji uruchamianych na surowym pliku product.csv:

       V1: EMPTY FILE — jesli plik jest pusty (0 wierszy), skrypt konczy sie
           bledem krytycznym (sys.exit). Plik mogl nie zostac dostarczony.

       V2: MISSING COLUMNS — jesli brakuje ktorejs z wymaganych kolumn
           (product_code, option_code, size_code, product_description, brand,
           product_group_1_code, product_group_1_description), skrypt konczy
           sie bledem krytycznym. Niepoprawny format pliku.

       V3: NULL product_code — wiersze z pustym product_code sa usuwane
           z dalszego przetwarzania. Logowany jest WARN z liczba usunietych.

       V4: DUPLICATE product_code + size_code — jesli istnieja duplikaty
           kombinacji (product_code, size_code), logowany jest WARN z liczba
           zduplikowanych kombinacji. Wiersze NIE sa usuwane.

       V5: STATYSTYKI — po zlaczeniu z aktywnoscia logowane sa statystyki:
           "Produkty w katalogu: 900k, Aktywne (warehouse+Occubee): 320k,
            Nowe: 150, Pominiete: 580k"

  4. OUTPUT_MODE = "FULL" (bylo "BOTH"):
     Produkcyjnie zapisujemy tylko pelny plik. Tryb SAMPLE/TEST mozna
     wlaczyc recznie zmieniajac OUTPUT_MODE.

PARAMETRY KONFIGUROWALNE (atrybuty klasy)
-----------------------------------------
  OUTPUT_MODE              — "FULL", "TEST", "BOTH"
  ACTIVE_SEASONS_YEARS_BACK — ile lat wstecz uznajemy sezon za aktualny (default: 1)
  CONSTANTS                — wartosci statyczne (unit_of_measure_code, start_selling_date)
  SOURCE_MAPPING           — mapowanie kolumn docelowych na kolumny zrodlowe
  CASTING                  — typy kolumn docelowych
  EXCLUDE                  — kolumny wykluczone z wyjscia
  REQUIRED_COLUMNS         — kolumny wymagane w product.csv (walidacja V2)

ZRODLO DANYCH (INPUT)
---------------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net

  product.csv  (sep=;)
    Katalog produktow. Kolumny: product_code, option_code, product_description,
    brand, model_code, color_code, size_code, product_group_1..4 (kod+opis),
    price_with_tax, tax_rate, cogs, currency, season.

  product_uda.csv  (sep=;)
    Wartosci atrybutow UDA przypisane do produktow.
    Kolumny: product_code, uda_code, uda_value.

  uda.csv  (sep=;)
    Definicje atrybutow UDA (slownik).
    Kolumny: uda_code, uda_description.

  inventory_warehouse.csv  (sep=;)
    Stany magazynowe — maly plik, uzyty do wyznaczenia aktywnosci.
    Kolumna: product_code (+ inne, ale uzywamy tylko product_code).

Kontener: bigdata @ otcf4fprodmainsa.dfs.core.windows.net

  dictionaries/product  (parquet, recursiveFileLookup=true)
    Produkty juz zaladowane do Occubee — uzyty do wyznaczenia aktywnosci.

LOGIKA BIZNESOWA
----------------
1. Wyznaczenie aktywnych produktow:
   Produkt aktywny = ma stan magazynowy (inventory_warehouse.csv) LUB
   juz istnieje w Occubee (dictionaries/product).
   Broadcast join na malym pliku inventory_warehouse.csv.

2. Walidacja danych wejsciowych (V1-V5).

3. Alert sezonowy: nowe produkty ze starym sezonem → WARN w logach.

4. Pivot atrybutow UDA:
   Wartosci UDA sa pivotowane z formatu dlugiego (product_code, attribute, value)
   na format szeroki (jedna kolumna na atrybut). Zduplikowane kolumny
   (te same nazwy co w product.csv) sa usuwane przed zlaczeniem.

5. Filtrowanie grup technicznych:
   Produkty z product_group_1_code = "-1" sa odrzucane (grupy techniczne).

6. Transformacje kodow kategorii:
   Kody grup produktowych sa rozszerzane sufiksem poziomu:
   product_group_1_code → product_group_1_code + "_l1" itd.
   Zapewnia to unikalnosc kodow miedzy poziomami hierarchii.

7. Hierarchia kategorii produktow (5 poziomow):
   - Level 1: Grupa 1 (np. "ALL PRODUCTS")
   - Level 2: Grupa 2 (np. "TEXTILE")
   - Level 3: Grupa 3 (np. "TROUSERS")
   - Level 4: Grupa 4 (np. "TROUSERS CAS")
   - Level 5: Kombinacja atrybutu UDA sex_department + Level 4
              (np. "GIRL TROUSERS CAS")
   Kody Level 5 generowane deterministycznie przez CRC32 pelnej sciezki.

8. SKU: generowany jako option_code + "_" + size_code.

9. Dynamiczne UDA: kolumny UDA niekolidujace z polami standardowymi
   sa doklejane jako dodatkowe kolumny w pliku wynikowym.

WYJSCIE (OUTPUT)
----------------
Kontener: data @ 4fprodmainsa.dfs.core.windows.net
Pliki:    TRANSFORMED_DATA/PRODUKTY/product.csv        (OUTPUT_MODE=FULL)
          TRANSFORMED_DATA/PRODUKTY/product_SAMPLE.csv (OUTPUT_MODE=TEST)
Format:   CSV, sep=;, encoding=UTF-8, emptyValue=""

URUCHOMIENIE
------------
/opt/spark3/bin/spark-submit 4f_product_import.py \\
  -s otcf4fprodmainsa.dfs.core.windows.net \\
  -k <OCCUBEE_KEY> -c bigdata \\
  -p <process_instance_id> \\
  -v spark_script_configs/<config>.json
================================================================================
"""

import sys, re
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DateType, StringType, IntegerType
from pyspark.sql import DataFrame

# Importy z biblioteki Occubee
from occubee.common import (GatherArguments, Configuration, LibHelpersFactory,
                            SparkWrapper, AzureHelper, SparkHelper, Logger)
from lib.parameters import Parameters
from lib.spark_profiles import apply_profile


class ProductImport:
    # ==========================================================================
    # TRYB ZAPISU: "TEST" (probka 1000 wierszy), "FULL" (calosc), "BOTH" (oba)
    # ==========================================================================
    OUTPUT_MODE = "FULL"

    # ==========================================================================
    # TRYB IMPORTU:
    #   "FULL"  — eksportuje WSZYSTKIE aktywne produkty (pelny plik, ~341k)
    #   "DELTA" — eksportuje TYLKO NOWE produkty (nieobecne w Occubee, ~17k)
    # W trybie DELTA istniejace produkty nie sa nadpisywane — Occubee upsertuje
    # po external_id, wiec pominiecie istniejacych jest bezpieczne.
    # ==========================================================================
    IMPORT_MODE = "FULL"

    # ==========================================================================
    # PARAMETR: Ile lat wstecz uznajemy sezon za aktualny (alert, nie filtr)
    # Przy wartosci 1 i roku 2026, aktywne sa sezony z lat 2025-2026.
    # ==========================================================================
    ACTIVE_SEASONS_YEARS_BACK = 1

    # --- 1. SLOWNIKI I MAPOWANIA ---
    SOURCE_MAPPING = {
        "external_id": "product_code", "code": "product_code", "name": "option_code",
        "parent_product_code": "option_code", "parent_product_name": "product_description",
        "product_brand_name": "brand", "product_brand_code": "brand",
        "category_level1_name": "product_group_1_description", "category_level1_code": "product_group_1_code",
        "category_level2_name": "product_group_2_description", "category_level2_code": "product_group_2_code",
        "category_level3_name": "product_group_3_description", "category_level3_code": "product_group_3_code",
        "category_level4_name": "product_group_4_description", "category_level4_code": "product_group_4_code",
        "is_active": "is_active_calc",
        "store_department": "storedepartment",
        "salesfloor_segment": "salesfloorsegmentqty",
        "stockroom_segment": "stockroomsegmentqty",
    }

    CONSTANTS = {"unit_of_measure_code": "ST", "start_selling_date": "2023-01-01"}

    CASTING = {
        "external_id": StringType(), "code": StringType(), "name": StringType(),
        "unit_of_measure_code": StringType(), "parent_product_code": StringType(),
        "parent_product_name": StringType(),
        "start_selling_date": DateType(), "product_brand_name": StringType(),
        "product_brand_code": StringType(), "tax_rate": DecimalType(7, 4),
        "is_active": IntegerType(), "category_level1_name": StringType(),
        "category_level1_code": StringType(), "category_level2_name": StringType(),
        "category_level2_code": StringType(), "category_level3_name": StringType(),
        "category_level3_code": StringType(), "category_level4_name": StringType(),
        "category_level4_code": StringType(), "category_level5_name": StringType(),
        "category_level5_code": StringType(), "model_code": StringType(),
        "size_code": StringType(), "color_code": StringType(),
        "price_with_tax": DecimalType(10, 2), "season": StringType(),
        "cogs": DecimalType(10, 2), "currency": StringType(),
        "sku": StringType(),
        "store_department": StringType(),
        "salesfloor_segment": IntegerType(),
        "stockroom_segment": IntegerType(),
    }

    EXCLUDE = [
        "end_selling_date", "description", "product_type_code", "product_type_name",
        "start_warehouse_accepted_date", "end_warehouse_accepted_date", "width", "depth", "height", "area", "capacity"
    ]

    REQUIRED_COLUMNS = [
        "product_code", "option_code", "size_code", "product_description",
        "brand", "product_group_1_code", "product_group_1_description"
    ]

    def __init__(self, spark_helper: SparkHelper, azure_helper: AzureHelper, client_azure_helper: AzureHelper,
                 script_cfg: Configuration, logger: Logger):
        self.__spark_helper = spark_helper
        self.__azure_helper = azure_helper
        self.__client_azure_helper = client_azure_helper
        self.__script_cfg = script_cfg
        self.__logger = logger

        # Inicjalizacja atrybutow dla kolumn UDA
        self.uda_columns_original = []
        self.cols_to_drop = []

    # ==========================================================================
    # ETAP 1: AKTYWNOSC PRODUKTOW (nowa logika v2)
    # ==========================================================================
    def get_active_products(self) -> tuple:
        """
        Wyznacza zbior aktywnych produktow na podstawie:
          a) inventory_warehouse.csv (kontener klienta 'data') — stany magazynowe
          b) inventory_*.csv z DATA_IN/STANY_SKLEPOWE/ (najnowszy) — stany sklepowe
          c) dictionaries/product (kontener Occubee 'bigdata') — produkty juz w Occubee

        Produkt AKTYWNY = ma stan w magazynie (a) LUB w sklepach (b) LUB juz w Occubee (c).
        Dzieki (b) nie umkna produkty calkowicie rozeslane do sklepow (magazyn=0).

        Zwraca tuple:
          (active_refs: DataFrame[product_code, is_active_calc],
           occubee_product_codes: DataFrame[product_code])
        """
        self.__logger.log("Wczytywanie zrodel aktywnosci...")

        spark = self.__spark_helper.spark
        sa_name = self.__azure_helper.storage_account_name

        # --- a) inventory_warehouse.csv (kontener klienta 'data', maly plik) ---
        wh_path = self.__client_azure_helper.create_azure_path(
            folder_path=self.__script_cfg['input']['raw_warehouse_inventory']
        )
        self.__logger.log(f"  Odczyt inventory_warehouse.csv: {wh_path}")
        df_warehouse = spark.read.options(header="true", sep="|").csv(wh_path)
        df_warehouse = df_warehouse.select("product_code").distinct()
        wh_count = df_warehouse.count()
        self.__logger.log(f"  Unikalne product_code w magazynie: {wh_count}")

        # --- b) Najnowszy plik stanow sklepowych (inventory_*.csv) ---
        store_inv_folder = self.__script_cfg['input'].get('raw_store_inventory_folder', '')
        df_store_inv = spark.createDataFrame([], "product_code: string")
        store_inv_count = 0

        if store_inv_folder:
            try:
                store_inv_path = self.__client_azure_helper.create_azure_path(
                    folder_path=store_inv_folder
                )
                # Listowanie plikow w folderze i wybranie najnowszego (alfabetycznie ostatni)
                hadoop_conf = spark._jsc.hadoopConfiguration()
                fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark._jvm.java.net.URI(store_inv_path), hadoop_conf
                )
                folder = spark._jvm.org.apache.hadoop.fs.Path(store_inv_path)
                files = [
                    f.getPath().getName() for f in fs.listStatus(folder)
                    if f.getPath().getName().startswith("inventory_") and f.getPath().getName().endswith(".csv")
                ]
                if files:
                    latest_file = sorted(files)[-1]
                    latest_path = self.__client_azure_helper.create_azure_path(
                        folder_path=f"{store_inv_folder}/{latest_file}"
                    )
                    self.__logger.log(f"  Odczyt stanow sklepowych: {latest_file}")
                    df_store_inv = spark.read.options(header="true", sep=";").csv(latest_path)
                    df_store_inv = df_store_inv.select("product_code").distinct()
                    store_inv_count = df_store_inv.count()
                    self.__logger.log(f"  Unikalne product_code w sklepach: {store_inv_count}")
                else:
                    self.__logger.log("  INFO: Brak plikow inventory_*.csv w folderze stanow sklepowych")
            except Exception as e:
                self.__logger.log(f"  WARN: Nie udalo sie odczytac stanow sklepowych ({e.__class__.__name__}: {e}). "
                                  "Kontynuacja bez stanow sklepowych.")

        # --- c) dictionaries/product (kontener Occubee 'bigdata', parquet) ---
        product_dict_path = f"abfs://bigdata@{sa_name}/{self.__script_cfg['input']['product_dict']}"
        self.__logger.log(f"  Odczyt dictionaries/product: {product_dict_path}")
        try:
            df_occubee = spark.read.option("recursiveFileLookup", "true").parquet(product_dict_path)

            # Kolumna z kodem produktu moze sie nazywac 'product_code' lub 'code' lub 'external_id'
            if "product_code" in df_occubee.columns:
                df_occubee_codes = df_occubee.select("product_code").distinct()
            elif "external_id" in df_occubee.columns:
                df_occubee_codes = df_occubee.select(F.col("external_id").alias("product_code")).distinct()
            elif "code" in df_occubee.columns:
                df_occubee_codes = df_occubee.select(F.col("code").alias("product_code")).distinct()
            else:
                self.__logger.log("  WARN: Nie znaleziono kolumny product_code w dictionaries/product. "
                                  "Uzycie tylko warehouse + store inventory.")
                df_occubee_codes = spark.createDataFrame([], "product_code: string")
        except Exception as e:
            self.__logger.log(f"  WARN: Nie udalo sie odczytac dictionaries/product ({e.__class__.__name__}). "
                              "Pierwszy import? Uzycie tylko warehouse + store inventory.")
            df_occubee_codes = spark.createDataFrame([], "product_code: string")

        occ_count = df_occubee_codes.count()
        self.__logger.log(f"  Unikalne product_code w Occubee: {occ_count}")

        # --- Union: aktywne = warehouse OR sklepy OR Occubee ---
        active_refs = df_warehouse.union(df_store_inv).union(df_occubee_codes).distinct() \
            .withColumn("is_active_calc", F.lit(1))

        total_active = active_refs.count()
        self.__logger.log(f"  Laczna liczba aktywnych product_code: {total_active} "
                          f"(magazyn: {wh_count}, sklepy: {store_inv_count}, Occubee: {occ_count})")

        return active_refs, df_occubee_codes

    # ==========================================================================
    # WALIDACJA DANYCH WEJSCIOWYCH
    # ==========================================================================
    def validate_input(self, df_product: DataFrame) -> DataFrame:
        """
        Walidacje V1-V3 na surowym pliku product.csv.
        Zwraca oczyszczony DataFrame (po usunieciu wierszy z NULL product_code).

        V1: Pusty plik → CRITICAL, sys.exit(1)
        V2: Brakujace kolumny → CRITICAL, sys.exit(1)
        V3: NULL product_code → WARN + drop
        """
        # --- V1: Pusty plik ---
        row_count = df_product.count()
        if row_count == 0:
            self.__logger.log("CRITICAL [V1]: Plik product.csv jest PUSTY (0 wierszy). "
                              "Sprawdz dostawe plikow od klienta!")
            sys.exit(1)
        self.__logger.log(f"  V1 OK: product.csv zawiera {row_count} wierszy")

        # --- V2: Brakujace kolumny ---
        existing_cols = set(df_product.columns)
        missing = [c for c in self.REQUIRED_COLUMNS if c not in existing_cols]
        if missing:
            self.__logger.log(f"CRITICAL [V2]: Brakujace kolumny w product.csv: {missing}. "
                              f"Dostepne kolumny: {sorted(existing_cols)}")
            sys.exit(1)
        self.__logger.log(f"  V2 OK: Wszystkie wymagane kolumny obecne")

        # --- V3: NULL product_code ---
        null_count = df_product.filter(F.col("product_code").isNull() | (F.trim(F.col("product_code")) == "")).count()
        if null_count > 0:
            self.__logger.log(f"  WARN [V3]: {null_count} wierszy z pustym product_code — usuwam z przetwarzania")
            df_product = df_product.filter(F.col("product_code").isNotNull() & (F.trim(F.col("product_code")) != ""))
        else:
            self.__logger.log(f"  V3 OK: Brak wierszy z pustym product_code")

        return df_product

    # ==========================================================================
    # ALERT SEZONOWY + STATYSTYKI (V5)
    # ==========================================================================
    def validate_season(self, df_active: DataFrame, occubee_codes: DataFrame, total_catalog: int):
        """
        Loguje statystyki (V5) i alerty sezonowe dla nowych produktow.

        Parametry:
          df_active       — DataFrame po INNER JOIN z active_refs (aktywne produkty)
          occubee_codes   — DataFrame[product_code] produktow juz w Occubee
          total_catalog   — calkowita liczba produktow w katalogu (przed filtrem aktywnosci)
        """
        active_count = df_active.count()
        skipped = total_catalog - active_count

        # Nowe produkty = aktywne MINUS te co juz sa w Occubee
        df_new = df_active.join(occubee_codes, "product_code", "left_anti")
        new_count = df_new.count()

        self.__logger.log(
            f"  V5 STATS: Produkty w katalogu: {total_catalog}, "
            f"Aktywne (warehouse+Occubee): {active_count}, "
            f"Nowe: {new_count}, Pominiete: {skipped}"
        )

        # --- V6: Alert StoreDepartment (aktywne produkty bez przypisanego departamentu) ---
        if "storedepartment" in df_active.columns:
            no_dept = df_active.filter(
                (F.col("storedepartment").isNull()) |
                (F.trim(F.col("storedepartment")) == "") |
                (F.trim(F.col("storedepartment")) == "0")
            ).count()
            with_dept = active_count - no_dept
            self.__logger.log(
                f"  V6 STORE_DEPT: Aktywne product_code z departamentem: {with_dept}, "
                f"bez departamentu (0/puste): {no_dept}"
            )
            if no_dept > 0:
                self.__logger.log(
                    f"  WARN [V6]: {no_dept} aktywnych product_code bez StoreDepartment — "
                    f"nie beda przypisane do sekcji w sklepach"
                )

        # --- Alert sezonowy (tylko dla nowych produktow) ---
        if new_count == 0 or "season" not in df_active.columns:
            self.__logger.log("  Brak nowych produktow lub brak kolumny 'season' — pomijam alert sezonowy")
            return

        current_year = datetime.now().year
        min_year = current_year - self.ACTIVE_SEASONS_YEARS_BACK

        # Statystyki per sezon (na poziomie product_code — size-level)
        season_stats = df_new.groupBy("season").count().orderBy(F.desc("count")).collect()

        parts = []
        for row in season_stats:
            season_val = row["season"] or ""
            cnt = row["count"]

            # Parsowanie sezonu: SS26, AW25, MM, itp.
            match = re.match(r'^(SS|AW)(\d{2})$', season_val.strip().upper())
            if match:
                prefix, year_2d = match.group(1), int(match.group(2))
                full_year = 2000 + year_2d
                if full_year < min_year:
                    parts.append(f"{season_val}: {cnt} (UWAGA stary sezon!)")
                else:
                    parts.append(f"{season_val}: {cnt}")
            else:
                # MM / Basic / NOOS / puste — zawsze OK
                label = season_val if season_val else "BRAK"
                parts.append(f"{label}: {cnt}")

        season_line = ", ".join(parts)
        self.__logger.log(f"  Nowe produkty: {new_count} | {season_line}")

    # ==========================================================================
    # ETAP 2: PIVOT UDA + JOIN
    # ==========================================================================
    def process_uda_and_join(self, active_refs: DataFrame) -> DataFrame:
        """Wczytywanie surowych plikow, pivotowanie UDA i laczenie danych"""
        self.__logger.log("Wczytywanie surowych plikow produktowych z kontenera 'data'...")

        opts = {"header": "true", "sep": ";"}
        spark = self.__spark_helper.spark

        # Pobieranie z kontenera klienta 'data'
        p_path = self.__client_azure_helper.create_azure_path(folder_path=self.__script_cfg['input']['raw_product'])
        uda_val_path = self.__client_azure_helper.create_azure_path(folder_path=self.__script_cfg['input']['raw_product_uda'])
        uda_def_path = self.__client_azure_helper.create_azure_path(folder_path=self.__script_cfg['input']['raw_uda'])

        df_p = spark.read.options(**opts).csv(p_path)
        df_uda_val = spark.read.options(**opts).csv(uda_val_path)
        df_uda_def = spark.read.options(**opts).csv(uda_def_path)

        # Czyszczenie naglowkow
        df_p = df_p.toDF(*[c.strip().lower() for c in df_p.columns])

        # --- Walidacja danych wejsciowych (V1-V4) ---
        df_p = self.validate_input(df_p)
        total_catalog = df_p.count()

        self.__logger.log("Pivotowanie cech UDA...")
        df_piv = df_uda_val.join(F.broadcast(df_uda_def), "uda_code") \
            .groupBy("product_code").pivot("uda_description").agg(F.first("uda_value"))

        self.uda_columns_original = [c for c in df_piv.columns if c != "product_code"]

        # Usuwanie duplikatow kolumn (Ambiguous)
        cols_to_drop = [c for c in df_piv.columns if c.lower() in df_p.columns and c != "product_code"]
        df_piv = df_piv.drop(*cols_to_drop)
        self.cols_to_drop = cols_to_drop

        self.__logger.log("Glowne zlaczenie i filtrowanie technicznej grupy (-1)...")
        df_full = df_p.join(df_piv, "product_code", "left") \
            .join(active_refs, "product_code", "inner") \
            .filter(F.col("product_group_1_code") != "-1")

        # --- V4: Duplikaty product_code po filtrze aktywnosci ---
        before_dedup = df_full.count()
        df_full = df_full.dropDuplicates(["product_code"])
        after_dedup = df_full.count()
        dup_count = before_dedup - after_dedup
        if dup_count > 0:
            self.__logger.log(f"  WARN [V4]: {dup_count} zduplikowanych product_code wsrod aktywnych — usunieto")
        else:
            self.__logger.log(f"  V4 OK: Brak duplikatow product_code wsrod aktywnych")

        # Transformacje
        for i in range(1, 6):
            c = f"product_group_{i}_code"
            if c in df_full.columns:
                df_full = df_full.withColumn(c, F.concat(F.col(c), F.lit(f"_l{i}")))

        for c in ["tax_rate", "price_with_tax", "cogs"]:
            df_full = df_full.withColumn(c, F.regexp_replace(F.regexp_replace(F.col(c), r"[^0-9,.-]", ""), ",", ".").cast("double"))

        return df_full, total_catalog

    def add_sku(self, df_full: DataFrame) -> DataFrame:
        return df_full.withColumn(
            "sku",
            F.concat(F.col("option_code"), F.lit("_"), F.col("size_code"))
        )

    def add_level5_category(self, df_full: DataFrame) -> DataFrame:
        def _clean(c):
            return re.sub(r'_+', '_', re.sub(r'[^a-zA-Z0-9]', '_', str(c)).lower()).strip('_')

        sex_uda_col = next((c for c in df_full.columns if _clean(c) == "sex_department"), None)
        self.__logger.log(f"Znaleziona kolumna UDA dla sex_department: {sex_uda_col!r}")

        sex_col = F.trim(F.col(sex_uda_col)) if sex_uda_col else F.lit(None)
        level5_name = F.concat_ws(" ", sex_col, F.trim(F.col("product_group_4_description")))
        level5_code = F.concat(
            F.lpad(F.abs(F.crc32(level5_name)).cast(StringType()), 10, "0"),
            F.lit("_l5")
        )

        return df_full \
            .withColumn("category_level5_name", level5_name) \
            .withColumn("category_level5_code", level5_code)

    # ==========================================================================
    # WALIDACJA SPOJNOSCI KATEGORII NA POZIOMIE PARENTA
    # ==========================================================================
    def fix_parent_category_conflicts(self, df: DataFrame) -> DataFrame:
        """
        Occubee wymaga, by wszystkie childy tego samego parent_product_code
        (= option_code) mialy identyczne kategorie (L1-L5).

        Jesli sex_department jest niespojny (np. WOMAN vs NONE) dla roznych
        rozmiarow tego samego option_code, generowany L5 bedzie inny
        i import sie nie powiedzie.

        Rozwiazanie: dla kazdego option_code wyznacz dominujaca wartosc
        kazdej kolumny kategorii (majority wins). Nadpisz outliers.
        """
        cat_cols = []
        for i in range(1, 6):
            desc = f"product_group_{i}_description"
            code = f"product_group_{i}_code"
            if desc in df.columns:
                cat_cols.append(desc)
            if code in df.columns:
                cat_cols.append(code)
        if "category_level5_name" in df.columns:
            cat_cols.append("category_level5_name")
        if "category_level5_code" in df.columns:
            cat_cols.append("category_level5_code")

        if not cat_cols:
            return df

        # Znajdz parentow z niespojnymi kategoriami
        from pyspark.sql import Window
        conflict_check = df.groupBy("option_code").agg(
            *[F.countDistinct(F.col(c)).alias(f"_cd_{c}") for c in cat_cols]
        )
        conflict_filter = None
        for c in cat_cols:
            cond = F.col(f"_cd_{c}") > 1
            conflict_filter = cond if conflict_filter is None else (conflict_filter | cond)

        conflicting_parents = conflict_check.filter(conflict_filter)
        conflict_count = conflicting_parents.count()

        if conflict_count == 0:
            self.__logger.log("Walidacja parent-category: OK, brak konfliktow.")
            return df

        self.__logger.log(
            f"WARN: {conflict_count} parentow z niespojnymi kategoriami — naprawiam (majority wins)."
        )

        # Dla kazdej kolumny kategorii: dominujaca wartosc per option_code
        dominant = df.groupBy("option_code").agg(
            *[F.mode(F.col(c)).alias(f"_dom_{c}") for c in cat_cols]
        )

        # Join i nadpisanie
        df = df.join(F.broadcast(dominant), "option_code", "left")
        for c in cat_cols:
            df = df.withColumn(c, F.coalesce(F.col(f"_dom_{c}"), F.col(c)))
        df = df.drop(*[f"_dom_{c}" for c in cat_cols])

        # Weryfikacja
        recheck = df.groupBy("option_code").agg(
            *[F.countDistinct(F.col(c)).alias(f"_cd_{c}") for c in cat_cols]
        )
        recheck_filter = None
        for c in cat_cols:
            cond = F.col(f"_cd_{c}") > 1
            recheck_filter = cond if recheck_filter is None else (recheck_filter | cond)
        remaining = recheck.filter(recheck_filter).count()
        self.__logger.log(f"Po naprawie: {remaining} parentow nadal z konfliktami (powinno byc 0).")

        return df

    def map_columns(self, df_full: DataFrame) -> list:
        """Dynamiczne mapowanie kolumn standardowych i doklejanie UDA"""
        self.__logger.log("Przygotowywanie mapowania kolumn...")
        final_exprs = []
        added_cols = set()

        for target, cast_type in self.CASTING.items():
            if target in self.EXCLUDE: continue
            added_cols.add(target)

            if target in self.CONSTANTS:
                expr = F.lit(self.CONSTANTS[target])
            elif target in self.SOURCE_MAPPING:
                src = self.SOURCE_MAPPING[target]
                expr = F.col(src) if src in df_full.columns else F.lit(None)
            elif target in df_full.columns:
                expr = F.col(target)
            else:
                expr = F.lit(None)

            final_exprs.append(expr.cast(cast_type).alias(target))

        for uda_col in self.uda_columns_original:
            clean_name = re.sub(r'[^a-zA-Z0-9]', '_', str(uda_col)).lower()
            clean_name = re.sub(r'_+', '_', clean_name).strip('_')

            if clean_name not in added_cols and uda_col not in self.cols_to_drop:
                final_exprs.append(F.col(uda_col).alias(clean_name))
                added_cols.add(clean_name)

        return final_exprs

    def _write_single_file(self, df: DataFrame, base_path: str, file_name: str, write_opts: dict):
        """Zapisuje DF jako pojedynczy plik CSV o czystej nazwie (Hadoop rename)."""
        spark = self.__spark_helper.spark
        from py4j.java_gateway import java_import
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem

        temp_dir = self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/_tmp_product")
        df.repartition(1).write.mode("overwrite").options(**write_opts).csv(temp_dir)

        fs = FileSystem.get(spark._jvm.java.net.URI(temp_dir), hadoop_conf)
        temp_path = spark._jvm.org.apache.hadoop.fs.Path(temp_dir)
        part_files = [f.getPath() for f in fs.listStatus(temp_path) if f.getPath().getName().startswith("part-")]

        if part_files:
            target = spark._jvm.org.apache.hadoop.fs.Path(
                self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/{file_name}")
            )
            if fs.exists(target):
                fs.delete(target, False)
            fs.rename(part_files[0], target)

        fs.delete(temp_path, True)

    def save_and_move_file(self, df: DataFrame, cols: list, occubee_codes: DataFrame = None) -> str:
        """Zapis splaszczonego pliku do Azure Data Lake (kontener klienta: data@4fprodmainsa)"""
        write_opts = {"header": "true", "sep": ";", "encoding": "UTF-8", "quoteAll": "false", "emptyValue": ""}

        df_prepared = df.select(*cols).orderBy("parent_product_code").fillna("")

        # --- Tryb DELTA: filtruj tylko nowe produkty (anti-join z Occubee) ---
        if self.IMPORT_MODE == "DELTA" and occubee_codes is not None and occubee_codes.count() > 0:
            total_before = df_prepared.count()
            df_prepared = df_prepared.join(
                occubee_codes.withColumnRenamed("product_code", "external_id"),
                on="external_id", how="left_anti"
            )
            total_after = df_prepared.count()
            self.__logger.log(f"DELTA MODE: {total_before} -> {total_after} "
                              f"(pominieto {total_before - total_after} istniejacych w Occubee)")
        else:
            self.__logger.log(f"FULL MODE: eksport wszystkich aktywnych produktow")

        result = ""
        if self.OUTPUT_MODE in ("FULL", "BOTH"):
            base_path = self.__script_cfg['output']['result_full_path']
            file_name = self.__script_cfg['output']['result_full_filename']
            self.__logger.log(f"Zapisywanie FULL: {base_path}/{file_name}")
            self._write_single_file(df_prepared, base_path, file_name, write_opts)
            result = f"{base_path}/{file_name}"
            self.__logger.log(f"Sukces! Zapisano FULL.")

        if self.OUTPUT_MODE in ("TEST", "BOTH"):
            base_path = self.__script_cfg['output']['result_sample_path']
            file_name = self.__script_cfg['output']['result_sample_filename']
            self.__logger.log(f"Zapisywanie SAMPLE: {base_path}/{file_name}")
            self._write_single_file(df_prepared.limit(1000), base_path, file_name, write_opts)
            result = f"{base_path}/{file_name}"
            self.__logger.log(f"Sukces! Zapisano probke.")

        return result


if __name__ == "__main__":
    # 1. Inicjalizacja obiektow pomocniczych Occubee
    script_cfg = Configuration()
    cmd_args = GatherArguments(script_cfg["std_cmd_args"]["std_cmd_args"])

    apply_profile(script_cfg, "medium")
    lib_helpers_factory = LibHelpersFactory(config=script_cfg, cmd_args=cmd_args)
    app_name = "Import Produktow 4F"

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

        # 2. Uruchomienie Pipeline'u Produktowego
        importer = ProductImport(spark_helper, azure_helper, client_azure_helper, script_cfg, logger)

        logger.printSection("ETAP 1: Aktywnosc Produktow (warehouse + Occubee)")
        active_refs, occubee_codes = importer.get_active_products()
        active_refs = active_refs.cache()

        logger.printSection("ETAP 2: Pivotowanie i Transformacja UDA")
        transformed_df, total_catalog = importer.process_uda_and_join(active_refs)
        transformed_df = importer.add_sku(transformed_df)
        transformed_df = importer.add_level5_category(transformed_df)
        transformed_df = importer.fix_parent_category_conflicts(transformed_df)

        logger.printSection("ETAP 3: Walidacja i Statystyki")
        importer.validate_season(transformed_df, occubee_codes, total_catalog)

        logger.printSection("ETAP 4: Mapowanie Kolumn")
        mapped_cols = importer.map_columns(transformed_df)

        logger.printSection("ETAP 5: Zapis Danych")
        output_path = importer.save_and_move_file(transformed_df, mapped_cols, occubee_codes)

        logger.log(f"PROCES ZAKONCZONY SUKCESEM. Plik docelowy: {output_path}")
