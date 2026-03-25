"""
================================================================================
SKRYPT: Import i transformacja sklepow 4F do struktur Occubee (v2)
Autor:  Patryk Bubula / Occubee Data Engineering
Data:   2026-03-17
================================================================================

OPIS
----
Skrypt pobiera surowy plik sklepow dostarczony przez klienta 4F z Azure Data Lake,
wykonuje transformacje biznesowe i generuje plik CSV zgodny ze struktura importu
slownika sklepow (MOS) platformy Occubee.

Wersja 2 wprowadza:
  - Zmiane pliku zrodlowego z store_with_community_id.csv na store.csv
    (community_id jest teraz kolumna opcjonalna w store.csv)
  - Obsluge pliku store_activity_calendar.csv (daty otwarcia/zamkniecia sklepow)
  - Obsluge 5 kolumn departamentow (mos_attribute_1..5)
  - Framework walidacji z poziomami CRITICAL / WARN
  - Lepsza obsluge brakujacych kolumn (graceful degradation)

ZRODLO DANYCH (INPUT)
---------------------
Kontener:  data @ 4fprodmainsa.dfs.core.windows.net

  store.csv  (sep=;)
    Glowny plik sklepow. Wymagane kolumny: store_code, store_description.
    Opcjonalne kolumny: country, building_type, gross_area, city, zip,
    longitude, latitude, class, first_price, actual_price, segment,
    community_id, mos_attribute_1..5.

  store_activity_calendar.csv  (sep=;)  [OPCJONALNY]
    Kalendarz aktywnosci sklepow z datami otwarcia/zamkniecia.
    Kolumny: store_code, opening_date, closing_date.
    Jezeli plik istnieje, jest LEFT JOIN-owany z store.csv i uzupelnia
    daty opening_date / closing_date TYLKO tam, gdzie store.csv ma je puste.
    Jezeli plik nie istnieje, skrypt kontynuuje bez niego.

LOGIKA BIZNESOWA
----------------
1. Walidacja danych wejsciowych:
   - V1: Pusty plik store.csv -> CRITICAL ERROR, przerwanie skryptu.
   - V2: Brak wymaganych kolumn (store_code, store_description) -> CRITICAL ERROR.
   - V3: Wiersze z NULL w store_code -> WARN, usuwane z przetwarzania.
   - V4: Sklepy bez kraju -> WARN (beda zmapowane na "Eshop" i odfiltrowane).
   Po walidacji wyswietlana jest statystyka:
   "Sklepy: total {N}, aktywne: {M}, zamkniete: {K}, odfiltrowane Eshop: {J}"

2. Standaryzacja kodow krajow:
   - Puste / NULL  -> "Eshop"
   - "PO*"         -> "PL"   (kod polski w starym systemie ERP klienta)
   - "SV*"         -> "SK"   (kod slowacki w starym systemie ERP klienta)
   - Pozostale     -> pierwsze 2 znaki (uppercase), standard ISO 3166-1 alpha-2

3. Filtrowanie sklepow online:
   Sklepy z country = "Eshop" sa zarzadzane recznie w Occubee i sa usuwane
   z pliku importu (filtr: country != "Eshop").

4. Flagi aktywnosci (jezeli kolumna 'class' jest obecna):
   - is_active = 1  gdy class != "ZAMKNIETE"
   - is_closed = 1  gdy class == "ZAMKNIETE"

5. Uzupelnienie dat z kalendarza aktywnosci:
   Jezeli store_activity_calendar.csv istnieje, daty opening_date i closing_date
   sa uzupelniane z kalendarza TYLKO tam, gdzie store.csv ma je puste/NULL.
   Priorytet: dane z store.csv > dane z kalendarza.

6. Obsluga community_id:
   Jezeli kolumna community_id istnieje w store.csv, jest przekazywana dalej.
   Jezeli nie istnieje, skrypt laduje mapowanie store_code->community_id
   z pliku store_with_community_id.csv (cfg: input.community_mapping)
   i dolacza LEFT JOIN po store_code. Jezeli plik nie istnieje — NULL.

7. Kolumny departamentow (mos_attribute_1..5):
   Jezeli kolumny mos_attribute_1 przez mos_attribute_5 istnieja w store.csv,
   sa przekazywane do pliku wynikowego. Mapowanie nazw:
     mos_attribute_1 = Meski
     mos_attribute_2 = Damski
     mos_attribute_3 = Junior
     mos_attribute_4 = Akcesoria
     mos_attribute_5 = Obuwie
   Wartosci w kolumnach to kody kategorii departamentow: A/B/C/D.
   Jezeli kolumny nie istnieja, logowany jest komunikat i skrypt kontynuuje.

8. Hierarchia kategorii sklepow (4 poziomy):
   - Level 1: Kraj (pelna polska nazwa, np. "Polska", "Czechy")
   - Level 2: Segment handlowy (np. "SPORT", "FASHION")
   - Level 3: Typ budynku / kanal sprzedazy (building_type)
   - Level 4: Kod sklepu (store_code)
   Kody kazdego poziomu sa generowane deterministycznie z SHA-256
   pelnej sciezki hierarchii (concat_ws "|"), co gwarantuje stabilnosc
   kodow miedzy kolejnymi importami.

WYJSCIE (OUTPUT)
----------------
Kontener:  data @ 4fprodmainsa.dfs.core.windows.net
Plik:      TRANSFORMED_DATA/SKLEPY/store.csv        (OUTPUT_MODE=FULL)
           TRANSFORMED_DATA/SKLEPY/store_SAMPLE.csv (OUTPUT_MODE=TEST)
Format:    CSV, sep=;, encoding=UTF-8, naglowek=tak

URUCHOMIENIE
------------
/opt/spark3/bin/spark-submit 4f_store_import.py \\
  -s otcf4fprodmainsa.dfs.core.windows.net \\
  -k <OCCUBEE_KEY> -c bigdata \\
  -p <process_instance_id> \\
  -v spark_script_configs/<config>.json
================================================================================
"""

import sys
from pyspark.sql import Column, functions as F
from pyspark.sql.types import *
from pyspark.sql import DataFrame
from occubee.common import (GatherArguments, Configuration, LibHelpersFactory,
                            SparkWrapper, DictionaryAzureHelper, SparkHelper,
                            AzureHelper, Logger)
from lib.client_data_access_layer import ClientDataAccessLayer
from lib.parameters import Parameters
from lib.spark_profiles import apply_profile


class StoreImport:
    # ==========================================================================
    # TRYB ZAPISU: "TEST" (probka 1000 wierszy), "FULL" (calosc), "BOTH" (oba)
    # ==========================================================================
    OUTPUT_MODE = "FULL"

    ORDER = ["external_id", "code", "name", "country_symbol", "market_code", "sales_channel_code",
             "opening_date", "closing_date", "is_active", "is_closed", "surface_area", "city_name", "zip_code",
             "longitude", "latitude"]
    EXTRA = ["channel_type", "local_currency", "first_price", "actual_price", "segment", "community_id",
             "category_level1_name", "category_level1_code",
             "category_level2_name", "category_level2_code",
             "category_level3_name", "category_level3_code",
             "category_level4_name", "category_level4_code"]
    MOS_ATTRIBUTES = ["mos_attribute_1", "mos_attribute_2", "mos_attribute_3",
                      "mos_attribute_4", "mos_attribute_5"]
    MOS_ATTRIBUTE_NAMES = {
        "mos_attribute_1": "Meski",
        "mos_attribute_2": "Damski",
        "mos_attribute_3": "Junior",
        "mos_attribute_4": "Akcesoria",
        "mos_attribute_5": "Obuwie",
    }
    MAP = {"external_id": "store_code", "code": "store_code", "name": "store_description", "country_symbol": "country",
           "market_code": "market_zone", "sales_channel_code": "building_type", "surface_area": "gross_area",
           "city_name": "city", "zip_code": "zip"}
    CASTS = {"surface_area": DecimalType(15, 2), "longitude": DecimalType(13, 6), "latitude": DecimalType(13, 6),
             "opening_date": DateType(), "closing_date": DateType(), "is_active": IntegerType(),
             "is_closed": IntegerType(), "community_id": IntegerType(),
             "category_level1_name": StringType(), "category_level1_code": StringType(),
             "category_level2_name": StringType(), "category_level2_code": StringType(),
             "category_level3_name": StringType(), "category_level3_code": StringType(),
             "category_level4_name": StringType(), "category_level4_code": StringType()}

    COUNTRY_NAMES = {
        "PL": "Polska", "LT": "Litwa", "HR": "Chorwacja", "CZ": "Czechy", "SK": "Slowacja",
        "DE": "Niemcy", "AT": "Austria", "HU": "Wegry", "RO": "Rumunia", "UA": "Ukraina",
        "BG": "Bulgaria", "SI": "Slowenia", "EE": "Estonia", "LV": "Lotwa", "RS": "Serbia",
        "Eshop": "Eshop"
    }

    REQUIRED_COLUMNS = ["store_code", "store_description"]

    def __init__(self, client_data_access_layer: ClientDataAccessLayer, azure_helper: AzureHelper,
                 script_cfg: Configuration, logger: Logger,
                 client_azure_helper: AzureHelper = None, client_spark_helper: SparkHelper = None):
        self.__client_data_access_layer = client_data_access_layer
        self.__azure_helper = azure_helper
        self.__script_cfg = script_cfg
        self.__logger = logger
        self.__client_azure_helper = client_azure_helper
        self.__client_spark_helper = client_spark_helper

    # ==========================================================================
    # WALIDACJA
    # ==========================================================================
    def validate_input(self, df: DataFrame) -> DataFrame:
        """
        Walidacja danych wejsciowych z pliku store.csv.

        V1: Pusty plik -> CRITICAL ERROR (sys.exit)
        V2: Brak wymaganych kolumn -> CRITICAL ERROR (sys.exit)
        V3: NULL w store_code -> WARN + drop
        V4: Brak country -> WARN (info o mapowaniu na Eshop)

        Returns:
            DataFrame po usunieciu wierszy z NULL store_code.
        """
        # V1: Pusty plik
        total_count = df.count()
        if total_count == 0:
            self.__logger.log("CRITICAL: Plik store.csv jest pusty! Przerywam import.")
            sys.exit(1)
        self.__logger.log(f"Wczytano {total_count:,} wierszy z store.csv")

        # V2: Brak wymaganych kolumn
        missing_cols = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        if missing_cols:
            self.__logger.log(f"CRITICAL: Brak wymaganych kolumn: {missing_cols}. Przerywam import.")
            sys.exit(1)
        self.__logger.log(f"Wymagane kolumny obecne: {self.REQUIRED_COLUMNS}")

        # V3: NULL w store_code
        null_store_code_count = df.filter(F.col("store_code").isNull() | (F.trim(F.col("store_code")) == "")).count()
        if null_store_code_count > 0:
            self.__logger.log(f"WARN: {null_store_code_count} wierszy z pustym store_code — usuwam je.")
            df = df.filter(F.col("store_code").isNotNull() & (F.trim(F.col("store_code")) != ""))

        # V4: Brak country
        if "country" in df.columns:
            no_country_count = df.filter(
                F.col("country").isNull() | (F.trim(F.col("country")) == "")
            ).count()
            if no_country_count > 0:
                self.__logger.log(
                    f"WARN: {no_country_count} sklepow bez kraju — zostana zmapowane na 'Eshop' i odfiltrowane."
                )
        else:
            self.__logger.log("WARN: Kolumna 'country' nie istnieje — wszystkie sklepy beda traktowane jako Eshop.")

        return df

    # ==========================================================================
    # OBSLUGA COMMUNITY_ID
    # ==========================================================================
    def handle_community_id(self, df: DataFrame) -> DataFrame:
        """
        Obsluga kolumny community_id:
        1. Jezeli community_id juz istnieje w store.csv — zachowywana bez zmian.
        2. Jezeli nie istnieje — laduje mapowanie store_code->community_id
           z pliku store_with_community_id.csv (cfg: input.community_mapping)
           i dolacza LEFT JOIN po store_code.
        3. Jezeli plik mapowania nie istnieje — ustawia NULL z logiem WARN.
        """
        if "community_id" in df.columns:
            non_null = df.filter(F.col("community_id").isNotNull()).count()
            self.__logger.log(f"INFO: Kolumna 'community_id' obecna w store.csv ({non_null} niepustych).")
            return df

        # Probuj zaladowac mapowanie z pliku zewnetrznego
        self.__logger.log("INFO: Kolumna 'community_id' nie istnieje w store.csv — laduje z pliku mapowania...")
        try:
            mapping_path = self.__client_azure_helper.create_azure_path(
                folder_path=self.__script_cfg['input']['community_mapping']
            )
            df_mapping = self.__client_spark_helper.spark.read \
                .option("header", "true").option("sep", ";") \
                .option("encoding", "UTF-8").csv(mapping_path)
            df_mapping = df_mapping.toDF(*[c.strip().lower().replace('\ufeff', '') for c in df_mapping.columns])

            df_mapping = df_mapping.select(
                F.trim(F.col("store_code")).alias("_map_store_code"),
                F.col("community_id").cast(IntegerType()).alias("community_id")
            ).filter(F.col("community_id").isNotNull())

            matched = df_mapping.count()
            self.__logger.log(f"INFO: Zaladowano mapowanie community_id — {matched} sklepow z community_id.")

            store_code_col = "store_code" if "store_code" in df.columns else "code"
            df = df.join(
                F.broadcast(df_mapping),
                F.trim(F.col(store_code_col)) == F.col("_map_store_code"),
                "left"
            ).drop("_map_store_code")

            filled = df.filter(F.col("community_id").isNotNull()).count()
            self.__logger.log(f"INFO: Dopasowano community_id dla {filled}/{df.count()} sklepow.")

        except Exception as e:
            self.__logger.log(f"WARN: Nie udalo sie zaladowac mapowania community_id: {e}")
            self.__logger.log("WARN: Ustawiam community_id na NULL.")
            df = df.withColumn("community_id", F.lit(None).cast(IntegerType()))

        return df

    # ==========================================================================
    # OBSLUGA KOLUMN DEPARTAMENTOW (mos_attribute_1..5)
    # ==========================================================================
    def handle_mos_attributes(self, df: DataFrame) -> DataFrame:
        """
        Sprawdza czy kolumny mos_attribute_1..5 istnieja w store.csv.
        Jezeli istnieja — sa przekazywane do outputu.
        Jezeli nie — logowany jest komunikat i skrypt kontynuuje.

        Mapowanie nazw:
          mos_attribute_1 = Meski, mos_attribute_2 = Damski,
          mos_attribute_3 = Junior, mos_attribute_4 = Akcesoria,
          mos_attribute_5 = Obuwie
        Wartosci w kolumnach: A/B/C/D (kody kategorii departamentow).
        """
        present = [c for c in self.MOS_ATTRIBUTES if c in df.columns]
        missing = [c for c in self.MOS_ATTRIBUTES if c not in df.columns]

        if present:
            names_info = ", ".join(f"{c} ({self.MOS_ATTRIBUTE_NAMES[c]})" for c in present)
            self.__logger.log(f"INFO: Znaleziono kolumny departamentow: {names_info}")
        if missing:
            names_info = ", ".join(f"{c} ({self.MOS_ATTRIBUTE_NAMES[c]})" for c in missing)
            self.__logger.log(f"INFO: Brak kolumn departamentow (pomijam): {names_info}")

        return df

    # ==========================================================================
    # KALENDARZ AKTYWNOSCI SKLEPOW
    # ==========================================================================
    def enrich_with_calendar(self, df: DataFrame) -> DataFrame:
        """
        Wzbogaca dane sklepow o daty otwarcia/zamkniecia z pliku
        store_activity_calendar.csv (LEFT JOIN na store_code).

        Daty z kalendarza uzupelniaja TYLKO puste pola w store.csv:
          - opening_date: uzupelniany z kalendarza gdy NULL w store.csv
          - closing_date: uzupelniany z kalendarza gdy NULL w store.csv

        Jezeli plik kalendarza nie istnieje na Azure, logowany jest komunikat
        informacyjny i dane sa zwracane bez zmian.
        """
        if self.__client_azure_helper is None or self.__client_spark_helper is None:
            self.__logger.log("INFO: Brak client_azure_helper/client_spark_helper — pomijam kalendarz.")
            return df

        calendar_path_cfg = self.__script_cfg['input']['raw_store_calendar']
        calendar_azure_path = self.__client_azure_helper.create_azure_path(
            folder_path=calendar_path_cfg
        )

        # Sprawdzenie czy plik istnieje
        try:
            if not self.__client_azure_helper.exists(calendar_azure_path):
                self.__logger.log(
                    "INFO: Plik store_activity_calendar.csv nie istnieje na Azure — pomijam wzbogacanie dat."
                )
                return df
        except Exception as e:
            self.__logger.log(
                f"WARN: Nie udalo sie sprawdzic istnienia pliku kalendarza: {e} — pomijam."
            )
            return df

        self.__logger.log("Wczytywanie pliku store_activity_calendar.csv...")
        calendar_df = self.__client_spark_helper \
            .spark \
            .read \
            .option("header", "true") \
            .option("sep", ";") \
            .option("inferSchema", "true") \
            .csv(calendar_azure_path)

        cal_count = calendar_df.count()
        self.__logger.log(f"Wczytano {cal_count:,} wierszy z kalendarza aktywnosci.")

        if cal_count == 0:
            self.__logger.log("INFO: Plik kalendarza jest pusty — pomijam.")
            return df

        # Deduplikacja kalendarza: dla kazdego sklepu bierzemy najnowszy wpis
        # (sklepy sezonowe maja wiele wierszy — otwieranie/zamykanie co rok)
        from pyspark.sql.window import Window
        cal_window = Window.partitionBy("store_code").orderBy(F.col("opening_date").desc())
        calendar_df = calendar_df.withColumn("_cal_rn", F.row_number().over(cal_window)) \
            .filter(F.col("_cal_rn") == 1).drop("_cal_rn")

        # Rename kolumn kalendarza zeby uniknac kolizji
        calendar_df = calendar_df.select(
            F.col("store_code").alias("cal_store_code"),
            F.col("opening_date").alias("cal_opening_date"),
            F.col("closing_date").alias("cal_closing_date"),
        )

        # LEFT JOIN (1:1 po deduplikacji)
        df = df.join(F.broadcast(calendar_df), df["store_code"] == calendar_df["cal_store_code"], "left")

        # Uzupelnienie dat: dane z store.csv maja priorytet
        if "opening_date" in df.columns:
            df = df.withColumn("opening_date",
                               F.coalesce(F.col("opening_date"), F.col("cal_opening_date")))
        else:
            df = df.withColumn("opening_date", F.col("cal_opening_date"))

        if "closing_date" in df.columns:
            df = df.withColumn("closing_date",
                               F.coalesce(F.col("closing_date"), F.col("cal_closing_date")))
        else:
            df = df.withColumn("closing_date", F.col("cal_closing_date"))

        # Usuniecie kolumn tymczasowych
        df = df.drop("cal_store_code", "cal_opening_date", "cal_closing_date")

        filled_opening = df.filter(F.col("opening_date").isNotNull()).count()
        filled_closing = df.filter(F.col("closing_date").isNotNull()).count()
        self.__logger.log(
            f"Po wzbogaceniu z kalendarza: {filled_opening} sklepow z opening_date, "
            f"{filled_closing} sklepow z closing_date."
        )

        return df

    # ==========================================================================
    # TRANSFORMACJA DANYCH SKLEPOW
    # ==========================================================================
    def transform_store_raw_data(self) -> DataFrame:
        """
        Glowna transformacja danych sklepow:
        1. Normalizacja kodow krajow (PO->PL, SV->SK, puste->Eshop)
        2. Filtrowanie sklepow Eshop
        3. Ustawienie flag is_active / is_closed na podstawie kolumny 'class'
        """
        raw_country = F.substring(F.upper(F.trim(F.col("country"))), 1, 2)

        df = self.__client_data_access_layer.raw_store_data \
            .withColumn("country",
                F.when((F.col("country").isNull()) | (F.trim(F.col("country")) == ""), F.lit("Eshop"))
                .when(raw_country == "PO", F.lit("PL"))
                .when(raw_country == "SV", F.lit("SK"))
                .otherwise(raw_country))

        # Statystyki przed filtrowaniem Eshop
        total_before_filter = df.count()
        eshop_count = df.filter(F.col("country") == "Eshop").count()

        # Filtrowanie sklepow online (Eshop) — zarzadzane recznie w Occubee
        df = df.filter(F.col("country") != "Eshop")

        # market_zone = prefiks strefy cenowej (first_price bez ostatniego znaku F/A)
        # np. PLREGF -> PLREG, CZREGF -> CZREG
        if "first_price" in df.columns:
            df = df.withColumn("market_zone",
                F.expr("substring(trim(first_price), 1, length(trim(first_price)) - 1)"))
            self.__logger.log("INFO: market_zone obliczone z first_price (strip F/A).")
        else:
            df = df.withColumn("market_zone", F.col("country"))
            self.__logger.log("WARN: Brak kolumny first_price — market_zone = country (fallback).")

        if "class" in df.columns:
            df = df \
                .withColumn("is_active", F.when(F.col("class") != "ZAMKNIETE", 1).otherwise(0)) \
                .withColumn("is_closed", F.when(F.col("class") == "ZAMKNIETE", 1).otherwise(0))

        # Override: sklepy z agregatu musza byc aktywne (kalendarz wymaga is_active=1)
        # Lista 192 sklepow zamknietych/sezonowych obecnych w agregacie forecasting
        FORCE_ACTIVE_CODES = {
            "PL17004", "PL15047", "CZ11002", "PL21011", "RO11004", "PL11108",
            "PL17001", "PL21006", "PL13025", "PL54068", "PL12015", "PL11091",
            "PL54232", "PL55016", "PL54133", "PL55024", "PL11092", "PL21019",
            "PL11126", "PL15029", "SK14020", "PL13089", "PL54149", "PL13096",
            "PL13079", "PL54080", "PL13053", "PL54002", "SK14015", "PL54048",
            "PL54007", "PL79006", "PL11102", "PL21009", "PL54026", "PL54074",
            "PL13090", "PL15060", "PL15120", "PL17002", "PL54228", "PL54244",
            "PL67001", "PL13017", "PL15127", "PL54113", "PL54150", "PL13035",
            "PL54082", "PL15058", "SK14003", "PL11122", "SK14001", "PL21007",
            "PL11098", "PL13117", "PL13073", "PL54138", "PL54227", "PL13098",
            "PL54014", "PL54040", "PL21016", "PL54234", "PL54027", "PL54245",
            "PL54209", "PL54103", "PL11173", "PL13108", "PL54104", "PL54220",
            "PL11151", "PL13085", "PL54021", "PL54097", "PL54259", "PL54136",
            "PL54110", "PL54237", "PL55022", "CZ14006", "PL54262", "PL11148",
            "PL13027", "PL54131", "PL54089", "CZ14002", "PL13011", "PL54024",
            "PL15101", "RO11003", "PL11138", "PL13056", "PL15136", "PL54246",
            "PL54098", "PL54053", "PL13083", "PL13021", "PL54248", "PL55004",
            "PL25006", "PL55020", "PL54005", "PL54240", "PL54035", "PL21012",
            "PL13047", "PL54109", "PL11023", "PL54004", "PL54144", "PL15130",
            "PL21021", "PL12023", "SK11002", "PL54043", "PL54015", "PL15005",
            "PL54132", "PL13022", "PL54258", "PL79008", "PL11052", "PL54127",
            "PL12021", "PL13034", "PL21010", "PL54070", "PL54011", "PL13006",
            "PL55005", "PL54081", "PL54085", "PL54135", "PL55025", "PL17003",
            "PL54202", "PL54032", "PL54076", "PL11018", "PL67002", "PL12019",
            "PL55023", "PL54130", "PL15097", "PL54137", "PL11050", "PL21014",
            "PL13052", "PL54019", "PL54139", "PL55014", "PL54210", "PL13026",
            "SK14019", "PL13030", "PL54229", "PL55013", "PL13036", "PL55006",
            "CZ14001", "PL12016", "SK11006", "PL54270", "PL54102", "PL55019",
            "PL54077", "PL54016", "PL21020", "SK14017", "PL54141", "PL54116",
            "SK14018", "PL13092", "PL55021", "PL54205", "PL15078", "PL13050",
            "RO17001", "PL54230", "PL13042", "PL54054", "PL13091", "PL12020",
            "PL54231", "PL13049", "PL21017", "PL13111", "CZ11005", "PL13094",
        }

        if "is_active" in df.columns:
            force_active_count = df.filter(
                (F.col("store_code").isin(FORCE_ACTIVE_CODES)) & (F.col("is_active") == 0)
            ).count()
            df = df \
                .withColumn("is_active",
                            F.when(F.col("store_code").isin(FORCE_ACTIVE_CODES), F.lit(1))
                            .otherwise(F.col("is_active"))) \
                .withColumn("is_closed",
                            F.when(F.col("store_code").isin(FORCE_ACTIVE_CODES), F.lit(0))
                            .otherwise(F.col("is_closed")))
            self.__logger.log(f"Override: {force_active_count} sklepow z agregatu wymuszono is_active=1")

        # Statystyki aktywnosci
        active_count = df.filter(F.col("is_active") == 1).count() if "is_active" in df.columns else "N/A"
        closed_count = df.filter(F.col("is_closed") == 1).count() if "is_closed" in df.columns else "N/A"
        self.__logger.log(
            f"Sklepy: total {total_before_filter}, aktywne: {active_count}, "
            f"zamkniete: {closed_count}, odfiltrowane Eshop: {eshop_count}"
        )

        return df

    # ==========================================================================
    # DETERMINISTYCZNY KOD SHA-256
    # ==========================================================================
    @staticmethod
    def _det_code(col_expr) -> "Column":
        """Deterministyczny 10-cyfrowy kod z SHA256 wartosci kolumny."""
        return F.lpad(
            (F.conv(F.substring(F.sha2(col_expr.cast(StringType()), 256), 1, 15), 16, 10)
             .cast(LongType()) % F.lit(10000000000)).cast(StringType()),
            10, "0"
        )

    # ==========================================================================
    # HIERARCHIA KATEGORII SKLEPOW
    # ==========================================================================
    def add_category_columns(self, df: DataFrame) -> DataFrame:
        """Dodaje 8 kolumn hierarchii kategorii sklepow (4 levele x name + code)."""
        # Level 1: Kraj (pelna nazwa)
        country_map = F.create_map(*[x for pair in
            [(F.lit(k), F.lit(v)) for k, v in self.COUNTRY_NAMES.items()]
            for x in pair])
        df = df.withColumn("category_level1_name",
                           F.coalesce(country_map[F.col("country")], F.col("country")))

        # Level 2: Segment
        df = df.withColumn("category_level2_name",
                           F.when(F.col("segment").isNotNull() & (F.trim(F.col("segment")) != ""),
                                  F.trim(F.col("segment"))).otherwise(F.lit(None)))

        # Level 3: Kanal (building_type)
        df = df.withColumn("category_level3_name",
                           F.when(F.col("building_type").isNotNull() & (F.trim(F.col("building_type")) != ""),
                                  F.trim(F.col("building_type"))).otherwise(F.lit(None)))

        # Level 4: Kod sklepu
        df = df.withColumn("category_level4_name", F.trim(F.col("store_code")))

        # Kody deterministyczne (SHA256-based) — hash pelnej sciezki,
        # aby ten sam "Regular" pod roznymi segmentami mial rozny kod
        l1 = F.col("category_level1_name")
        l2 = F.col("category_level2_name")
        l3 = F.col("category_level3_name")
        l4 = F.col("category_level4_name")

        df = df \
            .withColumn("category_level1_code", self._det_code(l1)) \
            .withColumn("category_level2_code", self._det_code(F.concat_ws("|", l1, l2))) \
            .withColumn("category_level3_code", self._det_code(F.concat_ws("|", l1, l2, l3))) \
            .withColumn("category_level4_code", self._det_code(F.concat_ws("|", l1, l2, l3, l4)))

        return df

    # ==========================================================================
    # MAPOWANIE KOLUMN
    # ==========================================================================
    def map_columns(self, df: DataFrame) -> list:
        """
        Mapuje kolumny zrodlowe na kolumny docelowe Occubee.
        Obsluguje kolumny ORDER + EXTRA + opcjonalne mos_attribute_1..5.
        """
        # Kolumny mos_attribute obecne w danych
        mos_present = [c for c in self.MOS_ATTRIBUTES if c in df.columns]

        final_cols = []
        for c in self.ORDER + self.EXTRA + mos_present:
            src = self.MAP.get(c, c)
            col_expr = F.col(src) if src in df.columns else F.lit(None)
            if c in self.CASTS:
                col_expr = col_expr.cast(self.CASTS[c])
            final_cols.append(col_expr.alias(c))
        return final_cols

    # ==========================================================================
    # ZAPIS PLIKU WYNIKOWEGO
    # ==========================================================================
    def _write_single_file(self, df: DataFrame, base_path: str, file_name: str, write_opts: dict):
        """Zapisuje DF jako pojedynczy plik CSV o czystej nazwie (Hadoop rename)."""
        spark = self.__client_spark_helper.spark
        from py4j.java_gateway import java_import
        java_import(spark._jvm, "org.apache.hadoop.fs.Path")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        FileSystem = spark._jvm.org.apache.hadoop.fs.FileSystem

        temp_dir = self.__client_azure_helper.create_azure_path(folder_path=f"{base_path}/_tmp_store")
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

    def upload_store_file(self, df: DataFrame, cols: list) -> str:
        """Zapisuje plik wynikowy do Azure. Obsluguje tryby: FULL, TEST, BOTH."""
        write_opts = {
            "header": "true", "sep": ";", "encoding": "UTF-8",
            "quoteAll": "false", "emptyValue": ""
        }
        df_prepared = df.select(*cols)

        if self.OUTPUT_MODE in ("FULL", "BOTH"):
            base_path = self.__script_cfg['output']['result_full_path']
            file_name = self.__script_cfg['output']['result_full_filename']
            self.__logger.log(f"Zapisywanie FULL: {base_path}/{file_name}")
            self._write_single_file(df_prepared, base_path, file_name, write_opts)
            self.__logger.log("Sukces! Zapisano FULL.")

        if self.OUTPUT_MODE in ("TEST", "BOTH"):
            base_path = self.__script_cfg['output']['result_sample_path']
            file_name = self.__script_cfg['output']['result_sample_filename']
            self.__logger.log(f"Zapisywanie SAMPLE: {base_path}/{file_name}")
            self._write_single_file(df_prepared.limit(1000), base_path, file_name, write_opts)
            self.__logger.log("Sukces! Zapisano SAMPLE.")

        return f"{base_path}/{file_name}"


if __name__ == "__main__":
    script_cfg = Configuration()
    cmd_args = GatherArguments(script_cfg["std_cmd_args"]["std_cmd_args"])

    apply_profile(script_cfg, "light")
    lib_helpers_factory = LibHelpersFactory(config=script_cfg,
                                            cmd_args=cmd_args)
    app_name = "Import Sklepow 4F"
    spark_wrapper = lib_helpers_factory.make_spark_wrapper(
        spark_application_name=app_name)

    with spark_wrapper as sc:
        spark_wrapper.spark.sparkContext.setLogLevel("ERROR")
        azure_helper = lib_helpers_factory.make_azure_helper()
        dictionary_helper = lib_helpers_factory.make_dictionary_helper()
        spark_helper = lib_helpers_factory.make_spark_helper()
        logger = Logger(script_cfg['logging']['log_on_the_screen'], app_name)

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
        client_spark_helper = SparkHelper(spark=spark_wrapper.spark)
        client_data_access_layer = ClientDataAccessLayer(client_spark_helper, client_azure_helper, script_cfg, logger)

        store_import = StoreImport(
            client_data_access_layer=client_data_access_layer,
            azure_helper=azure_helper,
            script_cfg=script_cfg,
            logger=logger,
            client_azure_helper=client_azure_helper,
            client_spark_helper=client_spark_helper,
        )

        # --- WALIDACJA ---
        raw_df = client_data_access_layer.raw_store_data
        validated_df = store_import.validate_input(raw_df)

        # --- OBSLUGA OPCJONALNYCH KOLUMN ---
        validated_df = store_import.handle_community_id(validated_df)
        validated_df = store_import.handle_mos_attributes(validated_df)

        # --- WZBOGACENIE Z KALENDARZA ---
        validated_df = store_import.enrich_with_calendar(validated_df)

        # Podmiana danych w DAL (aby transform_store_raw_data pracowal na zwalidowanych danych)
        client_data_access_layer._ClientDataAccessLayer__raw_store_data = validated_df

        # --- TRANSFORMACJA ---
        transformed_store_data = store_import.transform_store_raw_data()
        transformed_store_data = store_import.add_category_columns(transformed_store_data)
        mapped_columns = store_import.map_columns(transformed_store_data)

        # --- ZAPIS ---
        output_path = store_import.upload_store_file(transformed_store_data, mapped_columns)
        logger.log(f"Zakonczono import sklepow. Plik wynikowy: {output_path}")
