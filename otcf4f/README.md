# otcf4f — ETL Pipeline dla klienta 4F (OTCF S.A.)

Repozytorium zawiera zestaw skryptów PySpark realizujących pipeline ETL pomiędzy surowymi danymi dostarczanymi przez klienta 4F (OTCF S.A.) a strukturami danych platformy analitycznej Occubee. Skrypty odpowiadają za pobieranie, transformację i dostarczanie plików CSV gotowych do importu przez silnik Occubee.

---

## Architektura systemu

Pipeline składa się z dwóch magazynów danych w chmurze Azure (Azure Data Lake Storage Gen2) oraz warstwy przetwarzania opartej na Apache Spark:

```
┌─────────────────────────────────────────────────────────────────────┐
│                        AZURE DATA LAKE GEN2                         │
│                                                                     │
│  ┌──────────────────────────┐    ┌──────────────────────────────┐   │
│  │  Storage: 4fprodmainsa   │    │  Storage: otcf4fprodmainsa   │   │
│  │  Container: "data"       │    │  Container: "bigdata"        │   │
│  │                          │    │                              │   │
│  │  Surowe dane od klienta: │    │  Dane przetworzone Occubee:  │   │
│  │  - DATA_IN/PRODUKTY/     │    │  - dictionaries/product      │   │
│  │  - DATA_IN/SKLEPY/       │    │  - dictionaries/store        │   │
│  │  - DATA_IN/CENNIKI/      │    │  - sales/sales_base          │   │
│  │  - DATA_IN/MAGAZYN/      │    │  - TRANSFORMED_DATA/         │   │
│  └────────────┬─────────────┘    └──────────────┬───────────────┘   │
│               │  INPUT                          │  OUTPUT           │
└───────────────┼─────────────────────────────────┼───────────────────┘
                │                                 │
                ▼                                 │
┌─────────────────────────────────────────────────┘
│              SPARK CLUSTER (on-premises)
│              /opt/spark3/bin/spark-submit
│
│   custom_scripts/
│   ├── 4f_store_import.py            → import sklepów
│   ├── 4f_sales_import.py            → import sprzedaży
│   ├── 4f_product_import.py          → import produktów
│   ├── 4f_price_import.py            → import cenników
│   ├── 4f_goods_in_transit_import.py → import towaru w drodze
│   ├── 4f_store_stock_import.py      → import stanów sklepowych
│   └── 4f_store_warehouse_import.py  → import stanów magazynowych
└─────────────────────────────────────────────────────────────────────
```

### Przepływ danych

1. **4F (klient)** dostarcza surowe pliki CSV do kontenera `data` na swoim koncie Azure Storage (`4fprodmainsa`).
2. **Skrypty ETL** (ten projekt) pobierają dane z kontenera `data`, wykonują transformacje i zapisują wynikowe pliki CSV do kontenera `bigdata` na koncie Occubee (`otcf4fprodmainsa`), w folderze `TRANSFORMED_DATA/`.
3. **Platforma Occubee** wczytuje przetworzone pliki CSV z `bigdata/TRANSFORMED_DATA/` i importuje je do swojej bazy danych przez silnik walidacyjny.

---

## Stos technologiczny

| Komponent | Szczegóły |
|---|---|
| Apache Spark | 3.5.1 — klaster on-premises, `/opt/spark3` |
| Python | 3.10 — conda env `da_env` |
| Azure Storage SDK | `hadoop-azure` 3.2.1, `azure-storage` 8.6.6 |
| Framework ETL | `occubee.common` — biblioteka platformy Occubee |
| Serwer produkcyjny | `ocb-da-onprem-01`, user `pbubula` |

---

## Struktura repozytorium

```
otcf4f/
├── custom_scripts/
│   │
│   ├── lib/
│   │   ├── __init__.py                    eksporty modułu
│   │   ├── data_access_layer.py           dostęp do słowników Occubee (bigdata)
│   │   ├── client_data_access_layer.py    dostęp do danych surowych klienta (data)
│   │   └── parameters.py                  dane autoryzacyjne konta klienta 4F
│   │
│   ├── 4f_store_import.py / .cfg          transformacja i import sklepów
│   ├── 4f_sales_import.py / .cfg          transformacja i import sprzedaży
│   ├── 4f_product_import.py / .cfg        transformacja i import produktów
│   ├── 4f_price_import.py / .cfg          transformacja i import cenników
│   ├── 4f_goods_in_transit_import.py / .cfg    import towaru w drodze
│   ├── 4f_store_stock_import.py / .cfg    import stanów sklepowych
│   ├── 4f_store_warehouse_import.py / .cfg     import stanów magazynowych
│   │
│   └── data/                              lokalne pliki pomocnicze i próbki
│
├── spark_script_configs/                  konfiguracje słowników generowane przez Occubee
└── README.md
```

---

## Konfiguracja autoryzacji — dwa konta Azure

Każdy skrypt korzysta z **dwóch oddzielnych kont Azure Storage**:

### Konto Occubee (`bigdata`) — przekazywane przez Occubee w argumentach wywołania

```
-s otcf4fprodmainsa.dfs.core.windows.net   storage account
-k <OCCUBEE_STORAGE_KEY>                    klucz dostępu
-c bigdata                                  kontener
```

Obsługiwane przez `azure_helper` (framework Occubee). Używane do:
- zapisu plików wynikowych (`TRANSFORMED_DATA/`)
- odczytu słowników i danych historycznych (`dictionaries/`, `sales/`)

### Konto klienta 4F (`data`) — konfiguracja w `lib/parameters.py`

```
storage_account: 4fprodmainsa.dfs.core.windows.net
container:       data
key:             <CLIENT_STORAGE_KEY>
```

Obsługiwane przez `client_azure_helper` zbudowany w bloku `__main__` każdego skryptu. Używane wyłącznie do odczytu surowych danych wejściowych.

> **Uwaga:** Klucz konta klienta jest obecnie przechowywany w `lib/parameters.py`.
> Docelowo powinien zostać przeniesiony do Azure Key Vault lub zewnętrznego pliku konfiguracyjnego (TODO w kodzie).

---

## Pliki konfiguracyjne (.cfg)

Każdy skrypt posiada powiązany plik `.cfg` w formacie INI wczytywany automatycznie przez framework Occubee (na podstawie nazwy skryptu). Przykładowa struktura:

```ini
[std_cmd_args]
std_cmd_args = container_name, storage_account_key, storage_account_name, dict_version_file, process_instance_id

[spark_config]
spark.default.parallelism = 72
spark.sql.shuffle.partitions = 72

[dictionaries]
# opcjonalne słowniki Occubee do wczytania przy starcie

[input]
# ścieżki do plików wejściowych (relatywne do root kontenera)

[output]
# ścieżki do plików wynikowych

[logging]
log_on_the_screen = true
```

---

## Uruchomienie skryptu

```bash
/opt/spark3/bin/spark-submit \
  /home/pbubula/projekty/otcf4f/custom_scripts/<nazwa_skryptu>.py \
  -s otcf4fprodmainsa.dfs.core.windows.net \
  -k <OCCUBEE_STORAGE_KEY> \
  -c bigdata \
  -p <process_instance_id> \
  -v spark_script_configs/<runscripts_config_file>.json
```

### Tryb zapisu (`OUTPUT_MODE`)

Każdy skrypt posiada zmienną klasową `OUTPUT_MODE` sterującą zakresem wyjścia:

| Wartość | Zachowanie |
|---|---|
| `"FULL"` | Zapis pełnego zbioru danych do `result_path` / `result_full_path` |
| `"TEST"` | Zapis próbki 1 000 wierszy do `result_sample_path` |
| `"BOTH"` | Zapis obu wersji jednocześnie (dane cachowane w RAM) |

---

## Biblioteka `lib/`

### `parameters.py`
Enkapsuluje dane dostępowe do konta Azure klienta 4F. Udostępnia trzy właściwości:
`client_storage_account_name`, `client_storage_account_key`, `client_container_name`.

### `client_data_access_layer.py`
Abstrakcja I/O nad danymi surowymi klienta. Wczytuje pliki CSV z kontenera `data`
przy użyciu dedykowanego `AzureHelper` skonfigurowanego na konto 4F. Udostępnia właściwości
dla każdego typu danych wejściowych (np. `raw_store_data`).

### `data_access_layer.py`
Abstrakcja nad słownikami platformy Occubee. Wczytuje i cachuje słowniki z kontenera `bigdata`
przez `DictionaryAzureHelper` na starcie skryptu.

---

## Opis skryptów

### `4f_store_import.py` — Import sklepów
- **Wejście:** `DATA_IN/SKLEPY/store_with_community_id.csv` (konto klienta `data`)
- **Wyjście:** `TRANSFORMED_DATA/store_import_occubee.csv` (konto Occubee `bigdata`)
- **Logika:** Standaryzacja kodów krajów (2-literowe ISO, obsługa wyjątków PO→PL, SV→SK), obliczanie flag aktywności z pola `class`, budowanie 4-poziomowej hierarchii kategorii sklepów (Kraj → Segment → Kanał → Kod sklepu) z deterministycznymi kodami SHA-256.

### `4f_sales_import.py` — Import sprzedaży
- **Wejście:** pliki `transactional_sales_*.csv`, `price.csv`, `retail_price.csv`, `store.csv` (konto klienta `data`)
- **Wyjście:** `TRANSFORMED_DATA/sales_import_*.csv` (konto Occubee `bigdata`)
- **Logika:** Obsługa zwrotów (odwrócenie znaków ilości i wartości), ASOF Join z cennikami (najnowsza obowiązująca cena na dzień transakcji), mechanizm fallback cenowy, typowanie `Decimal(13,2)`.

### `4f_product_import.py` — Import produktów
- **Wejście:** `product.csv`, `product_uda.csv`, `uda.csv` (konto klienta `data`); `sales/sales_base`, `dictionaries/store_stock_level` (konto Occubee `bigdata`)
- **Wyjście:** `TRANSFORMED_DATA/product_import_occubee.csv` (konto Occubee `bigdata`)
- **Logika:** Pivot atrybutów UDA, łączenie z historią aktywności z `bigdata`, budowanie 5-poziomowej hierarchii kategorii, deterministyczne kody CRC32/SHA-256.

### `4f_price_import.py` — Import cenników
- **Wejście:** `price.csv`, `retail_price.csv`, `store.csv` (konto klienta `data`); `dictionaries/product` (konto Occubee `bigdata`)
- **Wyjście:** `TRANSFORMED_DATA/price/` (konto Occubee `bigdata`)
- **Logika:** Grupowanie sklepów na strefy cenowe, deduplikacja dzienna (Lowest Price Wins), deterministyczne priorytety stref (sortowanie alfabetyczne).

### `4f_goods_in_transit_import.py` — Import towaru w drodze
- **Wejście:** `open_orders.csv`, `goods_in_transit.csv` (konto klienta `data`)
- **Wyjście:** `TRANSFORMED_DATA/goods_in_transit_*.csv` (konto Occubee `bigdata`)
- **Logika:** Integracja dwóch źródeł na wspólną strukturę `Expected Delivery`, odrzucanie rekordów bez daty dostawy z pełnym audytem wolumenowym.

### `4f_store_stock_import.py` — Import stanów sklepowych
- **Wejście:** `inventory*.csv` (konto klienta `data`)
- **Wyjście:** `TRANSFORMED_DATA/store_stock_*.csv` (konto Occubee `bigdata`)
- **Logika:** Czyszczenie notacji cudzysłowowej i naukowej liczb, formatowanie dat, dodanie stałej jednostki miary `"ST"`. W trybie TEST czytany jest mniejszy plik testowy.

### `4f_store_warehouse_import.py` — Import stanów magazynowych
- **Wejście:** `inventory_warehouse.csv` (konto klienta `data`)
- **Wyjście:** `TRANSFORMED_DATA/warehouse_stock_*.csv` (konto Occubee `bigdata`)
- **Logika:** Transformacja stanów magazynowych, audyt jakościowy (weryfikacja NULL w kolumnach wymaganych przed zapisem z ostrzeżeniami w logu).

---

## Uwagi operacyjne

- Skrypty uruchamiane są **wyłącznie przez `spark-submit`** na klastrze — nie lokalnie.
- Przed uruchomieniem produkcyjnym upewnij się, że `OUTPUT_MODE = "FULL"` lub `"BOTH"`.
- Pliki wynikowe trafiają do `bigdata/TRANSFORMED_DATA/` i są importowane przez Occubee.
- Ścieżka repozytorium na serwerze: `~/projekty/otcf4f/`
- Ścieżka skryptów produkcyjnych: `/home/spark3/Custom_Scripts/4f/`
