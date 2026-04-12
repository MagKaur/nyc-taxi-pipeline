# NYC Taxi ETL Pipeline (Spark + Dagster)

## Opis projektu

Projekt przedstawia kompletny pipeline ETL dla danych NYC Taxi, zbudowany w architekturze Medallion (Bronze → Silver → Gold) z wykorzystaniem:

* PySpark – przetwarzanie danych
* Dagster – orkiestracja pipeline’u
* Parquet – format danych
* Lokalny filesystem jako storage

Pipeline jest idempotentny i może być uruchamiany wielokrotnie bez duplikacji danych.

---

## Architektura

Pipeline składa się z 3 warstw:

### Bronze

* Surowe dane (raw ingestion)
* Minimalne transformacje
* Dodanie `raw_trip_id`

Input:

```
data/raw/
```

Output:

```
data/warehouse/bronze/
```

---

### Silver

* Czyszczenie danych
* Walidacja rekordów
* Deduplikacja (hash-based)
* Standaryzacja kolumn

Output:

```
data/warehouse/silver/
```

---

### Gold

* Agregacje biznesowe
* Joiny z lookupami
* Metryki (revenue, trip_count, avg_distance)

Output:

```
data/warehouse/gold/
```

---

## Technologie

* Python 3.12
* PySpark
* Dagster
* Java 17
* Hadoop (winutils – Windows fix)

---

## Jak uruchomić projekt

### 1. Aktywacja środowiska

```
python -m venv .venv
.venv\Scripts\activate
```

### 2. Instalacja zależności

```
pip install -r requirements.txt
+
Java Configuration: (Terminal)
$env:JAVA_HOME="C:\Program Files\Java\jdk-17"
$env:Path="$env:JAVA_HOME\bin;$env:Path"
+
Hadoop Configuration (only on Windows):
Utwórz folder C:\hadoop\bin
Pobierz plik winutils.exe i umieść zawartość jego bin w C:\hadoop\bin
Ustaw zmienne: (Terminal)
$env:HADOOP_HOME="C:\hadoop"
$env:Path="$env:HADOOP_HOME\bin;$env:Path"
```

### 3. Uruchomienie pipeline (CLI) -> Rekomendowany step 4

```
python .\spark_jobs\main.py --stage bronze
python .\spark_jobs\main.py --stage silver
python .\spark_jobs\main.py --stage gold
```

### 4. Uruchomienie Dagstera (Rekomendowane)

```
dagster dev -f .\dagster_project\definitions.py
```

Następnie otwórz:

```
http://127.0.0.1:3000
```

---

## Użycie Dagstera

1. Przejdź do zakładki "Assets"
2. Zaznacz:

   * bronze_load
   * silver_transform
   * gold_metrics
   * quality_checks
3. Kliknij "Materialize selected"

---

## Lineage pipeline

```
bronze_load
     ↓
silver_transform
     ↓
gold_metrics
     ↓
quality_checks
```

---

## Idempotencja

Pipeline jest idempotentny dzięki:

* zapisowi `mode="overwrite"`
* deduplikacji (`dropDuplicates`)
* wykorzystaniu identyfikatorów hash (`trip_id`, `gold_record_id`)

Możliwe jest wielokrotne uruchamianie bez duplikacji danych.

---

## Quality checks

W projekcie zaimplementowano:

* wykrywanie błędnych dat (dropoff < pickup)
* wartości ujemne
* brakujące lookupy
* liczbę niepoprawnych rekordów

---

## Struktura projektu

```
.
├── data/
│   ├── raw/
│   └── warehouse/
│       ├── bronze/
│       ├── silver/
│       └── gold/
│
├── spark_jobs/
│   ├── main.py
│   ├── bronze.py
│   ├── silver.py
│   ├── gold.py
│
├── dagster_project/
│   ├── definitions.py
│   ├── assets.py
│
├── requirements.txt
├── README.md
```

---

## Możliwe rozszerzenia

* przetwarzanie strumieniowe
* system kolejkowania (np. Kafka)
* incremental loading
* automatyczne wyzwalanie pipeline’u (Airflow)

---

## Podsumowanie

Projekt implementuje:

* pełny pipeline ETL
* architekturę data lake (Medallion)
* przetwarzanie w Spark
* orkiestrację w Dagster
* idempotentne przetwarzanie danych
