# Integracja Danych Klimatycznych Wrocław (2015-2025) - NoSQL & ETL

Repozytorium zawiera kod źródłowy oraz procedury bazodanowe opracowane w ramach pracy inżynierskiej:
> **"Wykorzystanie baz danych typu NoSQL do integracji otwartych danych klimatycznych z obszaru Wrocławia z okresu 2015-2025"**.

## O projekcie

Celem projektu było zaprojektowanie systemu Big Data integrującego heterogeniczne dane środowiskowe z dwóch niezależnych źródeł: pomiarów stacjonarnych (**IMGW**) oraz reanalizy numerycznej (**Open-Meteo API**). System weryfikuje elastyczność modelu dokumentowego **MongoDB** w procesie harmonizacji danych o różnej strukturze i rozdzielczości czasowej.

### Kluczowe funkcjonalności (Architektura Hybrydowa):
* **Proces ETL (Python):** Obsługa surowych plików CSV (brak nagłówków, zmienne kodowanie) oraz strumieni danych JSON.
* **Asymetryczna agregacja danych:**
    * *IMGW:* Pre-agregacja do średnich miesięcznych na poziomie Pythona (optymalizacja wydajności zapisu).
    * *Open-Meteo:* Import gęstych danych godzinowych i agregacja statystyczna wewnątrz silnika bazy danych (MongoDB Aggregation Framework).
* **Analiza przestrzenna:** Wykorzystanie standardu **GeoJSON** oraz indeksów przestrzennych `2dsphere` do integracji danych w układzie WGS84.

## Technologie

* **Język:** Python 3.10+
* **Biblioteki:** `pandas`, `openmeteo-requests`, `pymongo`, `matplotlib`, `seaborn`
* **Baza danych:** MongoDB (v6.0+)
* **Narzędzia:** QGIS (weryfikacja przestrzenna), MongoDB Compass

## Struktura repozytorium

```text
.
├── src/
│   ├── formatowanie_danych_imgw.py  # Przetwarzanie CSV z IMGW (czyszczenie + pre-agregacja miesięczna)
│   └── pobieranie_danych_api.py     # Pobieranie danych godzinowych z API (chunking + cache)
│
├── mongodb_pipelines/
│   ├── agregacje_api_open_meteo.js  # Agregacja danych godzinowych Open-Meteo -> miesięczne
│   └── agregacje_imgw.js       # Unifikacja schematu IMGW (Schema Alignment)
│
├── requirements.txt                 # Lista zależności (biblioteki Python)
└── README.md
