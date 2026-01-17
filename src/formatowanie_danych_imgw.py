import pandas as pd
import json
import glob
import os

INPUT_FOLDER = 'imgw_dane'
OUTPUT_FILE = 'pogoda_wroclaw_2015_2025_FINAL.json'
STATION_CODE = 351160424  # Kod stacji Wrocław_Strachowice
# --------------------

print(f"--- PRZETWARZANIE DANYCH DLA STACJI {STATION_CODE} ---")

files = glob.glob(os.path.join(INPUT_FOLDER, 's_d_*.csv'))
print(f"Znaleziono plików CSV: {len(files)}")

all_data_frames = []

for file in files:
    try:
        try:
            df = pd.read_csv(file, header=None, encoding='iso-8859-2')
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file, header=None, encoding='cp1250')
            except UnicodeDecodeError:
                df = pd.read_csv(file, header=None, encoding='latin1')

        df_wroclaw = df[df[0] == STATION_CODE].copy()

        if df_wroclaw.empty:
            print(f"[SKIP] Plik {os.path.basename(file)} nie zawiera danych dla tej stacji.")
            continue

        # Wybór kolumn (wg standardu s_d):
        # 2:Rok, 3:Miesiąc, 9:Temp Średnia, 13:Opad Suma, 20:Wiatr Średni
        df_wroclaw = df_wroclaw.rename(columns={
            2: 'Year',
            3: 'Month',
            9: 'Temp',
            13: 'Precip',
            20: 'Wind'
        })

        df_subset = df_wroclaw[['Year', 'Month', 'Temp', 'Precip', 'Wind']]
        all_data_frames.append(df_subset)

        print(f"[OK] {os.path.basename(file)} -> znaleziono dni: {len(df_subset)}")

    except Exception as e:
        print(f"[ERROR] Problem z plikiem {file}: {e}")

if all_data_frames:
    print("\nŁączenie danych...")
    full_df = pd.concat(all_data_frames)

    full_df['Temp'] = pd.to_numeric(full_df['Temp'], errors='coerce')
    full_df['Precip'] = pd.to_numeric(full_df['Precip'], errors='coerce')
    full_df['Wind'] = pd.to_numeric(full_df['Wind'], errors='coerce')

    print("Liczenie średnich miesięcznych...")
    monthly_stats = full_df.groupby(['Year', 'Month']).agg({
        'Temp': 'mean',  # Temperatura: średnia
        'Precip': 'sum',  # Opad: suma
        'Wind': 'mean'  # Wiatr: średnia
    }).reset_index()

    monthly_stats = monthly_stats.round(2)

    documents = []
    for _, row in monthly_stats.iterrows():
        ts = f"{int(row['Year'])}-{int(row['Month']):02d}-01T00:00:00Z"

        doc = {
            "timestamp": ts,
            "location": "Wroclaw_Strachowice",
            "source": "imgw-pib",
            "geometry": {
                "type": "Point",
                "coordinates": [16.8858, 51.1025]  # Lon, Lat
            },
            "lat": 51.1025,
            "lon": 16.8858,

            "temperature_2m": row['Temp'],
            "precipitation": row['Precip'],
            "wind_speed_10m": row['Wind'] if pd.notnull(row['Wind']) else None
        }
        documents.append(doc)

    documents.sort(key=lambda x: x['timestamp'])

    # Zapis
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(documents, f, indent=4)

    print(f"Wygenerowano plik: {OUTPUT_FILE}")
    print(f"Liczba rekordów (miesięcy): {len(documents)}")
    print(f"Zakres dat: od {documents[0]['timestamp']} do {documents[-1]['timestamp']}")

else:
    print("\nNie udało się przetworzyć żadnych danych.")
