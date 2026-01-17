db.imgw_monthly1.aggregate([
  {
    // ETAP 1: Konwersja daty wejściowej na obiekt systemowy
    // (Python dostarczył już dane miesięczne, więc nie grupujemy ich ponownie)
    $addFields: {
      convertedDate: { $toDate: "$timestamp" }
    }
  },
  {
    // ETAP 2: Unifikacja schematu (Schema Alignment)
    // Dopasowanie nazw pól i formatów do standardu kolekcji Open-Meteo
    $project: {
      _id: 0,
      
      // Standaryzacja znacznika czasu (np. "2024-03-01T00:00:00Z")
      timestamp: {
        $dateToString: {
          format: "%Y-%m-01T00:00:00Z",
          date: "$convertedDate"
        }
      },
      
      location: "$location",
      source: "$source",
      
      // Ekstrakcja roku i miesiąca dla celów indeksowania/filtrowania
      year: { $year: "$convertedDate" },
      month: { $month: "$convertedDate" },
      
      // Przepisanie wartości (są już policzone w Pythonie)
      // Używamy $round dla pewności, że typ danych to Double
      temperature_2m: { $round: ["$temperature_2m", 2] },
      precipitation: { $round: ["$precipitation", 2] },
      wind_speed_10m: { $round: ["$wind_speed_10m", 2] },
      
      // Zachowanie danych przestrzennych
      geometry: 1,
      lat: 1,
      lon: 1
    }
  },
  {
    // ETAP 3: Zapis do docelowej kolekcji analitycznej
    $out: "imgw_monthly"
  }
]);
