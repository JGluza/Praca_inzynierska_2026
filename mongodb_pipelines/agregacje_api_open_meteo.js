db.api_openmeteo_hourly.aggregate([
  {
    // ETAP 1: Konwersja daty ze stringa (ISO) na obiekt Date
    $addFields: {
      convertedDate: {
        $toDate: "$timestamp"
      }
    }
  },
  {
    // ETAP 2: Grupowanie po Lokalizacji, Roku i Miesiącu
    $group: {
      _id: {
        location: "$location",
        year: { $year: "$convertedDate" },
        month: { $month: "$convertedDate" }
      },
      // Obliczanie statystyk matematycznych
      avg_temp: { $avg: "$temperature_2m" },
      sum_precip: { $sum: "$precipitation" },
      avg_wind: { $avg: "$wind_speed_10m" },
      
      // Zachowanie metadanych (geometria jest stała dla lokalizacji)
      geometry: { $first: "$geometry" },
      lat: { $first: "$lat" },
      lon: { $first: "$lon" },
      source: { $first: "$source" }
    }
  },
  {
    // ETAP 3: Formatowanie wyniku (zaokrąglanie i rekonstrukcja daty)
    $project: {
      _id: 0, // Ukrycie technicznego ID grupy
      
      // Rekonstrukcja daty do formatu YYYY-MM-01T00:00:00Z (standard IMGW)
      timestamp: {
        $dateToString: {
          format: "%Y-%m-01T00:00:00Z",
          date: {
            $dateFromParts: {
              year: "$_id.year",
              month: "$_id.month",
              day: 1
            }
          }
        }
      },
      
      location: "$_id.location",
      source: "$source",
      year: "$_id.year",
      month: "$_id.month",
      
      // Zaokrąglenie metryk do 2 miejsc po przecinku (typ Double)
      temperature_2m: { $round: ["$avg_temp", 2] },
      precipitation: { $round: ["$sum_precip", 2] },
      wind_speed_10m: { $round: ["$avg_wind", 2] },
      
      geometry: 1,
      lat: 1,
      lon: 1
    }
  },
  {
    // ETAP 4: Zapis wyniku do nowej kolekcji
    $out: "api_openmeteo_monthly"
  }
]);
