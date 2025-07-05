import logging
import os

import pandas as pd

weather_code_map = {
    0: "clear sky",
    1: "mainly clear",
    2: "partly cloudy",
    3: "overcast clouds",
    45: "fog",
    48: "rime fog",

    51: "light drizzle",
    53: "moderate drizzle",
    55: "dense drizzle",

    56: "light freezing drizzle",
    57: "dense freezing drizzle",

    61: "light rain",
    63: "moderate rain",
    65: "heavy rain",

    66: "light freezing rain",
    67: "heavy freezing rain",

    71: "light snow",
    73: "moderate snow",
    75: "heavy snow",

    77: "snow grains",

    80: "light rain showers",
    81: "moderate rain showers",
    82: "violent rain showers",

    85: "light snow showers",
    86: "heavy snow showers",

    95: "thunderstorm",
    96: "thunderstorm with slight hail",
    99: "thunderstorm with heavy hail",
}

target_columns = [
    'city',
    'date',
    'temperature',
    'feels_like',
    'humidity',
    'description',
    'wind_speed',
]

rename_mapping = {
    'city': 'city',
    'time': 'date',
    'temperature_2m_mean (°C)': 'temperature',
    'apparent_temperature_mean (°C)': 'feels_like',
    'relative_humidity_2m_mean (%)': 'humidity',
    'wind_speed_10m_mean (km/h)': 'wind_speed',
}

def transform_and_merge() -> str:
    try:
        input_dir = "../data/raw"

        all_data = []
        for file in os.listdir(input_dir):
            if file.startswith("open-meteo") and file.endswith(".csv"):
                df = pd.read_csv(f"{input_dir}/{file}")

                # Changing the weather code into description
                df["description"] = df["weather_code (wmo code)"].map(weather_code_map)

                # Converting wind speed to m/s to match that of the daily data (in km/h)
                df["wind_speed_10m_mean (km/h)"] = (df["wind_speed_10m_mean (km/h)"] / 3.6).round(2)

                # Renaming columns
                df = df.rename(columns=rename_mapping)

                # Reorder columns to match target
                df = df[[col for col in target_columns if col in df.columns]]

                all_data.append(df)

                os.makedirs("../data/processed", exist_ok=True)

                # Combine all dataframes and save
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df.to_csv(f"../data/processed/global_history_weather.csv", index=False)

        return "File generated successfully"
    except Exception as e:
        logging.error(e)
    return "Something went wrong"

transform_and_merge()





