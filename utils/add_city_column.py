import logging
import os
import pandas as pd
import glob

def add_city_column() -> bool :
    """
    Add the city column to each weather historical data file (Open-Meteo CSVs),
    inferred from the filename. The modified data overwrites the original file.
    :return: True if operation was successful, False otherwise
    """
    try:
        # Identify the directory containing the files
        input_dir = "../data/raw"
        # Identify the pattern of the filename
        pattern = os.path.join(input_dir, "open-meteo-*.csv")
        files = glob.glob(pattern)

        if not files:
            logging.warning(f"No files matched in {input_dir}")
            return False

        for file_path in files:
            filename = os.path.basename(file_path)
            # Extract the city from the filename after the last hyphen
            city = filename.split("-")[-1].replace(".csv", "").strip()

            df = pd.read_csv(file_path)

            if "city" in df.columns:
                logging.info(f"Skipping {filename}: 'city' column already exists.")
                continue

            # Add the city column into the data
            df["city"] = city

            # Overwrite the file with updated data
            df.to_csv(file_path, index=False)

        return True
    except Exception as e:
        logging.error(f"Failed to add city column: {e}")
    return False

add_city_column()