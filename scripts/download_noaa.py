import os
import requests
from datetime import datetime

RAW_DIR = "data/raw"
YEAR_DIR = "data/raw/ghcnd_all_years"

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(YEAR_DIR, exist_ok=True)

BASE_URL = "https://www.ncei.noaa.gov/pub/data/ghcn/daily"
CURRENT_YEAR = datetime.now().year


def download_file(url, output_path):
    if os.path.exists(output_path):
        print(f"File already exists, skip download: {output_path}")
        return

    print("Downloading:", url)
    response = requests.get(url, timeout=120)

    if response.status_code == 200:
        with open(output_path, "wb") as f:
            f.write(response.content)
        print("Saved:", output_path)
    else:
        print("Failed:", url, response.status_code)


download_file(
    f"{BASE_URL}/by_year/{CURRENT_YEAR}.csv.gz",
    os.path.join(YEAR_DIR, f"{CURRENT_YEAR}.csv.gz")
)

download_file(
    f"{BASE_URL}/ghcnd-stations.txt",
    os.path.join(RAW_DIR, "ghcnd-stations.txt")
)

download_file(
    f"{BASE_URL}/ghcnd-countries.txt",
    os.path.join(RAW_DIR, "ghcnd-countries.txt")
)

print("NOAA download completed.")