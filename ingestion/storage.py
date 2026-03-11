import json
from pathlib import Path
from datetime import datetime
from config.settings import RAW_DATA_PATH


def store_raw_data(source: str, symbol: str, data: dict):

    now = datetime.utcnow()

    path = Path(RAW_DATA_PATH) / source / str(now.year) / f"{now.month:02d}" / f"{now.day:02d}"
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{symbol}.json"

    with open(file_path, "w") as f:
        json.dump(data, f)
