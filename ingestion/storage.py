import json
from pathlib import Path
from datetime import datetime
from config.settings import RAW_DATA_PATH


def store_raw_data(source: str, symbol: str, data: dict):

    now = datetime.utcnow()

    path = (
        Path(RAW_DATA_PATH)
        / source
        / str(now.year)
        / f"{now.month:02d}"
        / f"{now.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{symbol}.json"

    try:
        with open(file_path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        # don't raise from storage; log to stderr instead
        print(f"Failed to write raw data {file_path}: {e}")


def store_raw_error(source: str, symbol: str, data: dict):
    """Store error responses in a separate errors folder with the same date layout."""
    now = datetime.utcnow()

    path = (
        Path(RAW_DATA_PATH)
        / "errors"
        / source
        / str(now.year)
        / f"{now.month:02d}"
        / f"{now.day:02d}"
    )
    path.mkdir(parents=True, exist_ok=True)

    file_path = path / f"{symbol}.json"

    with open(file_path, "w") as f:
        try:
            json.dump(data, f)
        except Exception as e:
            print(f"Failed to write raw error {file_path}: {e}")


def store_checkpoint(summary: dict):
    """Write a run checkpoint/summary JSON to RAW_DATA_PATH/checkpoints/<timestamp>.json"""
    now = datetime.utcnow()
    path = Path(RAW_DATA_PATH) / "checkpoints"
    path.mkdir(parents=True, exist_ok=True)
    file_path = path / f"checkpoint_{now.strftime('%Y%m%dT%H%M%SZ')}.json"

    with open(file_path, "w") as f:
        try:
            json.dump(summary, f)
        except Exception as e:
            print(f"Failed to write checkpoint {file_path}: {e}")
