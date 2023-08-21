import hashlib
import json
import os

from typing_extensions import Literal


def _get_storage_env() -> Literal["fs", "dbfs"]:
    if "DATABRICKS_RUNTIME_VERSION" in os.environ:
        return "dbfs"
    else:
        return "fs"


def load_asset_value(asset_key: str, storage_path: str) -> int:
    env = _get_storage_env()
    if env == "dbfs":
        # Temporarily hardcoding values
        if asset_key == "number_x":
            return 2
        elif asset_key == "number_y":
            return 4
        else:
            raise Exception(f"Unknown asset key: {asset_key}")
    elif env == "fs":
        with open(os.path.join(storage_path, asset_key), "r") as f:
            content = f.read()
            json_content = json.loads(content)
            # This is only supposed to work with numbers
            assert isinstance(json_content, int)
            return json_content
    else:
        raise Exception(f"Unknown storage environment: {env}")


def store_asset_value(asset_key: str, storage_path: str, value: int):
    env = _get_storage_env()
    if env == "dbfs":
        # Temporarily avoiding writing to DBFS
        pass
    elif env == "fs":
        with open(os.path.join(storage_path, asset_key), "w") as f:
            return f.write(json.dumps(value))


def compute_data_version(value: int):
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()
