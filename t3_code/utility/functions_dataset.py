from typing import Dict, List, Any, Optional, Union
from fastapi import WebSocket, WebSocketDisconnect
from datetime import datetime
from pathlib import Path
import os
import polars as pl
import pandas as pd
import traceback
import asyncio
import zipfile
import shutil
import hashlib
import logging
import json
import pytz
import aiofiles
import tempfile
from contextlib import suppress

from t3_code.utility.foundry_utility import FoundryConnection

# - - - - - Configuration / Handling Environment - - - - -

DOWNLOAD_BATCHSIZE = int(os.environ.get("DOWNLOAD_BATCHSIZE", 1000000))  # Rows per batch, needs id column in dataset

# - - -

def _resolve_dataset_root() -> Path:
    """Determine where datasets should live inside the container."""
    env_override = os.environ.get("FDT_DATASET_DIR")
    if env_override:
        path = Path(env_override)
        path.mkdir(parents=True, exist_ok=True)
        return path

    for candidate in ("/app/fdt-container/datasets", "/app/datasets"):
        path = Path(candidate)
        if path.exists():
            path.mkdir(parents=True, exist_ok=True)
            return path

    # Fallback that matches legacy deployments
    fallback = Path("/app/datasets")
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


DATASET_ROOT = _resolve_dataset_root()
METADATA_DIR = DATASET_ROOT / "metadata"
UNZIPPED_DIR = DATASET_ROOT / "unzipped"
ZIPPED_DIR = DATASET_ROOT / "zipped"
TEMP_DIR = DATASET_ROOT / "tmp"

for directory in (METADATA_DIR, UNZIPPED_DIR, ZIPPED_DIR, TEMP_DIR):
    directory.mkdir(parents=True, exist_ok=True)

# - - -

logger = logging.getLogger(__name__)

# - - - Full Sequences - - -

async def get(websocket: WebSocket, foundry_con: FoundryConnection) -> Any:
    """ Get a dataset from the Foundry, using Websocket for continous updates """
    keepalive_task: Optional[asyncio.Task] = None

    try:
        await websocket.accept()
        setattr(websocket, "_send_lock", asyncio.Lock())
        keepalive_task = asyncio.create_task(_websocket_keepalive(websocket))

        initial_req = await websocket.receive_json()
        names = initial_req.get("names", [])

        name_rid_pairs, message = foundry_con.get_valid_rids(names)
        
        if not name_rid_pairs:  # No valid RIDs
            await send_message(websocket, "error", False, f"ERROR | {message}")
            return

        from_dt = initial_req.get("from_dt", "2025-06-01")
        to_dt = initial_req.get("to_dt", "2025-06-30")

        # Send acknowledgment with validated datasets
        await send_message(websocket, "update", True, "Connection established, starting operation...", add={"datasets": list(name_rid_pairs.keys())})

        results = []
        for name, rid in name_rid_pairs.items():
            try:
                result = await get_single_dataset(websocket, foundry_con, rid, name, from_dt, to_dt)
                results.append(result)
                print(f"Successfully processed dataset: {name}", flush=True)
            except Exception as e:
                print(f"Error in dataset retrieval for {name}: {str(e)}", flush=True)
                results.append(e)

        print("RAN THROUGH", flush=True)

        # Send final overview message
        await send_message(websocket, "final", True, "DONE", add={"datasets": f"{results[0] if results else 'No results'}"})

    except WebSocketDisconnect:
        print("Client disconnected")

    except Exception as e:
        # Handle exceptions with a graceful error message to the client
        progress = progress if 'progress' in locals() else "-"
        func_amount = func_amount if 'func_amount' in locals() else "-"

        traceback.print_exc()

        await send_message(websocket, "final", False, f"ERROR | {e}")
        await websocket.close(code=1008)  # Policy violation code

    finally:
        if keepalive_task:
            keepalive_task.cancel()
            with suppress(asyncio.CancelledError):
                await keepalive_task
        if hasattr(websocket, "_send_lock"):
            delattr(websocket, "_send_lock")


# - - - - - High Priority - - - - -

# - - - Versions - - -

async def get_versions(rid: str, name: str) -> tuple[list[dict], str]:
    """
    Returns a list of dictionaries with the available versions of a dataset.

    Folder / File Structure:
    ```
    RID    | UUID for the Dataset (Foundry ID)
    SHA256 | SHA256 checksum of the zipped dataset, used as the filename for the zipped and unzipped files
    dates  | list of dates on which the dataset was created or a identical dataset was pulled

    datasets
    ├── metadata (RID.json)
    │   ├── 12a3b4c5-d6e7-8f90-1a2b-3c4d5e6f7g8h.json
    │   └── z9y8x7w6-v5u4-3210-z9y8-x7w6v5u4t3s2.json
    ├── zipped (SHA256.zip)
    │   ├── a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2.zip
    │   ├── f7e6d5c4b3a2f7e6d5c4b3a2f7e6d5c4b3a2f7e6d5c4b3a2f7e6d5c4b3a2f7e6.zip
    │   └── c3b2a1f7e6d5c3b2a1f7e6d5c3b2a1f7e6d5c3b2a1f7e6d5c3b2a1f7e6d5c3b2.zip
    └── unzipped (SHA256.csv)
        ├── a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2.csv
        └── c3b2a1f7e6d5c3b2a1f7e6d5c3b2a1f7e6d5c3b2a1f7e6d5c3b2a1f7e6d5c3b2.csv

    12a3b4c5-d6e7-8f90-1a2b-3c4d5e6f7g8h.json (metadata file)
    {
       "name": "<dataset_name>",
       "rid": "<dataset_uuid>",
       "versions": [
           {
               "sha256": "a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2",
               "dates": ["YYYY-MM-DD HH:MM:SS"],
               "zipped": True,
               "unzipped": True
           },
           ...
       ]
    }
    ```
    """

    BASE_DIR = METADATA_DIR / f"{rid}.json"
    try:
        async with asyncio.Lock():
            async with aiofiles.open(BASE_DIR, 'r') as file:
                content = await file.read()
            data = json.loads(content)
            versions = data.get("versions", [])
            message = f"{len(versions)} available versions found."
            message = f"{len(versions)} available versions found."
    except FileNotFoundError as e:
        versions = []
        message = f"No metadata file found."
    except json.JSONDecodeError:
        versions = []
        message = f"Invalid JSON format in metadata file."
    except Exception as e:
        versions = []
        message = f"Error reading metadata: {str(e)}"  # TODO: Check how to change this to not expose RIDs or sensible data to the Enduser

    if versions:  # Sort versions descending (newest first) based on the last date in each version's dates list
        # TODO: check if necessary, as already trying to do this while writing to the file
        versions.sort(key=lambda v: datetime.fromisoformat(v.get("dates", [None])[-1]), reverse=True)

    return versions, message


async def get_filtered_versions(websocket: WebSocket, rid: str, name: str, date_start: str, date_end: str = None) -> tuple[list[dict], str]:
    """ Returns the version object for a dataset filtered by date range """

    versions, message = await get_versions(rid, name)

    if not versions:
        await send_message(websocket, "neutral", False, f"No available versions found for dataset '{name}'. {message}")
        return [], f"No versions for dataset '{name}' with RID '{rid}'. {message}"

    date_start_dt = datetime.fromisoformat(date_start).replace(tzinfo=None)
    # Ensure date_end_dt is a naive datetime to match with the ones from fromisoformat
    date_end_dt = datetime.fromisoformat(date_end).replace(tzinfo=None) if date_end else datetime.now(pytz.UTC).replace(tzinfo=None)

    filtered_versions = [  # Filter versions based on the date range
        version for version in versions
        if any(date_start_dt <= datetime.fromisoformat(date).replace(tzinfo=None) <= date_end_dt for date in version.get("dates", []))
    ]

    if not filtered_versions:
        message2 = f"No versions between '{date_start}' and '{date_end}' found for dataset '{rid}'. Internal Message: {message}"
    else:
        message2 = f"Found {len(filtered_versions)} versions for dataset '{rid}' between '{date_start}' and '{date_end}'."

    await send_message(websocket, "update", True, message2)
    return filtered_versions, message2


async def get_first_filtered_version(websocket: WebSocket, rid: str, name: str, date_start: str, date_end: str = None) -> tuple[Optional[dict], str]:
    """ Returns the first version object for a dataset filtered by date range """

    versions, message = await get_filtered_versions(websocket, rid, name, date_start, date_end)

    if not versions:
        return None, message

    return versions[0], message

# - - - Load Datasets - - -

async def load_datasets(sha256: str) -> pl.DataFrame | None:

    unzipped_path = UNZIPPED_DIR / f"{sha256}.csv"

    if not unzipped_path.exists():
        return None

    try:
        df = await asyncio.to_thread(pl.read_csv, unzipped_path, infer_schema_length=0)
        return df
    except Exception as e:
        print("ERROR IN load_datasets:", e, flush=True)
        return None
    
# - - - Unzip Datasets - - -

async def unzip_dataset(sha256: str) -> bool:
    """Unzip large archives without blocking the event loop."""

    return await asyncio.to_thread(_unzip_dataset_sync, sha256)


async def zip_dataset(sha256: str) -> bool:
    """Zip large CSVs without blocking the event loop."""

    return await asyncio.to_thread(_zip_dataset_sync, sha256)




# - - - Download - - -

async def download_dataset(websocket: WebSocket, foundry_con: FoundryConnection, rid: str, name: str) -> bool:
    """ Trigger the download of a dataset from the Foundry """
    
    await websocket.send_json({
        "type": "update",
        "success": True,
        "message": f"Downloading dataset '{name}' from Foundry..."  # TODO: add detection of progress and realize when download is not starting due to connection issues
    })

    try:
        # - - - Execute the Foundry SQL query asynchronously - - -

        df_count = await asyncio.to_thread(
            foundry_con.foundry_context.foundry_sql_server.query_foundry_sql,
            f"SELECT COUNT(*) FROM `{foundry_con.prefix}{rid}`"
        )

        # Extract row count from the COUNT(*) query result (returns pandas DataFrame)
        try:
            row_count = int(df_count.iloc[0, 0])
        except (ValueError, IndexError):
            row_count = 0

        # End if no rows found
        if row_count == 0:
            await send_message(websocket, "final", False, f"No rows found for dataset '{name}'.")
            return False, ""

        # DOWNLOAD WITH LAZY POLARS

        await send_message(websocket, "update", True, f"Downloading all {row_count} rows for dataset '{name}'...")

        dataset = foundry_con.foundry_context.get_dataset(f"{foundry_con.prefix}{rid}")
        lazy_df: pl.LazyFrame = dataset.to_lazy_polars()

        df: pl.DataFrame = await asyncio.to_thread(lazy_df.collect)
        print(df, flush=True)

        await send_message(websocket, "update", True, f"Dataset '{name}' downloaded successfully. Writing to disk...")

        tmp_csv_path = UNZIPPED_DIR / f"tmp_{rid}_{datetime.now(pytz.UTC).strftime('%Y%m%d_%H%M%S')}.csv"
        df.write_csv(tmp_csv_path)

        await send_message(websocket, "update", True, f"Dataset '{name}' written to disk. Calculating checksum...")

        # CHECKSUM
        sha256 = await asyncio.to_thread(_compute_file_sha256, tmp_csv_path)

        # RENAME
        new_csv_path = UNZIPPED_DIR / f"{sha256}.csv"
        new_csv_path.parent.mkdir(parents=True, exist_ok=True)

        if new_csv_path.exists():
            try:
                await asyncio.to_thread(new_csv_path.unlink)
            except FileNotFoundError:
                pass

        await asyncio.to_thread(shutil.move, str(tmp_csv_path), str(new_csv_path))

        # ZIP
        is_zipped = await zip_dataset(sha256)
        if not is_zipped:
            await send_message(
                websocket,
                "update",
                False,
                f"Failed to zip dataset '{name}'."
            )
            return False, sha256

        # METADATA
        version = {
            "sha256": sha256,
            "dates": [datetime.now(pytz.UTC).replace(tzinfo=None).isoformat()],
            "unzipped": True,
            "zipped": True
        }

        is_added = await add_version_to_metadata(name, rid, version)
        if not is_added:
            await send_message(
                websocket,
                "update",
                False,
                f"Failed to update metadata for dataset '{name}'."
            )
            return False, sha256

        return True, sha256

    except Exception as e:
        logger.exception("Failed to download dataset %s (%s)", name, rid)
        await send_message(
            websocket,
            "update",
            False,
            f"Failed to download dataset '{name}'. {e}"
        )
        return False, ""

# - - - Download - - -

# async def download_dataset(websocket: WebSocket, foundry_con: FoundryConnection, rid: str, name: str) -> tuple[bool, str]:
#     """Trigger the download of a dataset from the Foundry and persist it locally."""

#     await send_message(
#         websocket,
#         "update",
#         True,
#         f"Downloading dataset '{name}' from Foundry..."
#     )

#     try:
#         df = await asyncio.to_thread(
#             foundry_con.foundry_context.foundry_sql_server.query_foundry_sql,
#             f"SELECT * FROM `ri.foundry.main.dataset.{rid}`",
#             timeout=3600
#         )
#     except Exception as exc:
#         logger.exception("Failed to download dataset %s (%s)", name, rid)
#         await send_message(
#             websocket,
#             "update",
#             False,
#             f"Failed to download dataset '{name}'. {exc}"
#         )
#         return False, ""

#     dataset_meta = _get_dataframe_metadata(df)

#     temp_path: Optional[Path] = None
#     file_size_bytes: Optional[int] = None
#     try:
#         temp_path = await asyncio.to_thread(_write_dataframe_to_temp_csv, df)
#         sha256 = await asyncio.to_thread(_compute_file_sha256, temp_path)

#         unzipped_path = UNZIPPED_DIR / f"{sha256}.csv"
#         unzipped_path.parent.mkdir(parents=True, exist_ok=True)

#         if unzipped_path.exists():
#             try:
#                 await asyncio.to_thread(unzipped_path.unlink)
#             except FileNotFoundError:
#                 pass

#         await asyncio.to_thread(shutil.move, str(temp_path), str(unzipped_path))
#         file_size_bytes = await asyncio.to_thread(_get_file_size, unzipped_path)

#     except Exception as exc:
#         logger.exception("Failed to persist dataset %s (%s)", name, rid)
#         await send_message(
#             websocket,
#             "update",
#             False,
#             f"Failed to persist dataset '{name}'. {exc}"
#         )
#         if temp_path and temp_path.exists():
#             try:
#                 temp_path.unlink()
#             except FileNotFoundError:
#                 pass
#         return False, ""
#     finally:
#         del df

#     await send_message(
#         websocket,
#         "update",
#         True,
#         f"Dataset '{name}' downloaded successfully.",
#         add={
#             "rows": dataset_meta.get("rows"),
#             "columns": dataset_meta.get("columns"),
#             "approx_size": _human_readable_size(file_size_bytes)
#         }
#     )

    

# - - - Metadata Maintenance - - -

async def add_metadata(name: str, rid: str, versions: list[dict] = []):

    metadata_path = METADATA_DIR / f"{rid}.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    if not metadata_path.exists():
        metadata_path.write_text(json.dumps({"name": name, "rid": rid, "versions": versions}, indent=4))
        return True

    return False


async def add_version_to_metadata(name: str, rid: str, version: dict) -> bool:
    """ Add a new version entry to the dataset metadata """

    is_new_created = await add_metadata(name, rid, [version])
    if not is_new_created:
        # If metadata already exists, update it
        metadata_path = METADATA_DIR / f"{rid}.json"
        metadata = json.loads(metadata_path.read_text())

        tmp = metadata["versions"]
        tmp.append(version)

        if tmp:  # Sort versions descending (newest first) based on the last date in each version's dates list
            tmp.sort(key=lambda v: datetime.fromisoformat(v.get("dates", ["1970-01-01 00:00:00"])[-1]), reverse=True)

        metadata["versions"] = tmp

        metadata_path.write_text(json.dumps(metadata, indent=4))

    return True

# - - - Delete Datasets - - -

async def delete_unzipped(req: dict) -> Any:
    """ Trigger deletion of one or multiple unzipped dataset files """
    return {"message": "delete_unzipped endpoint not implemented yet"}

# - - - Less Priority - - -

async def delete_zipped(req: dict) -> Any:
    """ Trigger deletion of one or multiple zipped dataset files """
    return {"message": "delete_zipped endpoint not implemented yet"}

async def delete(req: dict) -> Any:
    """ Trigger deletion of dataset (both zipped and unzipped files) """
    return {"message": "delete endpoint not implemented yet"}

async def list_datasets(req: dict) -> Any:
    """ Returns a list of all available datasets and their versions """
    return {"message": "list endpoint not implemented yet"}

async def info(req: dict) -> Any:
    """ Returns information about one or multiple datasets """
    return {"message": "info endpoint not implemented yet"}

# - - - Core Processes - - -

async def get_single_dataset(websocket: WebSocket, foundry_con: FoundryConnection, rid: str, name: str, date_start, date_end = None) -> Union[pl.DataFrame, None]:
    """ Get a single dataset by name and uuid from the Foundry """

    print("IN: get_single_dataset", flush=True)

    version, message = await get_first_filtered_version(websocket, rid, name, date_start, date_end)

    print("HERE", flush=True)

    if version:

        print("IN VERSION IF", flush=True)

        sha256 = version.get('sha256', None)
        if not sha256:
            print("ERROR1", flush=True)
            raise ValueError(f"SHA256 not found in version data for dataset {rid}.")
        
        # UNZIP
        if not version.get("unzipped", False):
            is_unzipped = await unzip_dataset(version['sha256'])
            if not is_unzipped:
                print("ERROR2", flush=True)
                raise FileNotFoundError(f"Unzipped file for {rid} with SHA256 {version['sha256']} not found or could not be unzipped.")

        print("OUT VERSION IF", flush=True)

    else:

        print("IN VERSION ELSE", flush=True)

        # - NO VERSION FOUND -
        is_downloaded, sha256 = await download_dataset(websocket, foundry_con, rid, name)
        if not is_downloaded:
            print("ERROR3", flush=True)
            raise FileNotFoundError(f"Dataset {rid} could not be downloaded or does not exist.")

        print("OUT VERSION ELSE", flush=True)

    print("ABOUT TO GET DATASET", flush=True)

    unzipped_path = UNZIPPED_DIR / f"{sha256}.csv"
    if not unzipped_path.exists():
        print("ERROR4", flush=True)
        raise FileNotFoundError(f"Dataset {rid} with SHA256 {sha256} not found.")

    print("OUT: get_single_dataset", flush=True)

    return sha256

# - - - Utility Functions - - -

def _write_dataframe_to_temp_csv(df: Any) -> Path:
    temp_dir = TEMP_DIR
    temp_dir.mkdir(parents=True, exist_ok=True)

    fd, temp_path = tempfile.mkstemp(dir=temp_dir, suffix=".csv")
    os.close(fd)
    path_obj = Path(temp_path)

    try:
        destination = str(path_obj)
        if isinstance(df, pl.DataFrame):
            df.write_csv(destination)
        elif hasattr(df, "to_csv"):
            try:
                df.to_csv(destination, index=False, encoding="utf-8")
            except TypeError:
                df.to_csv(destination, index=False)
        else:
            raise TypeError(f"Unsupported dataframe type: {type(df)}")
    except Exception:
        if path_obj.exists():
            try:
                path_obj.unlink()
            except FileNotFoundError:
                pass
        raise

    return path_obj


def _compute_file_sha256(file_path: Path) -> str:
    hash_obj = hashlib.sha256()
    with open(file_path, "rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            hash_obj.update(chunk)
    return hash_obj.hexdigest()


def _get_file_size(file_path: Path) -> int:
    return file_path.stat().st_size


def _human_readable_size(num_bytes: int | None) -> str:
    if num_bytes is None:
        return "unknown"

    step_unit = 1024.0
    size = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < step_unit:
            return f"{size:.2f} {unit}"
        size /= step_unit
    return f"{size:.2f} PB"


def _get_dataframe_metadata(df: Any) -> dict[str, Optional[int]]:
    try:
        rows = len(df)
    except Exception:
        rows = None

    columns = None
    if isinstance(df, pl.DataFrame):
        columns = len(df.columns)
    elif hasattr(df, "columns"):
        try:
            columns = len(df.columns)
        except Exception:
            columns = None

    return {"rows": rows, "columns": columns}


async def _websocket_keepalive(websocket: WebSocket, interval: int = 50) -> None:
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                await send_message(websocket, "keepalive", True, "Heartbeat - still processing, please keep waiting...")
            except Exception:
                logger.debug("Keepalive send failed; stopping keepalive loop.", exc_info=True)
                break
    except asyncio.CancelledError:
        raise


def _unzip_dataset_sync(sha256: str) -> bool:
    zipped_path = ZIPPED_DIR / f"{sha256}.zip"
    unzipped_path = UNZIPPED_DIR / f"{sha256}.csv"
    temp_extract_dir = DATASET_ROOT / f"temp_extract_{sha256}"

    if not zipped_path.exists() or zipped_path.suffix != ".zip":
        return False

    try:
        temp_extract_dir.mkdir(exist_ok=True)

        with zipfile.ZipFile(zipped_path, "r") as zip_ref:
            zip_ref.extractall(temp_extract_dir)

        csv_files = list(temp_extract_dir.glob("**/*.csv"))
        if not csv_files:
            shutil.rmtree(temp_extract_dir, ignore_errors=True)
            return False

        source_csv = csv_files[0]
        unzipped_path.parent.mkdir(parents=True, exist_ok=True)
        if unzipped_path.exists():
            unzipped_path.unlink()
        shutil.move(str(source_csv), str(unzipped_path))

        return True
    except Exception:
        logger.exception("Failed to unzip dataset %s", sha256)
        return False
    finally:
        if temp_extract_dir.exists():
            shutil.rmtree(temp_extract_dir, ignore_errors=True)


def _zip_dataset_sync(sha256: str) -> bool:
    unzipped_path = UNZIPPED_DIR / f"{sha256}.csv"
    zipped_path = ZIPPED_DIR / f"{sha256}.zip"

    if not unzipped_path.exists() or unzipped_path.suffix != ".csv":
        return False

    try:
        zipped_path.parent.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(zipped_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.write(unzipped_path, arcname=f"{sha256}.csv")
        return True
    except Exception:
        logger.exception("Failed to zip dataset %s", sha256)
        return False


async def send_message(websocket: WebSocket, type: str, is_success: bool, message: str, add: dict = None) -> None:
    """ Send a message to the WebSocket client """
    payload = {
        "type": type,
        "success": is_success,
        "message": message,
        **(add or {})
    }

    lock: Optional[asyncio.Lock] = getattr(websocket, "_send_lock", None)
    if lock:
        async with lock:
            await websocket.send_json(payload)
    else:
        await websocket.send_json(payload)

# - - - Cancellation - - - (implement later)

# async def check_cancellation_non_blocking(websocket: WebSocket) -> tuple[str, bool]:
#     """ Check if the request contains a cancellation signal without blocking """
#     try:
#         # Use asyncio.wait_for with timeout=0 to make it non-blocking
#         req = await asyncio.wait_for(websocket.receive_json(), timeout=0.001)
#         if req.get("cancel", False) is True:
#             return "CANCELED | The process was canceled by the user.", True
#         return "", False
#     except asyncio.TimeoutError:
#         # No message received, continue processing
#         return "", False
#     except Exception:
#         # Any other error (like connection closed), treat as cancellation
#         return "CANCELED | Connection lost.", True

# async def check_cancellation(websocket: WebSocket) -> tuple[str, bool]:
#     """ Check if the request contains a cancellation signal (blocking version) """
#     try:
#         req = await websocket.receive_json()
#         if req.get("cancel", False) is True:
#             return "CANCELED | The process was canceled by the user.", False
#         return "", True
#     except Exception:
#         return "CANCELED | Connection lost.", False