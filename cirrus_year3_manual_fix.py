import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress

import azure.storage.filedatalake as azurelake  # type: ignore
import config
import pydicom

# Number of parallel workers (one per subfolder)
MAX_WORKERS = 8
_print_lock = threading.Lock()


BASE_ORIGINAL = "AI-READI/year3-fix/retinal_octa/enface/zeiss_cirrus"
BASE_OUTPUT = "AI-READI/year3-fix/retinal_octa/enface/zeiss_cirrus_fixed"


def update_cirrus_enface(inputfile, outputfile):
    dcm = pydicom.dcmread(inputfile)

    dcm.ImageOrientationPatient = ""
    dcm.PixelSpacing = [0.005859375, 0.005859375]

    dcm.save_as(outputfile, write_like_original=False)


def get_file_system_client():
    """Connect to Azure Data Lake (fairhubproduction) using project config."""
    return azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name="stage-one",
    )


def list_subfolders(fs_client, folder_path):
    """List immediate subfolder names under folder_path (e.g. ['1269', '1270'])."""
    prefix = folder_path.rstrip("/") + "/"
    paths = fs_client.get_paths(path=folder_path, recursive=True)
    subfolders = set()
    for item in paths:
        name = (item.name or "").strip()
        if not name or name == folder_path.strip("/"):
            continue
        rest = name[len(prefix) :] if name.startswith(prefix) else name
        parts = rest.split("/")
        if parts and parts[0]:
            subfolders.add(parts[0])
    return sorted(subfolders)


def list_files_in_folder(fs_client, folder_path):
    """List all files (blobs) under folder_path; skip directory markers."""
    paths = fs_client.get_paths(path=folder_path, recursive=True)
    files = []
    for item in paths:
        name = (item.name or "").strip()
        if not name or name == folder_path.strip("/"):
            continue
        if name.endswith("/"):
            continue
        with suppress(Exception):
            fc = fs_client.get_file_client(file_path=name)
            props = fc.get_file_properties()
            if getattr(props, "metadata", {}) and props.metadata.get("hdi_isfolder"):
                continue
        files.append(name)
    return files


def process_one_file(fs_client, remote_path):
    """Download one file, run update_cirrus_enface, upload to BASE_OUTPUT. Returns (success, message)."""
    prefix = BASE_ORIGINAL.rstrip("/") + "/"
    if not remote_path.startswith(prefix):
        return False, f"Path not under BASE_ORIGINAL: {remote_path}"
    relative = remote_path[len(prefix) :]
    out_blob_path = f"{BASE_OUTPUT.rstrip('/')}/{relative}"

    download_fd = tempfile.NamedTemporaryFile(
        delete=False, suffix=".dcm", prefix="cirrus_dl_"
    )
    write_fd = tempfile.NamedTemporaryFile(
        delete=False, suffix=".dcm", prefix="cirrus_out_"
    )
    try:
        download_path = download_fd.name
        write_path = write_fd.name
    finally:
        download_fd.close()
        write_fd.close()

    try:
        file_client = fs_client.get_file_client(file_path=remote_path)
        with open(download_path, "wb") as f:
            f.write(file_client.download_file().readall())
    except Exception as e:
        for p in (download_path, write_path):
            with suppress(FileNotFoundError):
                os.unlink(p)
        return False, f"Download failed {remote_path}: {e}"

    try:
        update_cirrus_enface(download_path, write_path)
    except Exception as e:
        for p in (download_path, write_path):
            with suppress(FileNotFoundError):
                os.unlink(p)
        return False, f"Transform failed {remote_path}: {e}"

    try:
        out_client = fs_client.get_file_client(file_path=out_blob_path)
        with open(write_path, "rb") as f:
            out_client.upload_data(f.read(), overwrite=True)
        return True, out_blob_path
    except Exception as e:
        return False, f"Upload failed {out_blob_path}: {e}"
    finally:
        for p in (download_path, write_path):
            with suppress(FileNotFoundError):
                os.unlink(p)


def _safe_print(msg: str) -> None:
    with _print_lock:
        print(msg)


def process_one_folder(fs_client, subfolder_name):
    """Process all files in BASE_ORIGINAL/subfolder_name; download, fix, upload. Returns (subfolder, ok_count, skip_count)."""
    folder_path = f"{BASE_ORIGINAL.rstrip('/')}/{subfolder_name}"
    files = list_files_in_folder(fs_client, folder_path)
    ok, skip = 0, 0
    for remote_path in files:
        success, msg = process_one_file(fs_client, remote_path)
        if success:
            ok += 1
            _safe_print(f"  [OK] {msg}")
        else:
            skip += 1
            _safe_print(f"  [SKIP] {msg}")
    return subfolder_name, ok, skip


def main():
    """Fix the cirrus data files: list subfolders of BASE_ORIGINAL, process each folder in parallel, upload to BASE_OUTPUT."""
    fs_client = get_file_system_client()
    subfolders = list_subfolders(fs_client, BASE_ORIGINAL)
    if not subfolders:
        print(f"No subfolders found under {BASE_ORIGINAL}")
        return
    print(f"Found {len(subfolders)} subfolder(s) under {BASE_ORIGINAL}: {subfolders}")
    total_ok, total_skip = 0, 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_one_folder, fs_client, name): name
            for name in subfolders
        }
        for future in as_completed(futures):
            subfolder_name = futures[future]
            try:
                name, ok, skip = future.result()
                total_ok += ok
                total_skip += skip
                _safe_print(f"  Done folder {name}: {ok} OK, {skip} skip")
            except Exception as e:
                _safe_print(f"  [ERROR] folder {subfolder_name}: {e}")
    print(f"Total: {total_ok} OK, {total_skip} skip")


if __name__ == "__main__":
    main()
