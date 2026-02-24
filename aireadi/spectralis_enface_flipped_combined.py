"""
Enface image processing for year3-enface-check.

- Heidelberg Spectralis: download from enface-original/heidelberg_spectralis/{patient_id}/,
  flip horizontally + rotate 180° with pydicom, save as heidelberg_spectralis_<original_filename>,
  upload to enface-flipped-combined/{patient_id}/.

- Topcon Maestro2, Topcon Triton, Zeiss Cirrus: download from respective enface-original
  subfolders, rename only (no transform) to <device>_<original_filename>, upload to the
  same enface-flipped-combined/{patient_id}/.

Uses b2aistaging Azure storage via config.AZURE_STORAGE_CONNECTION_STRING and
stage-1-container (see dev/download_folder.py, config.py).
"""

import os
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import suppress

import azure.storage.filedatalake as azurelake  # type: ignore
import config
import numpy as np
import pydicom
from pydicom.uid import ExplicitVRLittleEndian

# -----------------------------------------------------------------------------
# Azure paths (b2aistaging, stage-1-container)
# -----------------------------------------------------------------------------
BASE_ORIGINAL = "AI-READI/year3-enface-check/enface-original"
BASE_OUTPUT = "AI-READI/year3-enface-check/enface-flipped-combined"

# Heidelberg Spectralis: transform (flip + rotate) and prefix filename
HEIDELBERG_SPECTRALIS_SUBFOLDER = "heidelberg_spectralis"
HEIDELBERG_PREFIX = "heidelberg_spectralis_"

# Other devices: copy/rename only, same output folder
OTHER_DEVICES = [
    ("topcon_maestro2", "topcon_maestro2_"),
    ("topcon_triton", "topcon_triton_"),
    ("zeiss_cirrus", "zeiss_cirrus_"),
]

# Process only this patient ID when set (None = all patients)
PATIENT_ID_FILTER = None

# Number of parallel workers for download/transform/upload (per device)
MAX_WORKERS = 8

# Lock so print from workers doesn't interleave
_print_lock = threading.Lock()


def get_file_system_client():
    """Connect to Azure Data Lake (b2aistaging) using project config."""
    return azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_CONNECTION_STRING,
        file_system_name="stage-1-container",
    )


def list_patient_folders(fs_client, folder_path):
    """
    List immediate subfolders of folder_path (patient IDs).
    get_paths(recursive=False) would list only direct children; we use recursive=True
    and derive unique top-level folder names under folder_path.
    """
    prefix = folder_path.rstrip("/") + "/"
    paths = fs_client.get_paths(path=folder_path, recursive=True)
    patient_ids = set()
    for item in paths:
        name = (item.name or "").strip()
        if not name or name == folder_path.strip("/"):
            continue
        # First path segment after folder_path is the patient folder
        rest = name[len(prefix) :] if name.startswith(prefix) else name
        parts = rest.split("/")
        if parts and parts[0]:
            patient_ids.add(parts[0])
    return sorted(patient_ids)


def list_files_in_folder(fs_client, folder_path):
    """List all files (blobs) under folder_path; skip directory markers."""
    paths = fs_client.get_paths(path=folder_path, recursive=True)
    files = []
    for item in paths:
        name = (item.name or "").strip()
        if not name:
            continue
        # Skip directory markers (no file extension or metadata says folder)
        if name.endswith("/"):
            continue
        with suppress(Exception):
            fc = fs_client.get_file_client(file_path=name)
            props = fc.get_file_properties()
            if getattr(props, "metadata", {}) and props.metadata.get("hdi_isfolder"):
                continue
        files.append(name)
    return files


def _safe_print(msg: str) -> None:
    with _print_lock:
        print(msg)


def flip_and_rotate_spectralis(input_path: str, output_path: str) -> None:
    """Load DICOM, flip horizontally and rotate 180°. Handles 2D and 3D (multi-frame)."""
    ds = pydicom.dcmread(input_path)
    img = ds.pixel_array

    if img.ndim == 3:  # multi-frame
        flipped = np.flip(img, axis=2)
        rotated = np.rot90(flipped, k=2, axes=(1, 2))
    else:
        flipped = np.fliplr(img)
        rotated = np.rot90(flipped, k=2)

    rotated = rotated.astype(img.dtype)

    ds.PixelData = rotated.tobytes()
    ds.Rows, ds.Columns = rotated.shape[-2], rotated.shape[-1]

    # Force explicit uncompressed transfer syntax
    ds.file_meta.TransferSyntaxUID = ExplicitVRLittleEndian
    ds.is_little_endian = True
    ds.is_implicit_VR = False

    ds.save_as(output_path, write_like_original=False)


def _process_one_heidelberg_file(
    fs_client: azurelake.FileSystemClient, patient_id: str, remote_path: str
) -> tuple[bool, str]:
    """Download one file, flip+rotate, upload. Returns (success, out_blob_path or error)."""
    original_name = os.path.basename(remote_path)
    out_name = HEIDELBERG_PREFIX + original_name
    out_blob_path = f"{BASE_OUTPUT}/{patient_id}/{out_name}"

    download_fd = tempfile.NamedTemporaryFile(
        delete=False, suffix=".dcm", prefix="enface_dl_"
    )
    write_fd = tempfile.NamedTemporaryFile(
        delete=False, suffix=".dcm", prefix="enface_out_"
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
        flip_and_rotate_spectralis(download_path, write_path)
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


def process_heidelberg_spectralis(fs_client):
    """
    Process heidelberg_spectralis: for each patient folder, download each image,
    flip + rotate, save as heidelberg_spectralis_<original_filename>, upload to
    enface-flipped-combined/{patient_id}/. Runs file tasks in parallel.
    """
    input_prefix = f"{BASE_ORIGINAL}/{HEIDELBERG_SPECTRALIS_SUBFOLDER}"
    patient_ids = list_patient_folders(fs_client, input_prefix)
    if not patient_ids:
        print(f"No patient folders found under {input_prefix}")
        return
    if PATIENT_ID_FILTER is not None:
        patient_ids = [p for p in patient_ids if p == PATIENT_ID_FILTER]
        if not patient_ids:
            print(f"  Patient ID {PATIENT_ID_FILTER} not found under {input_prefix}")
            return
        print(f"  (filter: only patient {PATIENT_ID_FILTER})")

    tasks = [
        (patient_id, remote_path)
        for patient_id in patient_ids
        for remote_path in list_files_in_folder(
            fs_client, f"{input_prefix}/{patient_id}"
        )
    ]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _process_one_heidelberg_file, fs_client, patient_id, remote_path
            ): (patient_id, remote_path)
            for patient_id, remote_path in tasks
        }
        for future in as_completed(futures):
            success, msg = future.result()
            if success:
                _safe_print(f"  [OK] {msg}")
            else:
                _safe_print(f"  [SKIP] {msg}")


def _process_one_other_file(
    fs_client: azurelake.FileSystemClient,
    patient_id: str,
    remote_path: str,
    file_prefix: str,
) -> tuple[bool, str]:
    """Download one file, upload with prefix. Returns (success, out_blob_path or error)."""
    original_name = os.path.basename(remote_path)
    out_name = file_prefix + original_name
    out_blob_path = f"{BASE_OUTPUT}/{patient_id}/{out_name}"

    fd = tempfile.NamedTemporaryFile(
        delete=False, suffix=os.path.splitext(original_name)[1] or ".bin"
    )
    try:
        download_path = fd.name
    finally:
        fd.close()

    try:
        file_client = fs_client.get_file_client(file_path=remote_path)
        with open(download_path, "wb") as f:
            f.write(file_client.download_file().readall())
    except Exception as e:
        with suppress(FileNotFoundError):
            os.unlink(download_path)
        return False, f"Download failed {remote_path}: {e}"

    try:
        out_client = fs_client.get_file_client(file_path=out_blob_path)
        with open(download_path, "rb") as f:
            out_client.upload_data(f.read(), overwrite=True)
        return True, out_blob_path
    except Exception as e:
        return False, f"Upload failed {out_blob_path}: {e}"
    finally:
        with suppress(FileNotFoundError):
            os.unlink(download_path)


def process_other_device(fs_client, subfolder_name, file_prefix):
    """
    Process one of topcon_maestro2, topcon_triton, zeiss_cirrus: for each patient,
    download each file, rename to <file_prefix><original_filename>, upload to
    enface-flipped-combined/{patient_id}/ (no image transform). Runs file tasks in parallel.
    """
    input_prefix = f"{BASE_ORIGINAL}/{subfolder_name}"
    patient_ids = list_patient_folders(fs_client, input_prefix)
    if not patient_ids:
        print(f"No patient folders found under {input_prefix}")
        return
    if PATIENT_ID_FILTER is not None:
        patient_ids = [p for p in patient_ids if p == PATIENT_ID_FILTER]
        if not patient_ids:
            print(f"  Patient ID {PATIENT_ID_FILTER} not found under {input_prefix}")
            return
        print(f"  (filter: only patient {PATIENT_ID_FILTER})")

    tasks = [
        (patient_id, remote_path)
        for patient_id in patient_ids
        for remote_path in list_files_in_folder(
            fs_client, f"{input_prefix}/{patient_id}"
        )
    ]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                _process_one_other_file,
                fs_client,
                patient_id,
                remote_path,
                file_prefix,
            ): (patient_id, remote_path)
            for patient_id, remote_path in tasks
        }
        for future in as_completed(futures):
            success, msg = future.result()
            if success:
                _safe_print(f"  [OK] {msg}")
            else:
                _safe_print(f"  [SKIP] {msg}")


def main():
    fs_client = get_file_system_client()

    # 1) Heidelberg Spectralis: flip + rotate, then upload with heidelberg_spectralis_ prefix
    print("--- Heidelberg Spectralis (flip + rotate 180°) ---")
    process_heidelberg_spectralis(fs_client)

    # 2) Other devices: rename only, upload to same enface-flipped-combined folder
    for subfolder, prefix in OTHER_DEVICES:
        print(f"--- {subfolder} (rename only) ---")
        process_other_device(fs_client, subfolder, prefix)

    print("Done.")


if __name__ == "__main__":
    main()
