"""
Clean Garmin Monitor FIT files inside FIT-*.zip archives, in-place.

Rules:
- Only consider files under any 'Monitor' directory (case-insensitive).
- Only treat stems that match the copy-suffix pattern: ...0000[0-9]
  (e.g., M1I00000..M1I00009). Keep ...00000, delete ...00001-...00009.
- Do NOT touch other numeric endings like 0050 vs 0056.
- Normalize internal ZIP paths to use forward slashes ('/'), so unzipping
  yields 'GARMIN/...', without adding any extra top-level folder.

Python 3.8+
"""

import argparse
import sys
import zipfile
from pathlib import Path
from typing import Optional, List, Tuple
import re
import threading
import time
import uuid
import os
from contextlib import suppress

# ----------------------- thread safety -----------------------

# Global lock dictionary for file-level locking
_file_locks = {}
_lock_dict_lock = threading.Lock()


def get_file_lock(file_path: Path) -> threading.Lock:
    """Get or create a thread lock for a specific file path."""
    with _lock_dict_lock:
        if file_path not in _file_locks:
            _file_locks[file_path] = threading.Lock()
        return _file_locks[file_path]


def atomic_file_replace(source: Path, target: Path) -> bool:
    """
    Atomically replace target file with source file.
    Works on both Windows and Unix-like systems.
    """
    try:
        # On Windows, we need to handle the case where target might be in use
        if os.name == "nt" and target.exists():  # Windows
            target.unlink()
        # Rename source to target (atomic on Unix, works after unlink on Windows)
        source.rename(target)
        return True
    except OSError:
        return False


def create_unique_temp_file(base_path: Path) -> Path:
    """Create a unique temporary file path to avoid conflicts between threads."""
    timestamp = int(time.time() * 1000000)  # microseconds
    unique_id = str(uuid.uuid4())[:8]
    return base_path.with_suffix(f".zip.tmp.{timestamp}.{unique_id}")


# ----------------------- helpers -----------------------


def normalize_zip_path(name: str) -> str:
    """Convert backslashes to forward slashes for reliable path handling."""
    return name.replace("\\", "/")


def split_dir_file(norm_name: str) -> Tuple[str, str]:
    """Return (dirpath, filename) from a normalized path."""
    if "/" in norm_name:
        d, f = norm_name.rsplit("/", 1)
        return d, f
    return "", norm_name


def is_monitor_fit(norm_name: str) -> bool:
    """
    True if the normalized path points to a .FIT file under a 'Monitor' directory
    somewhere in the path (case-insensitive).
    """
    if not norm_name.lower().endswith(".fit"):
        return False
    parts = [p for p in norm_name.split("/") if p]
    return any(p.lower() == "monitor" for p in parts)


# Only match "copy suffix" patterns like ...0000[0-9]
COPY_SUFFIX_RE = re.compile(r"^(?P<base>.*0000)(?P<digit>\d)$")


def key_if_copy_suffix(filename: str) -> Optional[Tuple[str, str]]:
    """
    If filename (with extension) has stem matching ...0000[0-9],
    return (group_base, last_digit). Else None.
    """
    if "." not in filename:
        return None
    stem = filename.rsplit(".", 1)[0]
    m = COPY_SUFFIX_RE.match(stem)
    return (m.group("base"), m.group("digit")) if m else None


def plan_deletions(fit_files: List[Tuple[str, str]]) -> List[str]:
    """
    Given list of (dirpath, filename) for Monitor/*.FIT, compute full normalized
    paths to delete based on: keep ...00000, delete ...00001.. ...00009 in the same dir.
    Only act if the '0' variant exists in that group.
    """
    from collections import defaultdict

    groups = defaultdict(list)  # key: (dir, base) -> [filename]
    digits = defaultdict(set)  # key: (dir, base) -> set(last_digit)

    for d, fn in fit_files:
        k = key_if_copy_suffix(fn)
        if k is None:
            continue
        base, digit = k
        groups[(d, base)].append(fn)
        digits[(d, base)].add(digit)

    victims = []
    for (d, base), files in groups.items():
        if "0" in digits[(d, base)]:  # only if keeper exists
            for fn in files:
                stem = fn.rsplit(".", 1)[0]
                last_digit = stem[-1]
                if last_digit.isdigit() and last_digit != "0":
                    victims.append(f"{d}/{fn}" if d else fn)

    return sorted(set(victims))


# ----------------------- core -----------------------


def process_zip_in_place(zip_path: Path, logger=None) -> bool:
    """
    Process a single ZIP file to remove duplicate Monitor FIT files.
    Thread-safe implementation with file-level locking.

    Args:
        zip_path: Path to the ZIP file to process
        logger: Optional logger object for logging messages

    Returns:
        bool: True if processing was successful, False otherwise
    """
    # Get file-specific lock to prevent concurrent access to the same ZIP file
    file_lock = get_file_lock(zip_path)

    with file_lock:
        log_func = logger.info if logger else print
        error_func = logger.error if logger else print

        log_func(f"Processing {zip_path.name}")
        try:
            with zipfile.ZipFile(zip_path, "r") as zf:
                # Normalize all names and find Monitor FIT entries
                orig_names = zf.namelist()
                norm_names = [normalize_zip_path(n) for n in orig_names]

                monitor_fit_entries: List[Tuple[str, str]] = []
                for n in norm_names:
                    if is_monitor_fit(n):
                        d, f = split_dir_file(n)
                        monitor_fit_entries.append((d, f))

                if not monitor_fit_entries:
                    log_func("No Monitor/*.FIT entries found; skipping cleanup.")
                    return True

                victims = set(plan_deletions(monitor_fit_entries))
                if not victims:
                    log_func(
                        "No copy-suffix groups (…0000[0-9]) needing cleanup; nothing to delete."
                    )
                    return True

                # For logging: compute the expected keep path of a victim
                def kept_name_for(victim_norm: str) -> Optional[str]:
                    d, f = split_dir_file(victim_norm)
                    k = key_if_copy_suffix(f)
                    if not k:
                        return None
                    base, _ = k
                    keep_stem = f"{base}0"
                    ext = f.rsplit(".", 1)[-1]
                    keep = f"{d}/{keep_stem}.{ext}" if d else f"{keep_stem}.{ext}"
                    return keep if keep in norm_names else None

                # Create unique temporary file to avoid conflicts between threads
                tmp_zip = create_unique_temp_file(zip_path)

                # Ensure temp file doesn't exist (shouldn't happen with unique names, but safety first)
                if tmp_zip.exists():
                    tmp_zip.unlink()

                with zipfile.ZipFile(tmp_zip, "w") as zout:
                    for orig_name in orig_names:
                        norm_name = normalize_zip_path(orig_name)

                        if norm_name in victims:
                            k = kept_name_for(norm_name)
                            if k:
                                log_func(
                                    f"DELETE: {norm_name} -> KEEP: {k.split('/')[-1]}"
                                )
                            else:
                                log_func(
                                    f"DELETE: {norm_name} -> KEEP: (expected ...00000 not found)"
                                )
                            continue

                        # Copy entry but WRITE using normalized forward-slash path,
                        # so unzipping yields GARMIN/... as top-level.
                        data = zf.read(orig_name)
                        info = zf.getinfo(orig_name)
                        zi = zipfile.ZipInfo(
                            filename=norm_name, date_time=info.date_time
                        )
                        zi.compress_type = info.compress_type
                        zi.external_attr = info.external_attr
                        zi.create_system = info.create_system
                        zout.writestr(zi, data)

                # Atomically replace original with the new archive
                if atomic_file_replace(tmp_zip, zip_path):
                    log_func(f"Successfully repacked {zip_path.name}")
                    return True
                else:
                    error_func(
                        f"Failed to replace {zip_path.name} with processed version"
                    )
                    # Clean up temp file on failure
                    if tmp_zip.exists():
                        tmp_zip.unlink()
                    return False

        except zipfile.BadZipFile:
            error_func(f"Bad zip file: {zip_path}")
            return False
        except Exception as e:
            error_func(f"Failed to process {zip_path}: {e}")
            return False
        finally:
            # Clean up any temporary files that might have been created
            if "tmp_zip" in locals() and tmp_zip.exists():
                with suppress(OSError):
                    tmp_zip.unlink()


def deduplicate_garmin_zip(zip_path: str, logger=None) -> bool:
    """
    Clean Garmin Monitor FIT files in a single ZIP archive.

    This is the main API function for use in pipelines.

    Args:
        zip_path: Path to the ZIP file to process
        logger: Optional logger object for logging messages

    Returns:
        bool: True if processing was successful, False otherwise
    """
    return process_zip_in_place(Path(zip_path), logger)


def deduplicate_and_extract_garmin_zip(
    zip_path: str, extract_to: str = None, logger=None
) -> str:
    """
    Extract a Garmin ZIP file, deduplicate Monitor FIT files, and return the unzipped folder path.

    This function extracts the ZIP file to a temporary or specified directory,
    removes duplicate Monitor FIT files (keeping ...00000, deleting ...00001-...00009),
    and returns the path to the extracted folder.

    Args:
        zip_path: Path to the ZIP file to process
        extract_to: Optional directory to extract to. If None, creates a temporary directory.
        logger: Optional logger object for logging messages

    Returns:
        str: Path to the extracted and deduplicated folder

    Raises:
        Exception: If extraction or deduplication fails
    """
    import tempfile
    import shutil

    def get_log_funcs(logger):
        """Get logging functions from logger or use print as fallback."""
        return (logger.info if logger else print, logger.error if logger else print)

    log_func, error_func = get_log_funcs(logger)

    zip_path_obj = Path(zip_path)
    if not zip_path_obj.exists():
        raise FileNotFoundError(f"ZIP file not found: {zip_path}")

    # Determine extraction directory
    if (
        extract_dir := (
            Path(extract_to)
            if extract_to is not None
            else Path(tempfile.mkdtemp(prefix="garmin_dedup_"))
        )
    ) and extract_to is not None:
        extract_dir.mkdir(parents=True, exist_ok=True)
        cleanup_temp = False
    else:
        cleanup_temp = True

    try:
        log_func(f"Extracting {zip_path_obj.name} to {extract_dir}")

        # Extract the ZIP file
        with zipfile.ZipFile(zip_path_obj, "r") as zf:
            zf.extractall(extract_dir)

        log_func(f"Successfully extracted to {extract_dir}")

        # Find and deduplicate Monitor FIT files in the extracted directory
        monitor_fit_files = []
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                full_path = os.path.join(root, file)
                rel_path = os.path.relpath(full_path, extract_dir)
                norm_path = normalize_zip_path(rel_path)

                if is_monitor_fit(norm_path):
                    d, f = split_dir_file(norm_path)
                    monitor_fit_files.append((d, f))

        if monitor_fit_files:
            log_func(f"Found {len(monitor_fit_files)} Monitor FIT files to process")

            # Plan deletions
            victims = set(plan_deletions(monitor_fit_files))

            if victims:
                log_func(f"Removing {len(victims)} duplicate files")

                # Delete duplicate files
                for victim_path in victims:
                    full_victim_path = extract_dir / victim_path
                    if full_victim_path.exists():
                        # Log what we're keeping
                        d, f = split_dir_file(victim_path)
                        k = key_if_copy_suffix(f)
                        if k:
                            base, _ = k
                            keep_stem = f"{base}0"
                            ext = f.rsplit(".", 1)[-1]
                            keep_file = (
                                f"{d}/{keep_stem}.{ext}" if d else f"{keep_stem}.{ext}"
                            )
                            log_func(f"DELETE: {victim_path} -> KEEP: {keep_file}")

                        full_victim_path.unlink()
                        log_func(f"Deleted: {victim_path}")
                    else:
                        log_func(f"Warning: Expected file not found: {victim_path}")
            else:
                log_func("No duplicate files found to remove")
        else:
            log_func("No Monitor FIT files found")

        log_func(f"Deduplication complete. Extracted folder: {extract_dir}")
        return str(extract_dir)

    except Exception as e:
        error_func(f"Error during extraction and deduplication: {e}")
        # Clean up temporary directory if we created it
        if cleanup_temp and extract_dir.exists():
            shutil.rmtree(extract_dir, ignore_errors=True)
        raise


def main():
    """
    Main function for command-line usage.
    Note: This function processes files sequentially, but the underlying
    process_zip_in_place function is thread-safe for concurrent access.
    """
    parser = argparse.ArgumentParser(
        description="Clean Garmin Monitor FIT files in ZIPs (only collapse …0000[0-9] copies; keep …00000)."
    )
    parser.add_argument(
        "root_dir",
        type=Path,
        help="Directory containing FIT-*.zip archives (e.g., FitnessTracker/)",
    )
    args = parser.parse_args()

    root_dir: Path = args.root_dir
    if not root_dir.exists() or not root_dir.is_dir():
        print(f"[ERROR] Provided path is not a directory: {root_dir}")
        sys.exit(1)

    zip_files = sorted(
        [
            p
            for p in root_dir.iterdir()
            if p.is_file() and p.suffix.lower() == ".zip" and p.name.startswith("FIT-")
        ]
    )

    if not zip_files:
        print(f"[INFO] No FIT-*.zip files found in {root_dir}")
        sys.exit(0)

    for zp in zip_files:
        process_zip_in_place(zp)

    print("\n[DONE] All archives processed.")


if __name__ == "__main__":
    main()
