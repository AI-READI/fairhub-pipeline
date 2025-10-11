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
