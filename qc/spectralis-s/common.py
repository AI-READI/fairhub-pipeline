"""Shared helpers for Spectralis-S manifest QC scripts."""

import logging
import os
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

NOT_REPORTED = "Not Reported"

DEFAULT_ROOT = "/Volumes/Crucial X10/year4/merged"


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)


def load_manifest(manifest_path: Path) -> pd.DataFrame:
    """Load a manifest.tsv, keeping every column as a string."""
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")
    return pd.read_csv(manifest_path, sep="\t", dtype=str)


def resolve_path(root: Path, filepath: str) -> Path:
    """Manifest filepaths are written as root-relative paths starting with '/'."""
    return root / filepath.lstrip("/")


def manifest_relative_path(root: Path, full_path: Path) -> str:
    """Inverse of resolve_path: turn an absolute path back into a manifest-style path."""
    return "/" + str(full_path.relative_to(root)).replace(os.sep, "/")


def check_filepath_column(df: pd.DataFrame, column: str, root: Path):
    """Check every non-blank, non-'Not Reported' value in a filepath column exists on disk.

    Returns (missing_rows, checked_count) where missing_rows is a list of (index, value).
    """
    missing = []
    checked = 0
    for idx, value in df[column].items():
        if pd.isna(value) or value == NOT_REPORTED:
            continue
        checked += 1
        if not resolve_path(root, value).exists():
            missing.append((idx, value))
    return missing, checked


def find_dcm_files(folder: Path):
    """Recursively find real .dcm files, skipping macOS AppleDouble ('._*') siblings
    and other dotfiles that external/exFAT drives tend to accumulate.
    """
    found = []
    if not folder.exists():
        return found
    for dirpath, _, files in os.walk(folder):
        for f in files:
            if f.startswith("."):
                continue
            if f.lower().endswith(".dcm"):
                found.append(Path(dirpath) / f)
    return found


def check_nulls(df: pd.DataFrame, columns, logger, label) -> int:
    """Log and count null values in the given columns. Returns total null count."""
    total_nulls = 0
    for col in columns:
        if col not in df.columns:
            continue
        n = int(df[col].isna().sum())
        total_nulls += n
        if n:
            logger.error(f"{label}: {n} null values in '{col}'")
        else:
            logger.info(f"{label}: no null values in '{col}'")
    return total_nulls


def check_duplicates(df: pd.DataFrame, column: str, logger, label) -> int:
    """Log and count duplicate values in a column meant to be unique. Returns dup count."""
    if column not in df.columns:
        return 0
    dup_count = int(df[column].duplicated().sum())
    if dup_count:
        logger.error(f"{label}: {dup_count} duplicate values in '{column}'")
    else:
        logger.info(f"{label}: no duplicate values in '{column}'")
    return dup_count


def log_missing_files(logger, label, column, missing_rows, checked, limit=20):
    logger.info(f"{label}: checked {checked} paths in '{column}', {len(missing_rows)} missing")
    for idx, value in missing_rows[:limit]:
        logger.error(f"  Row {idx}: file not found for '{column}': {value}")
    if len(missing_rows) > limit:
        logger.error(f"  ... and {len(missing_rows) - limit} more missing '{column}' files")


def print_summary(logger, label, error_count):
    logger.info("=" * 60)
    if error_count:
        logger.error(f"{label}: QC FAILED — {error_count} issue(s) found")
    else:
        logger.info(f"{label}: QC PASSED — no issues found")
    logger.info("=" * 60)
