"""
QC script for the retinal_oct manifest in a merged export.

Checks:
- manifest.tsv exists and has the expected columns
- person_id / sop_instance_uid have no nulls, and sop_instance_uid has no duplicates
- placeholder values ("Not Reported" / "Not Provided") are reported per column, and
  are an error in columns that must always be filled
- numeric and categorical columns hold sensible values
- every 'filepath' entry resolves to a real file under retinal_oct
- every 'reference_filepath' entry (pointing back into retinal_photography) resolves
  to a real file
- every 'reference_instance_uid' exists in the retinal_photography manifest
- every .dcm file anywhere under retinal_oct is referenced by the manifest
  (orphan check)

Usage:
    python qc_retinal_oct.py [--root PATH]
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

from common import (
    DEFAULT_ROOT,
    NOT_REPORTED,
    add_file_handler,
    check_duplicates,
    check_filepath_column,
    check_nulls,
    find_dcm_files,
    get_logger,
    load_manifest,
    log_missing_files,
    manifest_relative_path,
    print_summary,
)

logger = get_logger("qc_retinal_oct")

LABEL = "retinal_oct"

REQUIRED_COLUMNS = [
    "person_id",
    "manufacturer",
    "manufacturers_model_name",
    "anatomic_region",
    "imaging",
    "laterality",
    "height",
    "width",
    "number_of_frames",
    "pixel_spacing",
    "slice_thickness",
    "sop_instance_uid",
    "filepath",
    "reference_instance_uid",
    "reference_filepath",
]

# The pipeline writes two different placeholder strings: "Not Reported" when a
# value genuinely does not apply, and "Not Provided" when a lookup failed.
# Both mean "there is no file here", so neither should be treated as a path.
NOT_PROVIDED = "Not Provided"
PLACEHOLDERS = {NOT_REPORTED, NOT_PROVIDED}

# Columns that must always hold a real value: a placeholder here means the
# pipeline failed to resolve something it was supposed to resolve.
MUST_BE_FILLED_COLUMNS = [
    "sop_instance_uid",
    "filepath",
    "reference_instance_uid",
    "reference_filepath",
]

# Columns where a placeholder is expected for some rows (e.g. Spectralis ONH
# does not report pixel spacing or slice thickness).
MAY_BE_PLACEHOLDER_COLUMNS = [
    "pixel_spacing",
    "slice_thickness",
]

POSITIVE_INT_COLUMNS = ["height", "width", "number_of_frames"]

VALID_LATERALITY = {"L", "R"}


def is_placeholder(value):
    """True if the value is one of the pipeline's 'no data here' strings."""
    return isinstance(value, str) and value.strip() in PLACEHOLDERS


def mask_placeholders(df, columns):
    """
    Return a copy of df where every placeholder string in `columns` is
    normalised to NOT_REPORTED, so the shared filepath helper skips those rows
    instead of trying to stat a file called "Not Provided".
    """
    masked = df.copy()
    for column in columns:
        if column in masked.columns:
            masked[column] = masked[column].map(
                lambda v: NOT_REPORTED if is_placeholder(v) else v
            )
    return masked


def check_placeholders(df, logger, label):
    """Report placeholder counts per column; error on columns that must be filled."""
    errors = 0
    for column in MUST_BE_FILLED_COLUMNS:
        if column not in df.columns:
            continue
        count = df[column].map(is_placeholder).sum()
        if count:
            errors += int(count)
            logger.error(f"{label}: '{column}' has {count} placeholder values (must always be filled)")
        else:
            logger.info(f"{label}: '{column}' has no placeholder values")

    for column in MAY_BE_PLACEHOLDER_COLUMNS:
        if column not in df.columns:
            continue
        count = df[column].map(is_placeholder).sum()
        if count:
            logger.info(f"{label}: '{column}' has {count} placeholder values (allowed)")
    return errors


def check_values(df, logger, label):
    """Sanity-check categorical and numeric columns."""
    errors = 0

    if "laterality" in df.columns:
        bad = df.loc[~df["laterality"].astype(str).str.upper().isin(VALID_LATERALITY), "laterality"]
        if len(bad):
            errors += len(bad)
            logger.error(
                f"{label}: 'laterality' has {len(bad)} unexpected values: "
                f"{sorted(set(bad.astype(str)))[:10]}"
            )
        else:
            logger.info(f"{label}: 'laterality' values all L/R")

    for column in POSITIVE_INT_COLUMNS:
        if column not in df.columns:
            continue
        numeric = pd.to_numeric(df[column], errors="coerce")
        bad_count = int((numeric.isna() | (numeric <= 0)).sum())
        if bad_count:
            errors += bad_count
            logger.error(f"{label}: '{column}' has {bad_count} non-positive or non-numeric values")
        else:
            logger.info(f"{label}: '{column}' values all positive")

    for column in ["manufacturer", "manufacturers_model_name", "anatomic_region", "imaging"]:
        if column in df.columns:
            logger.info(f"{label}: '{column}' values: {sorted(set(df[column].dropna().astype(str)))}")

    return errors


def check_reference_uids(df, root, logger, label):
    """Every reference_instance_uid must exist in the retinal_photography manifest."""
    photography_manifest = root / "retinal_photography" / "manifest.tsv"
    if not photography_manifest.exists():
        logger.warning(
            f"{label}: {photography_manifest} not found, skipping reference UID cross-check"
        )
        return 0

    photography_df = load_manifest(photography_manifest)
    if "sop_instance_uid" not in photography_df.columns:
        logger.warning(
            f"{label}: photography manifest has no 'sop_instance_uid' column, skipping cross-check"
        )
        return 0

    known_uids = set(photography_df["sop_instance_uid"].dropna().astype(str))
    referenced = df["reference_instance_uid"].dropna().astype(str)
    referenced = referenced[~referenced.map(is_placeholder)]

    unknown = sorted(set(referenced) - known_uids)
    logger.info(
        f"{label}: {len(set(referenced))} distinct reference UIDs, "
        f"{len(known_uids)} UIDs in photography manifest, {len(unknown)} unresolved"
    )
    if unknown:
        for uid in unknown[:20]:
            logger.error(f"  Reference UID not in photography manifest: {uid}")
        if len(unknown) > 20:
            logger.error(f"  ... and {len(unknown) - 20} more unresolved reference UIDs")
    return len(unknown)


def main():
    parser = argparse.ArgumentParser(
        description="QC the retinal_oct manifest.tsv against files on disk"
    )
    parser.add_argument(
        "--root",
        default=r"F:\TRITON_TRIO\final",
        help="Path to the merged data root",
    )
    parser.add_argument(
        "--orphan-limit", type=int, default=20, help="Max orphan file paths to print"
    )
    parser.add_argument(
        "--report-file",
        default=None,
        help="Path to write the QC report to (default: <root>/retinal_oct/qc_report.txt)",
    )
    args = parser.parse_args()




    root = Path(args.root)
    data_folder = root / "retinal_oct"
    manifest_path = data_folder / "manifest.tsv"
    report_path = Path(args.report_file) if args.report_file else data_folder / "qc_report.txt"
    add_file_handler(logger, report_path)

    logger.info(f"Root: {root}")
    logger.info(f"Manifest: {manifest_path}")
    logger.info(f"Report file: {report_path}")

    errors = 0

    try:
        df = load_manifest(manifest_path)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)

    logger.info(f"Loaded {len(df)} rows from manifest")

    missing_cols = sorted(set(REQUIRED_COLUMNS) - set(df.columns))
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        errors += len(missing_cols)
    else:
        logger.info("All required columns present")

    errors += check_nulls(df, ["person_id", "sop_instance_uid"], logger, LABEL)
    errors += check_duplicates(df, "sop_instance_uid", logger, LABEL)
    errors += check_placeholders(df, logger, LABEL)
    errors += check_values(df, logger, LABEL)

    # Placeholders must not be treated as paths by the filepath checks below.
    df_paths = mask_placeholders(df, ["filepath", "reference_filepath"])

    # Own-folder filepath: every OCT DICOM under retinal_oct.
    if "filepath" in df.columns:
        missing_files, checked = check_filepath_column(df_paths, "filepath", root)
        log_missing_files(logger, LABEL, "filepath", missing_files, checked, args.orphan_limit)
        errors += len(missing_files)

        # Scan the whole modality folder, not just one subfolder, so files in
        # any subfolder are covered by the orphan check.
        disk_files = find_dcm_files(data_folder)
        disk_paths = {manifest_relative_path(root, p) for p in disk_files}

        referenced_paths = {
            "/" + v.replace("\\", "/").lstrip("/")
            for v in df["filepath"].dropna()
            if not is_placeholder(v)
        }

        orphans = sorted(disk_paths - referenced_paths)
        logger.info(
            f"{LABEL}: {len(disk_files)} .dcm files on disk under {data_folder}, "
            f"{len(referenced_paths)} referenced in manifest, {len(orphans)} orphaned"
        )
        if orphans:
            errors += len(orphans)
            for path in orphans[: args.orphan_limit]:
                logger.error(f"  Orphan file (not in manifest): {path}")
            if len(orphans) > args.orphan_limit:
                logger.error(f"  ... and {len(orphans) - args.orphan_limit} more orphan files")

    # Cross-folder reference: each OCT points back at a retinal_photography IR frame.
    if "reference_filepath" in df.columns:
        missing_refs, checked = check_filepath_column(df_paths, "reference_filepath", root)
        log_missing_files(
            logger, LABEL, "reference_filepath", missing_refs, checked, args.orphan_limit
        )
        errors += len(missing_refs)

    if "reference_instance_uid" in df.columns:
        errors += check_reference_uids(df, root, logger, LABEL)

    print_summary(logger, LABEL, errors)
    sys.exit(0 if errors == 0 else 1)


if __name__ == "__main__":
    main()