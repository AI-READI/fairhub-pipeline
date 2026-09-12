"""
QC script for the retinal_photography manifest in a merged Spectralis-S export.

Checks:
- manifest.tsv exists and has the expected columns
- person_id / sop_instance_uid have no nulls, and sop_instance_uid has no duplicates
- every 'filepath' entry in the manifest resolves to a real file on disk
- every .dcm file under retinal_photography/ir is referenced by the manifest
  (orphan check, run in reverse of the usual filepath check)

Usage:
    python qc_retinal_photography.py [--root PATH]
"""

import argparse
import sys
from pathlib import Path

from common import (
    DEFAULT_ROOT,
    NOT_REPORTED,
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

logger = get_logger("qc_retinal_photography")

LABEL = "retinal_photography"

REQUIRED_COLUMNS = [
    "person_id",
    "manufacturer",
    "manufacturers_model_name",
    "laterality",
    "anatomic_region",
    "imaging",
    "height",
    "width",
    "color_channel_dimension",
    "sop_instance_uid",
    "filepath",
]


def main():
    parser = argparse.ArgumentParser(
        description="QC the retinal_photography manifest.tsv against files on disk"
    )
    parser.add_argument("--root", default=DEFAULT_ROOT, help="Path to the merged data root")
    parser.add_argument(
        "--orphan-limit", type=int, default=20, help="Max orphan file paths to print"
    )
    args = parser.parse_args()

    root = Path(args.root)
    data_folder = root / "retinal_photography"
    manifest_path = data_folder / "manifest.tsv"

    logger.info(f"Root: {root}")
    logger.info(f"Manifest: {manifest_path}")

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

    if "filepath" in df.columns:
        missing_files, checked = check_filepath_column(df, "filepath", root)
        log_missing_files(logger, LABEL, "filepath", missing_files, checked, args.orphan_limit)
        errors += len(missing_files)

        # Reverse check: files on disk that the manifest never references.
        ir_folder = data_folder / "ir"
        disk_files = find_dcm_files(ir_folder)
        disk_paths = {manifest_relative_path(root, p) for p in disk_files}
        referenced_paths = {v for v in df["filepath"].dropna() if v != NOT_REPORTED}

        orphans = sorted(disk_paths - referenced_paths)
        logger.info(
            f"{LABEL}: {len(disk_files)} .dcm files on disk under {ir_folder}, "
            f"{len(referenced_paths)} referenced in manifest, {len(orphans)} orphaned"
        )
        if orphans:
            errors += len(orphans)
            for path in orphans[: args.orphan_limit]:
                logger.error(f"  Orphan file (not in manifest): {path}")
            if len(orphans) > args.orphan_limit:
                logger.error(f"  ... and {len(orphans) - args.orphan_limit} more orphan files")

    print_summary(logger, LABEL, errors)
    sys.exit(0 if errors == 0 else 1)


if __name__ == "__main__":
    main()
