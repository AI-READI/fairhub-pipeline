"""
QC script for the retinal_oct manifest in a merged Spectralis-S export.

Checks:
- manifest.tsv exists and has the expected columns
- person_id / sop_instance_uid have no nulls, and sop_instance_uid has no duplicates
- every 'filepath' entry resolves to a real file under retinal_oct/structural_oct
- every 'reference_filepath' entry (pointing back into retinal_photography) resolves
  to a real file
- every .dcm file under retinal_oct/structural_oct is referenced by the manifest
  (orphan check)

Usage:
    python qc_retinal_oct.py [--root PATH]
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


def main():
    parser = argparse.ArgumentParser(
        description="QC the retinal_oct manifest.tsv against files on disk"
    )
    parser.add_argument("--root", default=DEFAULT_ROOT, help="Path to the merged data root")
    parser.add_argument(
        "--orphan-limit", type=int, default=20, help="Max orphan file paths to print"
    )
    args = parser.parse_args()

    root = Path(args.root)
    data_folder = root / "retinal_oct"
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

    # Own-folder filepath: structural_oct DICOMs.
    if "filepath" in df.columns:
        missing_files, checked = check_filepath_column(df, "filepath", root)
        log_missing_files(logger, LABEL, "filepath", missing_files, checked, args.orphan_limit)
        errors += len(missing_files)

        structural_oct_folder = data_folder / "structural_oct"
        disk_files = find_dcm_files(structural_oct_folder)
        disk_paths = {manifest_relative_path(root, p) for p in disk_files}
        referenced_paths = {v for v in df["filepath"].dropna() if v != NOT_REPORTED}

        orphans = sorted(disk_paths - referenced_paths)
        logger.info(
            f"{LABEL}: {len(disk_files)} .dcm files on disk under {structural_oct_folder}, "
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
        missing_refs, checked = check_filepath_column(df, "reference_filepath", root)
        log_missing_files(
            logger, LABEL, "reference_filepath", missing_refs, checked, args.orphan_limit
        )
        errors += len(missing_refs)

    print_summary(logger, LABEL, errors)
    sys.exit(0 if errors == 0 else 1)


if __name__ == "__main__":
    main()
