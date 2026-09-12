"""
QC script for the retinal_octa manifest in a merged Spectralis-S export.

retinal_octa is harder to validate than the other two modalities: each row fans
out across up to 12 filepath columns, some pointing at files inside
retinal_octa itself (flow_cube, enface, segmentation) and some pointing back
into retinal_photography / retinal_oct. Many of the enface / projection-removed
columns are legitimately "Not Reported" for a given row.

Checks:
- manifest.tsv exists and has the expected columns
- person_id / flow_cube_sop_instance_uid have no nulls, and
  flow_cube_sop_instance_uid has no duplicates
- every non-"Not Reported" filepath in every filepath column resolves to a
  real file on disk (own-folder columns and cross-folder reference columns)
- every .dcm file under retinal_octa/{flow_cube,enface,segmentation} is
  referenced by at least one of the own-folder filepath columns
  (orphan check, across all three subfolders at once since a file can only be
  distinguished by which column referenced it)

Usage:
    python qc_retinal_octa.py [--root PATH]
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

logger = get_logger("qc_retinal_octa")

LABEL = "retinal_octa"

REQUIRED_COLUMNS = [
    "person_id",
    "manufacturer",
    "manufacturers_model_name",
    "anatomic_region",
    "imaging",
    "laterality",
    "flow_cube_height",
    "flow_cube_width",
    "flow_cube_number_of_frames",
    "associated_segmentation_type",
    "associated_segmentation_number_of_frames",
    "flow_cube_sop_instance_uid",
    "flow_cube_file_path",
    "associated_retinal_photography_sop_instance_uid",
    "associated_retinal_photography_file_path",
    "associated_structural_oct_sop_instance_uid",
    "associated_structural_oct_file_path",
    "associated_segmentation_sop_instance_uid",
    "associated_segmentation_file_path",
    "variant_segmentation_sop_instance_uid",
    "variant_segmentation_file_path",
    "associated_enface_1_sop_instance_uid",
    "associated_enface_1_file_path",
    "associated_enface_2_sop_instance_uid",
    "associated_enface_2_file_path",
    "associated_enface_2_projection_removed_sop_instance_uid",
    "associated_enface_2_projection_removed_filepath",
    "associated_enface_3_sop_instance_uid",
    "associated_enface_3_file_path",
    "associated_enface_3_projection_removed_sop_instance_uid",
    "associated_enface_3_projection_removed_filepath",
    "associated_enface_4_sop_instance_uid",
    "associated_enface_4_file_path",
    "associated_enface_4_projection_removed_sop_instance_uid",
    "associated_enface_4_projection_removed_filepath",
]

# Filepath columns that point at files living inside retinal_octa itself.
OCTA_LOCAL_FILEPATH_COLUMNS = [
    "flow_cube_file_path",
    "associated_segmentation_file_path",
    "variant_segmentation_file_path",
    "associated_enface_1_file_path",
    "associated_enface_2_file_path",
    "associated_enface_2_projection_removed_filepath",
    "associated_enface_3_file_path",
    "associated_enface_3_projection_removed_filepath",
    "associated_enface_4_file_path",
    "associated_enface_4_projection_removed_filepath",
]

# Filepath columns that point back into other manifests' folders.
CROSS_REFERENCE_FILEPATH_COLUMNS = [
    "associated_retinal_photography_file_path",
    "associated_structural_oct_file_path",
]

OCTA_LOCAL_SUBFOLDERS = ["flow_cube", "enface", "segmentation"]


def main():
    parser = argparse.ArgumentParser(
        description="QC the retinal_octa manifest.tsv against files on disk"
    )
    parser.add_argument("--root", default=DEFAULT_ROOT, help="Path to the merged data root")
    parser.add_argument(
        "--orphan-limit", type=int, default=20, help="Max orphan file paths to print"
    )
    args = parser.parse_args()

    root = Path(args.root)
    data_folder = root / "retinal_octa"
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

    errors += check_nulls(df, ["person_id", "flow_cube_sop_instance_uid"], logger, LABEL)
    errors += check_duplicates(df, "flow_cube_sop_instance_uid", logger, LABEL)

    all_filepath_columns = OCTA_LOCAL_FILEPATH_COLUMNS + CROSS_REFERENCE_FILEPATH_COLUMNS
    referenced_local_paths = set()

    for column in all_filepath_columns:
        if column not in df.columns:
            logger.warning(f"{LABEL}: column '{column}' missing from manifest, skipping")
            continue

        missing_files, checked = check_filepath_column(df, column, root)
        log_missing_files(logger, LABEL, column, missing_files, checked, args.orphan_limit)
        errors += len(missing_files)

        if column in OCTA_LOCAL_FILEPATH_COLUMNS:
            referenced_local_paths.update(
                v for v in df[column].dropna() if v != NOT_REPORTED
            )

    # Orphan check across all three local subfolders at once: a file can only
    # be told apart from its filename/path, not which column referenced it.
    disk_files = []
    for subfolder in OCTA_LOCAL_SUBFOLDERS:
        disk_files.extend(find_dcm_files(data_folder / subfolder))
    disk_paths = {manifest_relative_path(root, p) for p in disk_files}

    orphans = sorted(disk_paths - referenced_local_paths)
    logger.info(
        f"{LABEL}: {len(disk_files)} .dcm files on disk under "
        f"{data_folder}/{{{','.join(OCTA_LOCAL_SUBFOLDERS)}}}, "
        f"{len(referenced_local_paths)} distinct paths referenced in manifest, "
        f"{len(orphans)} orphaned"
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
