"""
QC script for the retinal_octa manifest in a merged export.

retinal_octa is harder to validate than the other two modalities: each row fans
out across up to 12 filepath columns, some pointing at files inside
retinal_octa itself (flow_cube, enface, segmentation) and some pointing back
into retinal_photography / retinal_oct. Many of the enface / projection-removed
columns are legitimately "Not Reported" for a given row.

Checks:
- manifest.tsv exists and has the expected columns
- person_id / flow_cube_sop_instance_uid have no nulls, and
  flow_cube_sop_instance_uid has no duplicates
- placeholder values ("Not Reported" / "Not Provided") are reported per column,
  and are an error in columns that must always be filled
- numeric and categorical columns hold sensible values
- every non-placeholder filepath in every filepath column resolves to a
  real file on disk (own-folder columns and cross-folder reference columns)
- every cross-reference UID (photography / structural OCT) exists in the
  matching source manifest (forward check); source UIDs never referenced by
  octa are reported for information only (reverse check)
- every .dcm file under retinal_octa/{flow_cube,enface,segmentation} is
  referenced by at least one of the own-folder filepath columns
  (orphan check, across all three subfolders at once since a file can only be
  distinguished by which column referenced it)

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

# The pipeline writes two placeholder strings, in inconsistent casing:
# "Not Reported" / "Not reported" for values that do not apply, and
# "Not Provided" when a lookup failed. All mean "no file here".
PLACEHOLDER_VALUES = {"not reported", "not provided"}

# Columns that must hold a real value on every row.
MUST_BE_FILLED_COLUMNS = [
    "flow_cube_sop_instance_uid",
    "flow_cube_file_path",
    "associated_segmentation_sop_instance_uid",
    "associated_segmentation_file_path",
    "associated_enface_1_file_path",
    "associated_enface_2_file_path",
    "associated_enface_3_file_path",
    "associated_enface_4_file_path",
]

POSITIVE_INT_COLUMNS = ["flow_cube_height", "flow_cube_width", "flow_cube_number_of_frames"]

VALID_LATERALITY = {"L", "R"}

# Forward cross-reference checks: (uid column here, source manifest, uid column there)
CROSS_REFERENCE_UID_CHECKS = [
    (
        "associated_retinal_photography_sop_instance_uid",
        "retinal_photography",
        "sop_instance_uid",
    ),
    (
        "associated_structural_oct_sop_instance_uid",
        "retinal_oct",
        "sop_instance_uid",
    ),
]


def is_placeholder(value):
    return isinstance(value, str) and value.strip().lower() in PLACEHOLDER_VALUES


def check_placeholders(df, logger, label):
    """Report placeholder counts; error on columns that must always be filled."""
    errors = 0
    for column in MUST_BE_FILLED_COLUMNS:
        if column not in df.columns:
            continue
        count = int(df[column].map(is_placeholder).sum())
        if count:
            errors += count
            logger.error(f"{label}: '{column}' has {count} placeholder values (must always be filled)")
        else:
            logger.info(f"{label}: '{column}' has no placeholder values")
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


def check_cross_reference_uids(df, root, logger, label):
    """Forward: every referenced source UID must exist in its source manifest.
    Reverse: source UIDs never referenced by octa are reported for info only."""
    errors = 0
    for uid_column, source_name, source_uid_column in CROSS_REFERENCE_UID_CHECKS:
        if uid_column not in df.columns:
            continue

        source_manifest = root / source_name / "manifest.tsv"
        if not source_manifest.exists():
            logger.warning(f"{label}: {source_manifest} not found, skipping UID cross-check for '{uid_column}'")
            continue

        source_df = load_manifest(source_manifest)
        if source_uid_column not in source_df.columns:
            logger.warning(f"{label}: {source_name} manifest has no '{source_uid_column}', skipping")
            continue

        source_uids = set(source_df[source_uid_column].dropna().astype(str))
        referenced = df[uid_column].dropna().astype(str)
        referenced = set(referenced[~referenced.map(is_placeholder)])

        # Forward check (error): referenced UIDs that don't exist in the source.
        unresolved = sorted(referenced - source_uids)
        logger.info(
            f"{label}: '{uid_column}' -> {source_name}: {len(referenced)} referenced, "
            f"{len(source_uids)} in source, {len(unresolved)} unresolved"
        )
        if unresolved:
            errors += len(unresolved)
            for uid in unresolved[:20]:
                logger.error(f"  {uid_column}: UID not in {source_name} manifest: {uid}")
            if len(unresolved) > 20:
                logger.error(f"  ... and {len(unresolved) - 20} more unresolved")

        # Reverse check (info only): source UIDs never referenced by octa.
        never_referenced = source_uids - referenced
        logger.info(
            f"{label}: {len(never_referenced)} {source_name} UIDs are never referenced by octa (info only)"
        )
    return errors


def main():
    parser = argparse.ArgumentParser(
        description="QC the retinal_octa manifest.tsv against files on disk"
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
        help="Path to write the QC report to (default: <root>/retinal_octa/qc_report.txt)",
    )
    args = parser.parse_args()

    root = Path(args.root)
    data_folder = root / "retinal_octa"
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

    errors += check_nulls(df, ["person_id", "flow_cube_sop_instance_uid"], logger, LABEL)
    errors += check_duplicates(df, "flow_cube_sop_instance_uid", logger, LABEL)
    errors += check_placeholders(df, logger, LABEL)
    errors += check_values(df, logger, LABEL)
    errors += check_cross_reference_uids(df, root, logger, LABEL)

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
                "/" + v.replace("\\", "/").lstrip("/")
                for v in df[column].dropna()
                if not is_placeholder(v)
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