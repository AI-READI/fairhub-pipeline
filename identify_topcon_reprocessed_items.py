"""Identify manifest entries produced by the Triton/Maestro2 reprocessing script.

Context: aireadi_retinal_imaging/main/process_topcon.py was used to reprocess all
Triton and Maestro2 (Topcon) data. Before loading the new output, the previously
processed data for these two devices needs to be removed. This script scans a
manifest of filename/paths (default: dataset_paths.txt at repo root, one relative
path per line, as produced by dev/generate_folder_paths.py) and singles out every
entry that belongs to Triton or Maestro2, using identifying strings taken directly
from process_topcon.py:

  - Device folder names embedded in the final dataset path structure:
        topcon_maestro2
        topcon_triton
    These appear under every modality the two devices produce output for
    (retinal_oct/structural_oct, retinal_octa/{enface,flow_cube,segmentation},
    retinal_photography/{cfp,ir}), so matching on the device folder name catches
    all of it regardless of modality.

  - The six imaging protocols process_topcon.py drives through
    `Maestro2_Triton.convert`, used here only to break the matches down for
    reporting (a file must already match a device folder to be counted):
        maestro2_3d_macula_oct, maestro2_3d_wide_oct, maestro2_macula_6x6_octa
        triton_3d_radial_oct, triton_macula_6x6_octa, triton_macula_12x12_octa

Usage:
    python identify_topcon_reprocessed_items.py [manifest_path] [-o output_path]
"""

import argparse
import os
import re
from collections import Counter

DEVICE_FOLDER_PATTERNS = {
    "maestro2": "topcon_maestro2",
    "triton": "topcon_triton",
}

PROTOCOLS_BY_DEVICE = {
    "maestro2": [
        "maestro2_3d_macula_oct",
        "maestro2_3d_wide_oct",
        "maestro2_macula_6x6_octa",
    ],
    "triton": [
        "triton_3d_radial_oct",
        "triton_macula_6x6_octa",
        "triton_macula_12x12_octa",
    ],
}

# <modality>/<submodality>/topcon_<device>/<participant_id>/<filename>
PARTICIPANT_ID_RE = re.compile(r"topcon_(?:maestro2|triton)/([^/]+)/")


def classify(path: str):
    """Return (device, protocol_or_None) if path belongs to Triton/Maestro2, else None."""
    for device, folder in DEVICE_FOLDER_PATTERNS.items():
        if folder in path:
            protocol = next(
                (p for p in PROTOCOLS_BY_DEVICE[device] if p in path), None
            )
            return device, protocol
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Identify Triton/Maestro2 manifest entries to remove before reloading reprocessed data."
    )
    parser.add_argument(
        "manifest_path",
        nargs="?",
        default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "dataset_paths.txt"),
        help="Path to the manifest file (one relative path per line). Defaults to dataset_paths.txt in this repo.",
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output_path",
        default="topcon_maestro2_triton_paths_to_remove.txt",
        help="Where to write the matched paths (default: topcon_maestro2_triton_paths_to_remove.txt).",
    )
    args = parser.parse_args()

    with open(args.manifest_path, "r", encoding="utf-8") as f:
        all_paths = [line.strip() for line in f if line.strip()]

    matches = []
    device_counts = Counter()
    protocol_counts = Counter()
    modality_counts = Counter()
    participants = {"maestro2": set(), "triton": set()}
    unclassified_protocol = 0

    for path in all_paths:
        result = classify(path)
        if result is None:
            continue

        device, protocol = result
        matches.append(path)
        device_counts[device] += 1

        modality = "/".join(path.split("/")[:2])
        modality_counts[(device, modality)] += 1

        if protocol:
            protocol_counts[protocol] += 1
        else:
            unclassified_protocol += 1

        pid_match = PARTICIPANT_ID_RE.search(path)
        if pid_match:
            participants[device].add(pid_match.group(1))

    with open(args.output_path, "w", encoding="utf-8") as f:
        for path in matches:
            f.write(path + "\n")

    total = len(all_paths)
    print(f"Manifest: {args.manifest_path} ({total} total entries)")
    print(f"Matched Triton/Maestro2 entries: {len(matches)}")
    print(f"Wrote matched paths to: {args.output_path}")
    print()

    for device in ("maestro2", "triton"):
        print(f"{device}: {device_counts[device]} files, {len(participants[device])} participants")
        for modality, count in sorted(
            (m, c) for (d, m), c in modality_counts.items() if d == device
        ):
            print(f"    {modality}: {count}")
        for protocol in PROTOCOLS_BY_DEVICE[device]:
            print(f"    protocol {protocol}: {protocol_counts[protocol]}")
        print()

    if unclassified_protocol:
        print(
            f"Note: {unclassified_protocol} matched files did not contain a "
            "recognized protocol substring (still included in output; likely "
            "non-DICOM sidecar/metadata files under the device folder)."
        )


if __name__ == "__main__":
    main()
