"""Remove rows from manifest.tsv whose filepath is listed in the
topcon_maestro2_triton_paths_to_remove.txt removal list.

Writes the remaining rows to a new output manifest file.
"""

import csv

manifest_file = "manifest.tsv"
remove_paths_file = "topcon_maestro2_triton_paths_to_remove.txt"
output_file = "manifest_filtered.tsv"


def load_remove_paths(file_path):
    """Read the removal list and normalize each entry with a leading slash."""
    remove_paths = set()
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            path = line.strip()
            if not path:
                continue
            if not path.startswith("/"):
                path = f"/{path}"
            remove_paths.add(path)
    return remove_paths


def pipeline():
    """Filter manifest.tsv against the removal list and write the leftover rows."""
    print("Loading removal list...")
    remove_paths = load_remove_paths(remove_paths_file)
    print(f"Loaded {len(remove_paths)} path(s) to remove")

    print(f"Reading {manifest_file}...")
    with open(manifest_file, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f, delimiter="\t")
        fieldnames = reader.fieldnames
        rows = list(reader)

    print(f"Read {len(rows)} row(s) from manifest")

    kept_rows = []
    removed_count = 0
    for row in rows:
        if row["filepath"] in remove_paths:
            removed_count += 1
        else:
            kept_rows.append(row)

    print(f"Removed {removed_count} row(s), keeping {len(kept_rows)} row(s)")

    with open(output_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        writer.writerows(kept_rows)

    print(f"Wrote {len(kept_rows)} row(s) to {output_file}")


if __name__ == "__main__":
    pipeline()
