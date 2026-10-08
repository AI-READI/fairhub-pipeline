"""Compare DICOM tags between two converter output folders, file by file.

Files are matched by their path relative to each folder. For every matched pair, all
data elements (including nested sequences) are compared; pixel data is compared by hash
instead of being printed. The report is written to report_file.
"""

import hashlib
import os

import pydicom

# Large binary elements: compare by hash rather than printing the value
BINARY_VRS = {"OB", "OW", "OF", "OD", "OL", "OV", "UN"}

compare_root = os.path.abspath("D:\\year3+pre\\spectralis-s-dicomdir-compare")
folder_a = os.path.join(compare_root, "old_run1")
folder_b = os.path.join(compare_root, "old_run2")
report_file = os.path.join(compare_root, "old_run1_vs_old_run2.txt")


def collect_files(folder):
    """Return {relative path: absolute path} for every file under folder."""
    files = {}
    for root, _dirs, names in os.walk(folder):
        for name in names:
            if name.endswith(".txt"):  # converter logs, not DICOM
                continue
            path = os.path.join(root, name)
            files[os.path.relpath(path, folder)] = path
    return files


def flatten(dataset, prefix=""):
    """Flatten a dataset into {tag path: (keyword, printable value)}, recursing into sequences."""
    result = {}
    for elem in dataset:
        key = f"{prefix}{elem.tag}"
        if elem.VR == "SQ":
            result[key] = (elem.keyword, f"<sequence of {len(elem.value)} items>")
            for i, item in enumerate(elem.value):
                result.update(flatten(item, f"{key}[{i}]."))
        elif elem.VR in BINARY_VRS:
            raw = elem.value if isinstance(elem.value, bytes) else bytes(str(elem.value), "utf-8")
            result[key] = (elem.keyword, f"<{len(raw)} bytes, sha256 {hashlib.sha256(raw).hexdigest()[:16]}>")
        else:
            result[key] = (elem.keyword, str(elem.value))
    return result


def read_flat(path):
    """Read a DICOM file (header and dataset) and flatten it."""
    ds = pydicom.dcmread(path, force=True)
    flat = flatten(ds.file_meta, "meta:") if getattr(ds, "file_meta", None) else {}
    flat.update(flatten(ds))
    return flat


def compare_files(path_a, path_b):
    """Return a list of human-readable differences between two DICOM files."""
    a, b = read_flat(path_a), read_flat(path_b)
    diffs = []
    for key in sorted(set(a) | set(b)):
        if key not in a:
            diffs.append(f"  only in B: {key} {b[key][0]} = {b[key][1]}")
        elif key not in b:
            diffs.append(f"  only in A: {key} {a[key][0]} = {a[key][1]}")
        elif a[key][1] != b[key][1]:
            diffs.append(f"  {key} {a[key][0]}:\n    A: {a[key][1]}\n    B: {b[key][1]}")
    return diffs


def main():
    files_a, files_b = collect_files(folder_a), collect_files(folder_b)
    lines = [
        f"A: {folder_a} ({len(files_a)} files)",
        f"B: {folder_b} ({len(files_b)} files)",
        "",
    ]

    for rel in sorted(set(files_a) - set(files_b)):
        lines.append(f"Only in A: {rel}")
    for rel in sorted(set(files_b) - set(files_a)):
        lines.append(f"Only in B: {rel}")

    identical = 0
    differing = 0
    for rel in sorted(set(files_a) & set(files_b)):
        try:
            diffs = compare_files(files_a[rel], files_b[rel])
        except Exception as e:
            lines.append(f"{rel}: could not read as DICOM ({e})")
            continue
        if diffs:
            differing += 1
            lines.append(f"{rel}: {len(diffs)} differing elements")
            lines.extend(diffs)
        else:
            identical += 1

    lines.append("")
    lines.append(f"Matched files with identical tags: {identical}")
    lines.append(f"Matched files with differences:     {differing}")

    with open(report_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines[-2:]))
    print(f"Report written to {report_file}")


if __name__ == "__main__":
    main()
