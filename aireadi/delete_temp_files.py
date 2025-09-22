import os
import time
import stat
import contextlib
from tqdm import tqdm

# Folder to clean
TARGET_DIR = r"C:\Users\b2aiUsr\AppData\Local\Temp\2"
RETENTION_HOURS = 2  # keep files modified within the last N hours


def main():
    cutoff = time.time() - (RETENTION_HOURS * 60 * 60)
    removed = skipped = errors = 0

    if not os.path.isdir(TARGET_DIR):
        raise SystemExit(f"Directory does not exist: {TARGET_DIR}")

    # Collect files (no subfolders) so tqdm knows the total
    with os.scandir(TARGET_DIR) as it:
        files = [entry.path for entry in it if entry.is_file(follow_symlinks=False)]

    pbar = tqdm(files, desc="Deleting old temp files", unit="file", dynamic_ncols=True)
    for path in pbar:
        try:
            st = os.stat(path, follow_symlinks=False)
            mtime = st.st_mtime  # Windows: modified time
            if mtime < cutoff:
                # Ensure file isn't read-only before deleting
                with contextlib.suppress(Exception):
                    os.chmod(path, stat.S_IWRITE)
                os.remove(path)
                removed += 1
            else:
                skipped += 1
        except Exception as e:
            errors += 1
            tqdm.write(f"Error processing {path}: {e}")

        # live counters in the bar
        pbar.set_postfix(removed=removed, kept=skipped, errors=errors)

    pbar.close()
    print(
        f"\nDone. Removed {removed} file(s), kept {skipped} recent file(s), {errors} error(s)."
    )


if __name__ == "__main__":
    main()
