"""List every file path under AI-READI/adrc/Mini-Dataset/ in the stage-one container.

Writes each path with the "AI-READI/adrc/Mini-Dataset/" prefix stripped off.
"""

import azure.storage.filedatalake as azurelake  # type: ignore
import config

container_name = "stage-one"
root_folder = "AI-READI/adrc/Mini-Dataset/"
output_file = "dataset_paths.txt"


def pipeline():
    """List every file path under root_folder and write them to output_file."""
    print("Connecting to Azure Data Lake Storage...")
    file_system_client = azurelake.FileSystemClient.from_connection_string(
        config.AZURE_STORAGE_PRODUCTION_DANGEROUS_CONNECTION_STRING,
        file_system_name=container_name,
    )
    print("Connected.")

    print(f"Fetching paths under {root_folder}...")
    paths = file_system_client.get_paths(path=root_folder, recursive=True)

    file_paths = []
    for path in paths:
        if path.is_directory:
            continue

        relative_path = path.name[len(root_folder) :]
        file_paths.append(relative_path)

        if len(file_paths) % 100 == 0:
            print(f"Found {len(file_paths)} files so far...")

    print(f"Found {len(file_paths)} files total.")

    with open(output_file, "w", encoding="utf-8") as f:
        for relative_path in file_paths:
            f.write(relative_path + "\n")

    print(f"Wrote {len(file_paths)} paths to {output_file}")


if __name__ == "__main__":
    pipeline()
