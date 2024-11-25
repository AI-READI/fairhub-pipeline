import contextlib
import os
import tempfile
import config
import shutil
import azure.storage.filedatalake as azurelake
from azure.core.exceptions import ResourceNotFoundError
import pathlib
import json
import glob


class FileMapProcessor:
    """Class for handling file processing"""

    def __init__(self, dependency_folder: str, ignore_file=None):

        self.file_map = []
        # where actually ignored files are stored in the array
        self.ignore_files = []
        self.dependency_folder = dependency_folder

        # Create a temporary folder on the local machine
        self.meta_temp_folder_path = tempfile.mkdtemp()

        file_map_download_path = os.path.join(
            self.meta_temp_folder_path, "file_map.json"
        )

        self.file_system_client = azurelake.FileSystemClient.from_connection_string(
            config.AZURE_STORAGE_CONNECTION_STRING,
            file_system_name="stage-1-container",
        )

        meta_file_client = self.file_system_client.get_file_client(
            file_path=f"{dependency_folder}/file_map.json"
        )

        if ignore_file:
            # ignore File name coming from the ignore file path
            ignored_file_name = ignore_file.split("/")[-1]

            ignore_file_download_path = os.path.join(
                self.meta_temp_folder_path, ignored_file_name
            )
            ignore_meta_file_client = self.file_system_client.get_file_client(
                file_path=ignore_file
            )
            # Download the meta file for the pipeline
            with contextlib.suppress(Exception):
                with open(ignore_file_download_path, "wb") as data:
                    ignore_meta_file_client.download_file().readinto(data)

                # Read the ignore file
                with open(ignore_file_download_path, "r") as f:
                    self.ignore_files = f.readlines()

            # Save trimmed file names
            self.ignore_files = [x.strip() for x in self.ignore_files]

            # Remove empty lines
            self.ignore_files = [x for x in self.ignore_files if x]

            # Remove duplicates
            self.ignore_files = list(set(self.ignore_files))

            # Remove any that start with a '#'
            self.ignore_files = [x for x in self.ignore_files if not x.startswith("#")]

    # Downloading file map
        try:
            with open(file_map_download_path, "wb") as data:
                meta_file_client.download_file().readinto(data)
            with open(file_map_download_path, "r") as f:
                self.file_map = json.load(f)
        except ResourceNotFoundError:
            print("file map.json is not found")
        # Load the meta file

        if isinstance(self.file_map, dict):
            self.file_map = self.file_map["logs"]
        for entry in self.file_map:
            # This is to delete the output files of files that are no longer in the input folder
            entry["seen"] = False

    def __del__(self):
        shutil.rmtree(self.meta_temp_folder_path)

    def add_entry(self, path, input_last_modified):
        # Add files that do not exist in the array
        entry = [entry for entry in self.file_map if entry["input_file"] == path]
        if not entry:
            self.file_map.append(
                {
                    "input_file": path,
                    "output_files": [],
                    "input_last_modified": input_last_modified,
                    "seen": True,
                    "error": [],
                }
            )

    def file_should_process(self, path, input_last_modified) -> bool:
        """Check if the file has been modified since the last time it was
        processed and no errors exist during processing"""
        for entry in self.file_map:
            if entry["input_file"] == path:
                entry["seen"] = True

                t = input_last_modified.strftime("%Y-%m-%d %H:%M:%S+00:00")
                count_error = len(entry["error"])

                return t != entry["input_last_modified"] or count_error > 0

        return True

    def confirm_output_files(self, path, workflow_output_files, input_last_modified):
        # Add the new output files to the file map
        for entry in self.file_map:
            if entry["input_file"] == path:
                entry["output_files"] = workflow_output_files
                entry["input_last_modified"] = input_last_modified
                break

    def delete_preexisting_output_files(self, path):
        input_path = path
        # Delete the output files associated with the input file
        # We are doing a file level replacement
        for entry in self.file_map:
            if entry["input_file"] == input_path:
                for output_file in entry["output_files"]:
                    with contextlib.suppress(Exception):
                        output_file_client = self.file_system_client.get_file_client(
                            file_path=output_file
                        )
                        output_file_client.delete_file()
                break

    def delete_out_of_date_output_files(self):
        # Delete the output files that are no longer in the input folder
        for entry in self.file_map:
            if not entry["seen"]:
                for output_file in entry["output_files"]:
                    with contextlib.suppress(Exception):
                        output_file_client = self.file_system_client.get_file_client(
                            file_path=output_file
                        )

                        output_file_client.delete_file()

    def append_errors(self, error_exception, path):
        # This function appends errors to the json
        entry = [entry for entry in self.file_map if entry["input_file"] == path][0]
        entry["error"].append(error_exception)

    def clear_errors(self, path):
        # This function clear errors to the json
        entry = [entry for entry in self.file_map if entry["input_file"] == path][0]
        entry["error"] = []

    def remove_seen_flag_from_map(self):
        # Remove the entries that are no longer in the input folder
        self.file_map = [entry for entry in self.file_map if entry["seen"]]

        # Remove the seen flag from the file map
        for entry in self.file_map:
            del entry["seen"]

    def upload_json(self):
        # Write the file map to a file
        meta_temp_folder_path = tempfile.mkdtemp()

        file_map_file_path = os.path.join(meta_temp_folder_path, "file_map.json")

        errors_items_count = 0

        error_file_map = []
        error_file_list = []

        for item in self.file_map:
            if len(item["error"]) > 0:
                errors_items_count += 1
                error_file_list.append(item["input_file"])
                error_file_map.append(item)

        output_dict = {
            "logs": self.file_map,
            "errors": {
                "count": errors_items_count,
                "files": error_file_list,
                "items": error_file_map,
            },
        }

        with open(file_map_file_path, "w") as f:
            json.dump(output_dict, f, indent=4, sort_keys=True, default=str)

        with open(file_map_file_path, "rb") as data:
            output_file_client = self.file_system_client.get_file_client(
                file_path=f"{self.dependency_folder}/file_map.json",
            )

            # # delete the existing file map
            with contextlib.suppress(Exception):
                output_file_client.delete_file()

            output_file_client.upload_data(data, overwrite=True)

    def is_file_ignored(self, file_name, path) -> bool:
        return file_name in self.ignore_files or path in self.ignore_files

    def is_file_ignored_by_path(self, path) -> bool:
        for pattern in self.ignore_files:
            if not pattern:
                continue

            if pathlib.Path(path).match(pattern):
                return True

        return False

    def files_to_ignore(self, input_folder) -> bool:
        files_to_ignore = []

        # Using glob to get all files that match the ignore pattern
        for pattern in self.ignore_files:
            if not pattern:
                continue

            glob_pattern = os.path.join(input_folder, pattern)

            files_to_ignore.extend(glob.glob(glob_pattern))

        return files_to_ignore
