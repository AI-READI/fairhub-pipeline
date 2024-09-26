import logging
from . import es_utils

meta_logger = logging.getLogger("es.metadata")


def metadata_env_sensor(input_csv_file):
    """
    Read one environmental sensor *.csv file and retrieve only the metadata.
    Metadata will be returned as a dict() and can also be written to an output_file.

    Args:
        input_csv_file (string): EnvironmentalSensor self-documenting *.csv

    Returns:
        dictionary of metadata
    """

    meta_dict = dict()  # collect into a dict as we peruse the file

    meta_logger.info(f"metadata input_csv_file {input_csv_file}")

    try:
        sen55_id, header_list, column_string, data_list, err_dict = (
            es_utils.read_single_csv(input_csv_file)
        )
    except Exception as e:  # FileNotFoundError is most likely
        meta_logger.error(f"Exception {e} for {input_csv_file}")
        return meta_dict

    try:
        header_dict = dict()
        for line in header_list:
            myline = line.strip().split(":")
            k = myline[0][1:].strip()
            header_dict[k] = myline[1].strip()

        meta_dict["modality"] = "environmental_sensor"
        meta_dict["manufacturer"] = header_dict["environmental_sensor_manufacturer"]
        meta_dict["device"] = header_dict["environmental_sensor_device_model"]
        meta_dict["laterality"] = "none"
        meta_dict["participant_id"] = header_dict["meta_participant_id"]
        meta_dict["sensor_id"] = header_dict["meta_sensor_id"]
        meta_dict["sensor_location"] = header_dict["meta_sensor_location"]
        meta_dict["number_of_observations"] = header_dict["meta_number_of_observations"]
        meta_dict["sensor_sampling_extent_in_days"] = header_dict[
            "meta_extent_of_observation_in_days"
        ]
    except Exception as e:  # reading a non-self-documenting csv if most likely
        meta_logger.error(
            f"Exception {e} for {input_csv_file} when trying to fill meta_dict."
        )
        return meta_dict

    return meta_dict


class ESManifest:
    def __init__(self):
        self.manifest = []

    def add_metadata(self, entry, output_filepath):

        entry["env_sensor_filepath"] = output_filepath

        self.manifest.append(entry)

    def write_tsv(
        self,
        file_path: str,
    ):
        # Sort the manifest by participant_id
        self.manifest = sorted(self.manifest, key=lambda x: x["participant_id"])

        # Write the data to a TSV file
        with open(file_path, "w") as f:
            f.write(
                "participant_id\tmodality\tenv_sensor_filepath\tsensor_location\tnumber_of_observations\tsensor_sampling_extent_in_days\tsensor_id\tmanufacturer\tmanufacturers_model_name\n"
            )

            for entry in self.manifest:
                f.write(
                    f"{entry['participant_id']}\t{entry['modality']}\t{entry['env_sensor_filepath']}\t{entry['sensor_location']}\t{entry['number_of_observations']}\t{entry['sensor_sampling_extent_in_days']}\t{entry['sensor_id']}\t{entry['manufacturer']}\t{entry['device']}\n"
                )
