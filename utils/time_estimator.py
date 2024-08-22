import os
import tempfile
import time


class TimeEstimator:
    """Class for handling calculating estimated time of arrival"""

    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time

    def eta(self, total_processed_files, file_paths, processed_seconds):
        remaining_files = len(file_paths) - total_processed_files

        processed_seconds += (self.end_time - self.start_time)

        average_time = processed_seconds / total_processed_files
        eta = average_time * remaining_files
        return eta
