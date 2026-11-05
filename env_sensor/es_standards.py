"""Abstract base classes for the environmental sensor data domain."""

from abc import ABC, abstractmethod


class DataDomain(ABC):
    """Defines the common interface expected by environmental sensor processing tools.

    Subclasses must implement conversion and metadata extraction entry points.
    """

    def __init__(self):
        pass

    @abstractmethod
    def convert(self, infile, outfile, **kwargs):
        pass

    @abstractmethod
    def metadata(self, files, outfile, **kwargs):
        pass
