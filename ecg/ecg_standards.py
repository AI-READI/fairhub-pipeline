from abc import ABC, abstractmethod


class DataDomain(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def convert(self, infile, outfile, **kwargs):
        pass

    @abstractmethod
    def metadata(self, files, outfile, **kwargs):
        pass
