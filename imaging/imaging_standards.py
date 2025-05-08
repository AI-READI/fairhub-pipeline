from abc import abstractmethod


class DataDomain:
    def __init__(self):
        print("in standards init")

    @abstractmethod
    def organize(self, files, outfile, **kwargs):
        pass

    @abstractmethod
    def convert(self, infile, outfile, **kwargs):
        pass

    @abstractmethod
    def metadata(self, files, outfile, **kwargs):
        pass
