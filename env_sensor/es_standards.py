from abc import abstractmethod


class DataDomain:
    def __init__(self):
        pass
        # print("in standards init")

    @abstractmethod
    def convert(self, infile, outfile, **kwargs):
        pass

    @abstractmethod
    def metadata(self, files, outfile, **kwargs):
        pass
