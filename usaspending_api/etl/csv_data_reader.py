import csv


class CsvDataReader(object):
    """
    Class for reading csv files.

    This code borrows heavily from https://districtdatalabs.silvrback.com/simple-csv-data-wrangling-with-python
    """

    def __init__(self, path):
        self.path = path

        self._length = None

    def __iter__(self):
        self._length = 0
        with open(self.path, "r", encoding="utf8") as data:
            reader = csv.DictReader(data)
            for row in reader:
                # Save the statistics
                # A neat future enhancement would be to save counts by agency,
                # total obligated dollars, and other metrics that might be
                # good do display after the load is complete
                self._length += 1

                yield row

    def __len__(self):
        if self._length is None:
            for row in self:
                continue  # Read the data for length and counter
        return self._length

    def reset(self):
        """
        In case of partial seeks (e.g. breaking in the middle of the read)
        """
        self._length = None
