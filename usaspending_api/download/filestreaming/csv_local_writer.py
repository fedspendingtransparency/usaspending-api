from usaspending_api.download.filestreaming.csv_abstract_writer import CsvAbstractWriter


class CsvLocalWriter(CsvAbstractWriter):
    """
    Writes a CSV to the local file system in a steaming manner
    use with the "with" python construct
    """

    def __init__(self, filename, header):
        """

        args

        bucket - the string name of the S3 bucket
        filename - string filename and path in the S3 bucket
        header - list of strings for the header

        """
        self.stream = open(filename, "w", newline='')
        super(CsvLocalWriter, self).__init__(header)

    def _write(self, data):
        """

        args

        data -  (string) a string be written to the current file

        """
        self.stream.write(data)

    def __exit__(self, error_type, value, traceback):
        """

        args
        error_type - the type of error
        value - the value of the error
        traceback - the traceback of the error

        This function calls the smart open exit in the
        'with' block

        """
        self.stream.close()
