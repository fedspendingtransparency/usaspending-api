import io
import csv


class CsvAbstractWriter(object):
    """
    Writes a CSV to a Files System in a steaming manner
    use with the "with" python construct
    """

    BUFFER_SIZE = (5 * 1024 ** 2)
    BATCH_SIZE = 100

    def __init__(self, header):
        """

        args

        filename - string filename and path in the S3 bucket
        header - list of strings for the header

        """
        self.rows = []
        self.write(header)

    def write(self, data_list):
        """

        args
        dataList - list of strings to be written to a file system.
        Adds a row of csv into the S3 stream

        """
        str_list = []
        for data in data_list:
            if data is None:
                data = ""
            str_list.append(str(data))
        self.rows.append(str_list)
        if len(self.rows) > self.BATCH_SIZE:
            self.finish_batch()

    def finish_batch(self):
        """ Write the last unfinished batch """
        io_stream = io.StringIO()
        csv_formatter = csv.writer(io_stream)
        csv_formatter.writerows(self.rows)
        self._write(io_stream.getvalue())
        self.rows = []

    def _write(self, data):
        """

        args

        data -  (string) a string be written to the current file

        """
        raise NotImplementedError("Do not instantiate csvAbstractWriter directly.")

    def __enter__(self):
        return self

    def __exit__(self, error_type, value, traceback):
        """

        args
        error_type - the type of error
        value - the value of the error
        traceback - the traceback of the error

        """
        raise NotImplementedError("Do not instantiate csvAbstractWriter directly.")
