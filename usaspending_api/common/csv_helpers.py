import codecs
import csv
import os

from usaspending_api.common.retrieve_file_from_uri import RetrieveFileFromUri
from usaspending_api.download.helpers import write_to_download_log as write_to_log


def count_rows_in_delimited_file(filename, has_header=True, safe=True, delimiter=","):
    """
    Simple and efficient utility function to provide the rows in a valid delimited file
    If a header is not present, set head_header parameter to False

    Added "safe" mode which will handle any NUL BYTE characters in delimited files
    It does increase the function runtime by approx 10%.
        Example:
            "non-safe" counting ~1 million records in a delimited file takes 9s
            using "safe mode", it now takes 10s

    """
    with codecs.open(filename, "r") as f:
        if safe:
            row_count = sum(1 for row in csv.reader((line.replace("\0", "") for line in f), delimiter=delimiter))
        else:
            row_count = sum(1 for row in csv.reader(f, delimiter=delimiter))
    if has_header and row_count > 0:
        row_count -= 1

    return row_count


# Function inspired by Ben Welsh: https://gist.github.com/palewire/596056
def partition_large_delimited_file(
    download_job,
    file_path: str,
    delimiter=",",
    row_limit=10000,
    output_name_template="output_%s.csv",
    keep_headers=True,
):
    """Splits a delimited file into multiple partitions if it exceeds the row limit.

    "A quick bastardization of the Python CSV library."
    Arguments:
        `filepath`: filepath string of the csv file to partition
        `delimiter`: single character delimiter (typically used for CSVs which is the default value)
        `row_limit`: The number of rows you want in each output file. 10,000 by default.
        `output_name_template`: A %s-style template for the numbered output files.
        `keep_headers`: Whether or not to copy the original headers into each output file.
    """
    new_csv_list = []
    output_path = os.path.dirname(file_path)
    with open(file_path, "r") as source_csv:
        original_csv_file_reader = csv.reader(source_csv, delimiter=delimiter)
        partition_number = 1
        current_out_path = os.path.join(output_path, output_name_template % partition_number)
        new_csv_list.append(current_out_path)
        dest_csv = None
        line_number = 0
        try:
            dest_csv = open(current_out_path, "w")
            current_partition_writer = csv.writer(dest_csv, delimiter=delimiter)
            current_limit = row_limit

            if keep_headers:
                headers = next(original_csv_file_reader)
                current_partition_writer.writerow(headers)

            for line_number, row in enumerate(original_csv_file_reader, start=1):
                if line_number > current_limit:  # limit reached, create a new CSV file for the next partition
                    partition_number += 1
                    current_limit = row_limit * partition_number
                    current_out_path = os.path.join(output_path, output_name_template % partition_number)
                    new_csv_list.append(current_out_path)
                    dest_csv.close()
                    dest_csv = open(current_out_path, "w")
                    current_partition_writer = csv.writer(dest_csv, delimiter=delimiter)
                    if keep_headers:
                        current_partition_writer.writerow(headers)

                current_partition_writer.writerow(row)
        finally:
            write_to_log(message=f"Number of lines partitioned: {line_number}", download_job=download_job)
            if dest_csv and not dest_csv.closed:
                dest_csv.close()

    return new_csv_list


def read_csv_file_as_list_of_dictionaries(file_path):
    """
    Read in the specified CSV file and return as a list of dictionaries ("records").
    """
    with RetrieveFileFromUri(file_path).get_file_object(True) as f:
        return list(csv.DictReader(f))
