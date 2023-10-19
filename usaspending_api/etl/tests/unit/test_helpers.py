import contextlib
import csv
import os
import tempfile


@contextlib.contextmanager
def mutated_csv(filename, mutator):
    """Returns tempfile copied from filename, but mutated.

    If `filename` points to a CSV, then we load it,
    apply the `mutator` function to each row, and save
    the changed data to a tempfile, which is returned.

    Used to create small, specific variants on test CSVs.
    """

    outfile = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
    with open(filename) as infile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            writer.writerow(mutator(row))
    outfile.close()
    yield outfile
    os.unlink(outfile.name)
