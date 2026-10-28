"""Write the deterministic demo trace in XBlock ZIP format for end-to-end checks."""
import argparse
import csv
import io
from pathlib import Path
import tempfile
import zipfile

from arbor.data import make_demo
from arbor.xblock import FIELDS


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    output = parser.parse_args().output
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as temporary:
        raw = make_demo(temporary)
        with zipfile.ZipFile(output, "x", compression=zipfile.ZIP_DEFLATED) as archive:
            with archive.open("BlockTransaction.csv", "w") as member:
                with io.TextIOWrapper(member, encoding="utf-8", newline="") as stream:
                    writer = csv.writer(stream)
                    writer.writerow(FIELDS)
                    for name in ("history", "test"):
                        with (raw/(name+".csv")).open(newline="") as source:
                            writer.writerows(csv.reader(source))
    print(output)


if __name__ == "__main__":
    main()
