"""Stream ordered XBlock Block Transaction CSV/ZIP files into two causal splits."""
import argparse
from collections import Counter
from contextlib import contextmanager
import csv
import io
from pathlib import Path
import tempfile
import zipfile

from .data import ingest, parse
from .io import save, sha

FIELDS = "blockNumber timestamp transactionHash from to toCreate fromIsContract toIsContract value gasLimit gasPrice gasUsed callingFunction isError eip2718type baseFeePerGas maxFeePerGas maxPriorityFeePerGas".split()


@contextmanager
def source(path):
    path = Path(path)
    if path.suffix.lower() == ".zip":
        with zipfile.ZipFile(path) as archive:
            members = [m for m in archive.namelist() if m.lower().endswith(".csv") and "blocktransaction" in m.lower()]
            if len(members) != 1:
                raise ValueError("expected exactly one Block Transaction CSV per archive")
            with archive.open(members[0]) as raw, io.TextIOWrapper(raw,encoding="utf-8-sig",newline="") as stream:
                yield stream
    else:
        with path.open(encoding="utf-8-sig",newline="") as stream:
            yield stream


def split(paths, output, history_count, test_count):
    if any(type(n) is not int or n <= 0 for n in (history_count, test_count)):
        raise ValueError("split sizes must be positive integers")
    output = Path(output)
    output.mkdir(parents=True,exist_ok=False)
    count, previous, rejected, ranges, sources = 0, -1, Counter(), {}, []
    with (output/"history.csv").open("w",newline="") as h, (output/"test.csv").open("w",newline="") as t, \
            (output/"history.origin.csv").open("w", newline="") as ho, \
            (output/"test.origin.csv").open("w", newline="") as to:
        writers = [csv.writer(h,lineterminator="\r\n"),csv.writer(t,lineterminator="\r\n")]
        origins = [csv.writer(ho), csv.writer(to)]
        for path in paths:
            sources.append(dict(name=Path(path).name,sha256=sha(path)))
            with source(path) as stream:
                reader=csv.reader(stream)
                header=next(reader, None)
                if header != FIELDS:
                    raise ValueError("XBlock Block Transaction header does not match the documented 18 columns")
                for index,row in enumerate(reader,1):
                    value,reason = parse(row)
                    if reason:
                        rejected[reason]+=1; continue
                    a,b,stamp,amount=value
                    if stamp < previous:
                        raise ValueError("input archives are not chronologically ordered")
                    previous=stamp
                    part = 0 if count < history_count else 1
                    row[3],row[4],row[8]="0x"+a,"0x"+b,amount
                    writers[part].writerow(row)
                    origins[part].writerow([len(sources)-1, index])
                    mark=dict(source=Path(path).name,row=index,block=row[0],timestamp=stamp)
                    interval=ranges.setdefault(str(part),dict(first=mark))
                    interval["last"]=mark
                    count+=1
                    if count == history_count+test_count: break
            if count == history_count+test_count: break
    if count != history_count+test_count:
        raise ValueError(f"insufficient valid transactions: {count}; requested {history_count+test_count}")
    save(output/"source_manifest.json",dict(sources=sources,history=history_count,test=test_count,
         rejected=dict(rejected),ranges=ranges,order="source order; no deduplication; exact integer values",
         files={p.name:sha(p) for p in output.glob("*.csv")}))
    return output


def prepare(paths, output, history_count, test_count):
    """Turn ordered source archives into one ready-to-run dataset."""
    paths = [Path(path).resolve() for path in paths]
    if not paths:
        raise ValueError("at least one XBlock source is required")
    output = Path(output).resolve()
    if output.exists():
        raise FileExistsError("dataset already exists: "+str(output))
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        with tempfile.TemporaryDirectory(prefix=".arbor-import-", dir=output.parent) as temporary:
            staging = Path(temporary)
            raw = split(paths, staging/"split", history_count, test_count)
            dataset = ingest(raw/"history.csv", raw/"test.csv", staging/"dataset")
            dataset.rename(output)
    except zipfile.BadZipFile as error:
        raise ValueError("invalid XBlock ZIP: "+str(error)) from error
    return output


if __name__ == "__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sources",nargs="+",type=Path)
    parser.add_argument("--output",required=True,type=Path)
    parser.add_argument("--history",required=True,type=int)
    parser.add_argument("--test",required=True,type=int)
    args=parser.parse_args()
    print(split(args.sources,args.output,args.history,args.test))
