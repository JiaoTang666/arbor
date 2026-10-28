"""Ordered Block Transaction ingestion; amounts never pass through floating point."""
from array import array
from collections import Counter
from contextlib import ExitStack
import csv
from pathlib import Path
import re
import shutil

import numpy as np

from .io import load, save, sha
from .cache import signature

ADDRESS = re.compile(r"^(?:0x)?([0-9a-f]{40})$")
INTEGER = re.compile(r"^[+-]?\d+$")


def parse(row):
    """Return canonical fields and a single exclusion reason, preserving duplicates."""
    if len(row) < 9:
        return None, "short_row"
    if row[6] != "0" or row[7] != "0":
        return None, "contract_endpoint"
    a, b = ADDRESS.fullmatch(row[3].lower()), ADDRESS.fullmatch(row[4].lower())
    if not a or not b:
        return None, "invalid_address"
    if a[1] == b[1]:
        return None, "self_transfer"
    if not INTEGER.fullmatch(row[8]):
        return None, "invalid_amount"
    if int(row[8]) < 0:
        return None, "negative_amount"
    try:
        at = int(row[1])
    except ValueError:
        return None, "invalid_timestamp"
    return (a[1], b[1], at, str(int(row[8]))), None


def ingest(history, test, output, *, history_raw_limit=None, test_raw_limit=None):
    """Prepare both inputs in one causal address namespace, without shuffling."""
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    ids, names = {}, []
    parent, roots, first, birth, depth = (array("q") for _ in range(5))
    reports = {}
    previous = -1
    for split, source, limit in (("history", history, history_raw_limit), ("test", test, test_raw_limit)):
        directory = output/split
        directory.mkdir()
        columns = {k: array("q") for k in ("sender", "recipient", "timestamp", "raw_row")}
        kinds, rejected, accepted, raw_count = Counter(), Counter(), 0, 0
        origin = Path(source).with_suffix(".origin.csv")
        if origin.exists():
            columns.update(source_file=array("q"), source_row=array("q"))
        with ExitStack() as stack:
            stream = stack.enter_context(Path(source).open(newline="", encoding="utf-8-sig"))
            dest = stack.enter_context((directory/"input.csv").open("w", newline="", encoding="utf-8"))
            origin_rows = csv.reader(stack.enter_context(origin.open(newline=""))) if origin.exists() else None
            writer = csv.writer(dest, lineterminator="\r\n")
            for raw, row in enumerate(csv.reader(stream)):
                if limit is not None and raw >= limit:
                    break
                raw_count += 1
                original = next(origin_rows, None) if origin_rows is not None else None
                if origin_rows is not None and (original is None or len(original) != 2):
                    raise ValueError("source provenance sidecar does not match input rows")
                value, reason = parse(row)
                if reason:
                    rejected[reason] += 1
                    continue
                a, b, at, amount = value
                if at < previous:
                    raise ValueError(f"source timestamp regression at {split} row {raw}")
                previous = at
                old_a, old_b = a in ids, b in ids
                new = []
                for address in (a, b):
                    if address not in ids:
                        ids[address] = len(names)
                        names.append(address)
                        new.append(ids[address])
                        if split == "history":
                            parent.append(-1); roots.append(-(accepted+1))
                            first.append(accepted); birth.append(at); depth.append(0)
                u, v = ids[a], ids[b]
                kind = 0 if old_a and old_b else 1 if old_a or old_b else 2
                kinds[kind] += 1
                if split == "history" and kind == 1:
                    p, child = (u, v) if old_a else (v, u)
                    parent[child], roots[child], depth[child] = p, roots[p], depth[p]+1
                for key, item in zip(("sender", "recipient", "timestamp", "raw_row"), (u, v, at, raw)):
                    columns[key].append(item)
                if original is not None:
                    columns["source_file"].append(int(original[0]))
                    columns["source_row"].append(int(original[1]))
                # Preserve every other source field, including the transaction hash.
                row[3], row[4], row[8] = "0x"+a, "0x"+b, amount
                writer.writerow(row)
                accepted += 1
        if not accepted:
            raise ValueError(f"empty accepted {split} input")
        for key, values in columns.items():
            np.save(directory/(key+".npy"), np.asarray(values, dtype=np.int64))
        np.asarray(columns["timestamp"], dtype="<i8").tofile(directory/"source_time.bin")
        reports[split] = dict(raw_rows=raw_count, accepted=accepted, filtered=dict(rejected),
                             input_sha256=sha(directory/"input.csv"), source_sha256=sha(source),
                             raw_limit=limit, first_timestamp=columns["timestamp"][0],
                             last_timestamp=columns["timestamp"][-1], entry_classes=dict(kinds),
                             accounts=len(names))
        reports[split]["row_index_scope"] = "input CSV, zero-based"
        if origin.exists():
            reports[split]["original_row_scope"] = "source_manifest sources index and one-based archive data row"
        if split == "history":
            np.savez(output/"entry_forest.npz", parent=np.asarray(parent), root=np.asarray(roots),
                     first_sequence=np.asarray(first), first_timestamp=np.asarray(birth), depth=np.asarray(depth))
            np.save(output/"addresses.npy", np.asarray(names, dtype="S40"))
    reports["schema"] = "arbor_dataset_v1"
    reports["test_new_accounts"] = len(names)-reports["history"]["accounts"]
    source_manifest = Path(history).parent/"source_manifest.json"
    if source_manifest.exists() and Path(history).parent == Path(test).parent:
        shutil.copyfile(source_manifest, output/"source_manifest.json")
    reports["files"] = {str(p.relative_to(output)): sha(p) for p in sorted(output.rglob("*")) if p.is_file()}
    save(output/"manifest.json", reports)
    return output


def verify(directory, receipt=None):
    directory = Path(directory).resolve()
    manifest = load(directory/"manifest.json")
    if receipt is not None:
        record = load(receipt) if isinstance(receipt, (str, Path)) else receipt
        if record["directory"] != str(directory) or record["manifest_sha256"] != sha(directory/"manifest.json"):
            raise ValueError("verified dataset identity changed")
        if record["stats"] != dataset_stats(directory, manifest):
            raise ValueError("dataset changed during this run; verify it in a new run")
        return manifest
    before = dataset_stats(directory, manifest)
    for name, expected in manifest["files"].items():
        path = (directory/name).resolve()
        if not path.is_relative_to(directory) or not path.is_file() or sha(path) != expected:
            raise ValueError(f"dataset member mismatch: {name}")
    if dataset_stats(directory, manifest) != before:
        raise ValueError("dataset changed during verification")
    return manifest


def dataset_stats(directory, manifest):
    directory = Path(directory).resolve()
    result = {}
    for name in manifest["files"]:
        path = directory/name
        if not path.resolve().is_relative_to(directory) or path.is_symlink():
            raise ValueError("unsafe dataset member: "+name)
        result[name] = signature(path)
    return result


def verification_receipt(directory, manifest):
    directory = Path(directory).resolve()
    return dict(directory=str(directory), manifest_sha256=sha(directory/"manifest.json"),
                stats=dataset_stats(directory, manifest))


def make_demo(directory):
    """Deterministic public synthetic trace with OO, partner-following and NN traffic."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(43)
    for split, count in (("history", 2400), ("test", 120)):
        with (directory/(split+".csv")).open("w", newline="", encoding="utf-8") as stream:
            writer = csv.writer(stream, lineterminator="\n")
            for i in range(count):
                if split == "history":
                    a = i % 96 + 1
                    b = ((a-1)//8)*8 + int(rng.integers(1, 9))
                    if i % 10 == 2:
                        a, b = 10000+2*i, 10001+2*i
                    elif i % 10 == 3:
                        b = 10000+2*(i-1)
                    at = 1700000000 + i*300
                else:
                    a, b = i % 96 + 1, (i*7) % 96 + 1
                    if i % 3 == 1:
                        b = 1000+i
                    elif i % 3 == 2:
                        a, b = 2000+2*i, 2001+2*i
                    at = 1700000000 + 2400*300 + i*10
                if a == b:
                    b = b % 96 + 1
                writer.writerow([19000000+i, at, f"0x{i:064x}", f"0x{a:040x}", f"0x{b:040x}",
                                 "None", 0, 0, 10**20+i, 21000, 0, 21000, "0x", "None", "None", "None", "None", "None"])
    return directory
