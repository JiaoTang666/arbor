import csv
import io
import json
import zipfile
import numpy as np
import pytest
from arbor.cli import main
from arbor.data import ingest
from arbor.io import load
from arbor.xblock import FIELDS, prepare, split


def test_raw_archive_keeps_duplicate_records_and_exact_large_values(tmp_path):
    raw=tmp_path/"BlockTransaction.csv"
    amount=10**80+123
    row=[1,1700000000,"0xhash","0x"+"1"*40,"0x"+"2"*40,"None",0,0,str(amount),21000,1,21000,"0x","None","None","None","None","None"]
    with raw.open("w",newline="") as f:
        writer=csv.writer(f);writer.writerow(FIELDS)
        writer.writerow(row);writer.writerow(row)
        invalid=list(row);invalid[8]="-1";writer.writerow(invalid)
        writer.writerow(row);writer.writerow(row)
    archive=tmp_path/"source.zip"
    with zipfile.ZipFile(archive,"w") as z:z.write(raw,raw.name)
    out=split([archive],tmp_path/"split",2,2)
    receipt=load(out/"source_manifest.json")
    assert receipt["rejected"]=={"negative_amount":1}
    assert receipt["ranges"]["0"]["last"]["row"] < receipt["ranges"]["1"]["first"]["row"]
    prepared=ingest(out/"history.csv",out/"test.csv",tmp_path/"prepared")
    assert load(prepared/"manifest.json")["test"]["accepted"]==2
    np.testing.assert_array_equal(np.load(prepared/"test/source_file.npy"), [0, 0])
    np.testing.assert_array_equal(np.load(prepared/"test/source_row.npy"), [4, 5])
    assert (prepared/"source_manifest.json").exists()
    with (prepared/"test/input.csv").open() as f:
        rows=list(csv.reader(f))
    assert rows[0]==rows[1] and rows[0][8]==str(amount)


def archive(path, rows, header=FIELDS):
    content = io.StringIO(newline="")
    writer = csv.writer(content)
    writer.writerow(header)
    writer.writerows(rows)
    with zipfile.ZipFile(path, "w") as stream:
        stream.writestr("BlockTransaction.csv", content.getvalue())
    return path


def transaction(a, b, stamp, value="1"):
    return ["18750000", str(stamp), "0xhash", f"0x{a:040x}", f"0x{b:040x}", "None", "0", "0",
            value, "21000", "1", "21000", "0x", "None", "None", "None", "None", "None"]


def test_cli_import_multiple_archives_preserves_origin_and_exact_values(tmp_path, capsys):
    large = str(2**200+123)
    first = archive(tmp_path/"first.zip", [transaction(1, 2, 10), transaction(1, 2, 10),
                                          transaction(2, 3, 11, "-1")])
    second = archive(tmp_path/"second.zip", [transaction(2, 3, 12, large), transaction(2, 3, 12, large)])
    output = tmp_path/"dataset"
    main(["data", "xblock", "--source", str(first), str(second), "--history-count", "2",
          "--test-count", "2", "--output", str(output), "--json"])
    assert json.loads(capsys.readouterr().out)["dataset"] == str(output)
    np.testing.assert_array_equal(np.load(output/"test/source_file.npy"), [1, 1])
    np.testing.assert_array_equal(np.load(output/"test/source_row.npy"), [1, 2])
    assert load(output/"source_manifest.json")["rejected"] == {"negative_amount": 1}
    with (output/"test/input.csv").open() as stream:
        rows = list(csv.reader(stream))
    assert rows[0] == rows[1] and rows[0][8] == large


def test_paper_command_is_source_information_only(tmp_path, capsys, monkeypatch):
    monkeypatch.chdir(tmp_path)
    main(["data", "paper", "--json"])
    info = json.loads(capsys.readouterr().out)
    assert info["url"].endswith("file=18750000to18999999_BlockTransaction.zip")
    assert "data xblock" in info["prepare_command"]
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("fault", ["count", "order", "header", "zip"])
def test_failed_import_does_not_publish_partial_dataset(tmp_path, fault):
    rows = [transaction(1, 2, 10), transaction(2, 3, 11)]
    if fault == "order":
        rows.reverse()
    source = archive(tmp_path/"source.zip", rows, ["wrong"] if fault == "header" else FIELDS)
    if fault == "zip":
        source.write_text("not a zip")
    output = tmp_path/"dataset"
    with pytest.raises(ValueError):
        prepare([source], output, 2 if fault == "count" else 1, 1)
    assert not output.exists()
    assert source.exists()


def test_existing_dataset_is_not_overwritten(tmp_path):
    output = tmp_path/"dataset"
    output.mkdir()
    (output/"keep").write_text("keep")
    with pytest.raises(FileExistsError):
        prepare([tmp_path/"source.zip"], output, 1, 1)
    assert (output/"keep").read_text() == "keep"
