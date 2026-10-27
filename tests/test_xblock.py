import csv
import zipfile
from arbor.data import ingest
from arbor.io import load
from arbor.xblock import FIELDS, split


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
    with (prepared/"test/input.csv").open() as f:
        rows=list(csv.reader(f))
    assert rows[0]==rows[1] and rows[0][8]==str(amount)
