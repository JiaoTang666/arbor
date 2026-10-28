import csv
import numpy as np
import pytest

from arbor.data import ingest, make_demo, parse, verify, verification_receipt


def row(a=1, b=2, amount="1", at=10):
    return ["1", str(at), "hash", f"0x{a:040x}", f"0x{b:040x}", "None", "0", "0", amount]


def test_exact_integer_filters_and_duplicates(tmp_path):
    huge = str(2**200+123)
    history = tmp_path/"history.csv"
    test = tmp_path/"test.csv"
    with history.open("w", newline="") as f:
        csv.writer(f).writerows([row(amount=huge), row(amount=huge), row(amount="-1"), row(a=2,b=2)])
    with test.open("w", newline="") as f:
        csv.writer(f).writerows([row(2,3,at=11), row(4,5,at=12)])
    out = ingest(history, test, tmp_path/"dataset")
    manifest = verify(out)
    assert manifest["history"]["accepted"] == 2
    assert manifest["test_new_accounts"] == 3
    assert list(csv.reader((out/"history/input.csv").open()))[0][8] == huge
    np.testing.assert_array_equal(np.load(out/"test/sender.npy"), [1,3])
    np.testing.assert_array_equal(np.load(out/"test/recipient.npy"), [2,4])
    assert parse(row(amount="1e20"))[1] == "invalid_amount"
    (out/"test/input.csv").write_text("tampered")
    with pytest.raises(ValueError, match="mismatch"):
        verify(out)


def test_reject_reordered_input(tmp_path):
    h = tmp_path/"h.csv"; t = tmp_path/"t.csv"
    with h.open("w", newline="") as f:
        csv.writer(f).writerows([row(at=10), row(at=9)])
    t.write_text("")
    with pytest.raises(ValueError, match="regression"):
        ingest(h, t, tmp_path/"out")


def test_demo_covers_all_admission_classes(tmp_path):
    raw = make_demo(tmp_path/"raw")
    out = ingest(raw/"history.csv", raw/"test.csv", tmp_path/"data")
    m = verify(out)
    assert m["test"]["accepted"] == 120
    assert set(m["test"]["entry_classes"]) == {"0", "1", "2"}


def test_verified_identity_avoids_rehash_and_detects_changes(tmp_path, monkeypatch):
    from arbor import data
    raw = make_demo(tmp_path/"raw")
    dataset = ingest(raw/"history.csv", raw/"test.csv", tmp_path/"data")
    manifest = verify(dataset)
    receipt = verification_receipt(dataset, manifest)
    original = data.sha
    calls = []
    def counted(path):
        calls.append(str(path))
        return original(path)
    monkeypatch.setattr(data, "sha", counted)
    verify(dataset, receipt)
    assert calls == [str(dataset/"manifest.json")]
    (dataset/"test/input.csv").write_text("changed")
    with pytest.raises(ValueError, match="changed"):
        verify(dataset, receipt)
