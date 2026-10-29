import pytest

from arbor.io import load, save, sha
from arbor.training import pipeline


@pytest.mark.parametrize("custom", [False, True])
def test_admission_training_uses_selected_late_fusion(tmp_path, monkeypatch, custom):
    dataset = tmp_path/"dataset"
    dataset.mkdir()
    save(dataset/"manifest.json", {"dataset": "fixture"})
    frozen = tmp_path/"models/fusion.json"
    supplied = tmp_path/"custom-fusion.json"
    save(frozen, {"name": "frozen"})
    save(supplied, {"name": "custom"})
    monkeypatch.setattr(pipeline, "root", lambda: tmp_path)
    monkeypatch.setattr(pipeline, "ensure_build", lambda: (None, None))
    monkeypatch.setattr(pipeline, "Kernel", lambda library: object())
    monkeypatch.setattr(pipeline, "load_history", lambda *args: {"history": "fixture"})
    monkeypatch.setattr(pipeline, "fit_fusion", lambda *args: {"name": "early"})
    windows = {}
    def prepare(data, kernel, model, day, directory):
        windows[day] = model["name"]
        return directory
    monkeypatch.setattr(pipeline, "prepare_window", prepare)
    monkeypatch.setattr(pipeline, "collect_costs", lambda directory, day, family, **kwargs: [{"day": day}])
    monkeypatch.setattr(pipeline, "contract", lambda: {"fixture": True})
    monkeypatch.setattr(pipeline, "fit", lambda records, **kwargs: {"records": len(records)})
    selected = supplied if custom else frozen
    output = tmp_path/"training"
    receipt = pipeline.train("admission", dataset, output, fusion_model=selected if custom else None)
    assert windows == {day: ("early" if day < 17 else "custom" if custom else "frozen")
                       for day in range(13, 21)}
    assert receipt["fusion_input"] == dict(path=str(selected.resolve()), sha256=sha(selected),
                                            windows=[17, 18, 19, 20])
    assert load(output/"later_fusion.json") == load(selected)
    assert load(output/"model.json") == {"records": 40}


@pytest.mark.parametrize("kind,small", [("fusion", False), ("admission", True)])
def test_late_fusion_option_rejects_unused_paths(tmp_path, kind, small):
    output = tmp_path/"training"
    with pytest.raises(ValueError, match="days 17–20"):
        pipeline.train(kind, tmp_path/"data", output, small=small, fusion_model=tmp_path/"fusion.json")
    assert not output.exists()
