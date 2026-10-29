from arbor.cli import parser
from arbor.config import resolve


def test_nested_overrides_preserve_demo_profile(tmp_path):
    custom = tmp_path/"run.yaml"
    custom.write_text("resources:\n  max_owned_rss_gib: 12\n")
    limits = resolve("demo", custom)["resources"]
    assert limits["max_owned_rss_gib"] == 12
    assert limits["min_available_gib"] == limits["min_system_free_gib"] == 2
    assert limits["runtime_available_floor_gib"] == .5


def test_model_options_and_cli_precedence(tmp_path):
    custom = tmp_path/"run.yaml"
    custom.write_text("inject_tps: 10\nadmission_model: model.json\n")
    config = resolve("demo", custom, {"inject_tps": 20, "admission_model": tmp_path/"trained.json"})
    assert config["inject_tps"] == 20
    assert config["admission_model"] == str(tmp_path/"trained.json")
    args = parser().parse_args(["run", "full", "--admission-model", "new.json"])
    assert str(args.admission_model) == "new.json"


def test_yaml_paths_follow_config_location_and_cli_paths_follow_cwd(tmp_path, monkeypatch):
    settings = tmp_path/"settings"
    caller = tmp_path/"another working directory"
    settings.mkdir()
    caller.mkdir()
    custom = settings/"run.yaml"
    custom.write_text("dataset: ../inputs\nfusion_model: models/fusion.json\nadmission_model: models/admission.json\n")
    monkeypatch.chdir(caller)
    config = resolve("demo", custom)
    assert config["dataset"] == str(tmp_path/"inputs")
    assert config["fusion_model"] == str(settings/"models/fusion.json")
    assert config["admission_model"] == str(settings/"models/admission.json")
    overridden = resolve("demo", custom, {"dataset": "cli-data", "fusion_model": "cli-fusion.json"})
    assert overridden["dataset"] == str(caller/"cli-data")
    assert overridden["fusion_model"] == str(caller/"cli-fusion.json")


def test_home_directory_expands_for_all_config_paths(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    config = resolve("demo", overrides={"dataset": "~/inputs", "fusion_model": "~/fusion.json",
                                         "admission_model": "~/admission.json"})
    assert config["dataset"] == str(tmp_path/"inputs")
    assert config["fusion_model"] == str(tmp_path/"fusion.json")
    assert config["admission_model"] == str(tmp_path/"admission.json")


def test_resume_relocation_and_training_model_options():
    args = parser().parse_args(["resume", "runs/moved", "--dataset", "~/inputs"])
    assert str(args.dataset) == "~/inputs"
    args = parser().parse_args(["train", "admission", "--fusion-model", "trained/fusion.json"])
    assert str(args.fusion_model) == "trained/fusion.json"
