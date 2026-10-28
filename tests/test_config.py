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
