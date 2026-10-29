import numpy as np
import pytest

from arbor.build import ensure_build
from arbor.history.grouping import aggregate_groups, groups
from arbor.history.kernel import Kernel
from arbor.history.relations import cohort_credit
from arbor.history.representation import aggregate_counts, aggregate_graph


def test_cohort_conservation_and_closest_known_ancestor():
    _, library = ensure_build()
    kernel = Kernel(library)
    parent = np.array([-1,-1,0,2,-1,4])
    tags = kernel.ancestors(parent,2)
    np.testing.assert_array_equal(tags, [0,1,0,0,-1,-1])
    rows, owners, shares = cohort_credit(np.array([0,2,4,3]), np.array([1,3,5,3]),2,tags)
    np.testing.assert_array_equal(np.bincount(rows,weights=shares,minlength=4),[0,1,1,1])
    assert shares[owners < 0].sum() == 1


def test_group_size_and_deterministic_placement():
    _, library = ensure_build(); kernel = Kernel(library)
    u = np.array([0,1,2,3,4,5]); v = np.array([1,0,3,2,5,4])
    w = np.ones(6); demand = np.ones((6,4))
    a, gd, _, _ = groups(kernel,u,v,w,demand,2,max_size=2)
    assert np.bincount(a).max() <= 2
    adj, _, _ = aggregate_groups(a,np.arange(len(gd)),u,v,w)
    r1,_ = kernel.placement(adj,gd,2);r2,_ = kernel.placement(adj,gd,2)
    np.testing.assert_array_equal(r1,r2)
    assert len(r1[a]) == 6 and set(r1) <= {0,1}


def test_count_graph_matches_temporal_graph_counts():
    u = np.array([2, 0, 2, 1, 0, 2], dtype=np.int64)
    v = np.array([1, 1, 1, 0, 1, 2], dtype=np.int64)
    t = np.array([10, 11, 12, 13, 14, 15], dtype=np.int64)
    temporal, _ = aggregate_graph(u, v, t, 10, 15, 2, (3, 6, 12))
    counts = aggregate_counts(u, v)
    assert set(counts) == {"sender", "recipient", "count"}
    for field, values in counts.items():
        np.testing.assert_array_equal(values, temporal[field])


def test_monoxide_layout_needs_no_graph_forest_models_or_kernel(tmp_path, monkeypatch):
    from arbor.data import ingest, make_demo
    from arbor.history import layout
    raw = make_demo(tmp_path/"raw")
    dataset = ingest(raw/"history.csv", raw/"test.csv", tmp_path/"dataset")
    def forbidden(*args, **kwargs):
        raise AssertionError("Monoxide invoked Arbor preparation")
    for name in ("ensure_build", "prepare", "prepare_counts", "load_history", "Kernel"):
        monkeypatch.setattr(layout, name, forbidden)
    result = layout.build_layout(dataset, dict(method="Monoxide", shards=4))
    addresses = np.load(dataset/"addresses.npy")
    np.testing.assert_array_equal(np.load(result/"routes.npy"), [int(a[-8:], 16) % 4 for a in addresses])
    assert not (dataset/"prepared").exists() and not (dataset/"counts").exists()


@pytest.mark.parametrize("frozen", [False, True])
def test_clpa_uses_only_counts_and_selected_binary(tmp_path, monkeypatch, frozen):
    from arbor.data import ingest, make_demo
    from arbor.history import layout, prepare
    raw = make_demo(tmp_path/"raw")
    dataset = ingest(raw/"history.csv", raw/"test.csv", tmp_path/"dataset")
    binary = tmp_path/("frozen-clpa" if frozen else ".build/arbor-clpa-layout")
    binary.parent.mkdir(parents=True, exist_ok=True)
    binary.write_bytes(b"selected CLPA fixture")
    monkeypatch.setattr(layout, "root", lambda: tmp_path)
    def forbidden(*args, **kwargs):
        raise AssertionError("CLPA invoked Arbor representation")
    for name in ("prepare", "load_history", "Kernel", "ensure_build"):
        monkeypatch.setattr(layout, name, forbidden)
    for name in ("SourceForest", "temporal_profiles", "aggregate_graph"):
        monkeypatch.setattr(prepare, name, forbidden)
    commands = []
    def run(command, **kwargs):
        commands.append(command)
        assert command[0] == str(binary)
        route = np.arange(len(np.load(dataset/"addresses.npy")), dtype="<i4") % 4
        route.tofile(command[command.index("--output")+1])
    monkeypatch.setattr(layout.subprocess, "run", run)
    artifacts = (tmp_path/"engine", tmp_path/"unused-library")
    if frozen:
        artifacts += (binary,)
    result = layout.build_layout(dataset, dict(method="CLPA", shards=4), build=artifacts)
    assert len(commands) == 1 and (result/"initial_ledger.json").is_file()
    assert not (dataset/"prepared").exists()
    with np.load(dataset/"counts/graph.npz") as graph:
        assert set(graph.files) == {"sender", "recipient", "count"}
        assert int(graph["count"].sum()) == 2400
    assert layout.build_layout(dataset, dict(method="CLPA", shards=4), build=artifacts) == result
    assert len(commands) == 1
