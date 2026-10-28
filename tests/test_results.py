from arbor.io import save
from arbor.results import plot


def test_shared_scale_axes_include_largest_point_in_later_panel(tmp_path):
    rows, cases = [], []
    for load, throughput in ((50, 500), (75, 3000), (100, 2000)):
        for method in ("Arbor", "Monoxide", "CLPA"):
            key = f"{load}_{method}"
            cases.append(dict(id=key, load_percent=load))
            rows.append(dict(id=key, method=method, shards=4, complete_tps=throughput,
                             end_to_end_seconds=dict(mean=10)))
    save(tmp_path/"plan.json", dict(recipe="scale", cases=cases))
    save(tmp_path/"summary.json", rows)
    output = plot(tmp_path)
    assert (output/"results.svg").is_file()
