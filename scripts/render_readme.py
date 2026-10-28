"""Render README assets from the published measurement tables."""
import csv
from pathlib import Path
import shutil
import tempfile

from rich.console import Console
from rich.table import Table

from arbor.io import root, save
from arbor.results import plot


def main():
    project = root()
    output = project/"docs/assets"
    output.mkdir(exist_ok=True)
    with (project/"reference/scale_load.csv").open() as stream:
        reference = list(csv.DictReader(stream))
    assert len(reference) == 36
    keys = {(int(r["shards"]), int(r["load_percent"]), r["method"]) for r in reference}
    assert len(keys) == 36
    summary, cases = [], []
    for row in reference:
        case = dict(id=row["id"], load_percent=int(row["load_percent"]))
        cases.append(case)
        summary.append(dict(id=row["id"], method=row["method"], shards=int(row["shards"]),
                            complete_tps=float(row["complete_tps"]),
                            end_to_end_seconds=dict(mean=float(row["mean_latency_s"]))))
    with tempfile.TemporaryDirectory() as temporary:
        directory = Path(temporary)
        save(directory/"plan.json", dict(recipe="scale", cases=cases))
        save(directory/"summary.json", summary)
        figures = plot(directory)
        for extension in ("png", "pdf", "svg"):
            shutil.copyfile(figures/("results."+extension), output/("scale."+extension))
        shutil.copyfile(figures/"results_grayscale.png", output/"scale_grayscale.png")
    console = Console(record=True, width=104)
    console.print("[bold cyan]$ ./arbor run demo[/]")
    console.print("[bold]Arbor[/]  [green]3/3 completed[/]  ·  120 transactions per method")
    console.print("History → graph + forest → layouts → native execution → results\n", style="dim")
    table = Table(box=None, pad_edge=False)
    for column in ("Method", "Shards", "Complete TPS", "Mean (s)", "P95 (s)", "P99 (s)", "Cross (%)"):
        table.add_column(column, justify="left" if column == "Method" else "right")
    with (project/"validation/demo_summary.csv").open() as stream:
        for row in csv.DictReader(stream):
            table.add_row(row["method"], row["shards"], f"{float(row['complete_tps']):.2f}",
                          *(f"{float(row[k]):.3f}" for k in ("mean_seconds", "p95_seconds", "p99_seconds")),
                          f"{100*float(row['cross_fraction']):.2f}")
    console.print(table)
    console.print("\nResults: runs/<run-id>  ·  [dim]arbor status | pause | resume | stop[/]")
    console.save_svg(str(output/"cli.svg"), title="Arbor · native demo")
    save(output/"sources.json", dict(scale="reference/scale_load.csv", terminal="validation/demo_summary.csv",
                                    cells=36, measurements_per_cell=1))


if __name__ == "__main__":
    main()
