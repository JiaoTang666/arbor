"""Plots from completed results only; no smoothing or invented uncertainty."""
import csv
from pathlib import Path

import numpy as np

from .io import load, save

COLORS = {"Arbor": "#0072B2", "Monoxide": "#D55E00", "CLPA": "#009E73"}
MARKERS = {"Arbor": "o", "Monoxide": "s", "CLPA": "^"}
STYLES = {"Arbor": "-", "Monoxide": "--", "CLPA": ":"}


def plot(directory):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from PIL import Image
    directory = Path(directory)
    rows = load(directory/"summary.json")
    if not rows:
        raise ValueError("no completed results to plot")
    plan = load(directory/"plan.json")
    configs = {c["id"]: c for c in plan["cases"]}
    output = directory/"figures"
    output.mkdir(exist_ok=True)
    plt.rcParams.update({"font.family":"DejaVu Sans", "font.size":9, "axes.spines.top":False,
                         "axes.spines.right":False, "pdf.fonttype":42, "ps.fonttype":42,
                         "axes.axisbelow":True, "savefig.dpi":300})
    recipe = plan["recipe"]
    if recipe == "scale":
        fig, axes = plt.subplots(2, 3, figsize=(7.2,4.4), sharex=True, sharey="row", layout="constrained")
        for col, load_percent in enumerate((50,75,100)):
            for method in COLORS:
                selected = sorted([r for r in rows if r["method"] == method and
                                   configs[r["id"]]["load_percent"] == load_percent], key=lambda r:r["shards"])
                for axis, values in ((axes[0,col], [r["complete_tps"] for r in selected]),
                                     (axes[1,col], [r["end_to_end_seconds"]["mean"] for r in selected])):
                    axis.plot([r["shards"] for r in selected], values, label=method, color=COLORS[method],
                              marker=MARKERS[method], linestyle=STYLES[method], markersize=4)
            axes[0,col].set_title(f"{load_percent}% offered load")
            axes[0,col].set_ylim(bottom=0)
            axes[1,col].set_yscale("log")
            axes[1,col].set_xlabel("Shards")
            axes[1,col].set_xticks([4,8,12,16])
        axes[0,0].set_ylabel("Complete throughput (tx/s)")
        axes[1,0].set_ylabel("Mean complete latency (s, log)")
        axes[0,1].legend(frameon=False, fontsize=8)
    elif recipe == "halflife":
        fig, axes = plt.subplots(1,3,figsize=(7.2,2.5),layout="constrained",sharey=True)
        lo, hi = min(r["end_to_end_seconds"]["mean"] for r in rows), max(r["end_to_end_seconds"]["mean"] for r in rows)
        for ax, days in zip(axes,(3.5,7,14)):
            values = np.full((3,3),np.nan)
            for r in rows:
                s,m,l = configs[r["id"]]["half_seconds"]
                if l == days*86400:
                    values[[12,24,48].index(m//3600),[3,6,12].index(s//3600)] = r["end_to_end_seconds"]["mean"]
            im = ax.imshow(values,origin="lower",cmap="cividis",vmin=lo,vmax=hi)
            ax.set_title(f"Long: {days:g} d")
            ax.set_xticks(range(3),[3,6,12]); ax.set_yticks(range(3),[12,24,48]); ax.set_xlabel("Short half-life (h)")
            for y,x in np.ndindex(values.shape):
                if np.isfinite(values[y,x]): ax.text(x,y,f"{values[y,x]:.1f}",ha="center",va="center",fontsize=8,
                                                     color="white" if values[y,x] < (lo+hi)/2 else "black")
            if days == 3.5: ax.plot(1,2,marker="s",ms=26,mfc="none",mec="#D55E00",mew=2)
        axes[0].set_ylabel("Middle half-life (h)")
        fig.colorbar(im,ax=axes,label="Mean complete latency (s)",shrink=.85)
    else:
        fig, axes = plt.subplots(1,3,figsize=(7.2,2.7),layout="constrained")
        key = {"group-size":"group_size", "tolerance":"tolerance"}.get(recipe)
        labels = [str(configs[r["id"]][key]) if key else
                  (configs[r["id"]]["ablation"] if recipe == "ablation" else r["method"]) for r in rows]
        metrics = ([r["end_to_end_seconds"]["mean"] for r in rows],
                   [r["cross_fraction"]*100 for r in rows],
                   [max(r["shard_work"])/np.mean(r["shard_work"]) for r in rows])
        for ax, values, title in zip(axes,metrics,("Mean complete latency (s)","Cross-shard transactions (%)","Work concentration")):
            if key:
                ax.plot([float(x) for x in labels],values,"o-",color=COLORS["Arbor"])
                ax.set_xlabel("Group-size limit" if key == "group_size" else "Burden tolerance")
                ax.set_xticks([float(x) for x in labels])
            else:
                ax.scatter(values,range(len(labels)),color=[COLORS.get(r["method"],COLORS["Arbor"]) for r in rows])
                ax.set_yticks(range(len(labels)),labels,fontsize=7); ax.invert_yaxis()
            (ax.set_ylabel if key else ax.set_xlabel)(title)
    for ax in np.asarray(axes).flat: ax.grid(alpha=.22)
    fig.savefig(output/"results.pdf",bbox_inches="tight")
    fig.savefig(output/"results.png",bbox_inches="tight")
    Image.open(output/"results.png").convert("L").save(output/"results_grayscale.png")
    save(output/"plot_manifest.json",dict(recipe=recipe,completed=len(rows),planned=len(plan["cases"]),
         uncertainty="none; one measurement per configuration", rows=[r["id"] for r in rows]))
    plt.close(fig)
    return output
