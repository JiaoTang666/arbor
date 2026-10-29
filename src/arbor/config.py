"""One configuration resolver for single runs and experiment recipes."""
from copy import deepcopy
import itertools
import math
import random

import yaml

from .io import resolve_path, root

DEFAULTS = dict(method="Arbor", shards=16, nodes_per_shard=4, inject_tps=2000,
                batch_size=1000, block_size=1000, interval_ms=5000,
                half_seconds=[21600, 172800, 302400], group_size=64, tolerance=1.10,
                ablation="Full", timeout_seconds=14400, keep_db=True,
                fusion_model=str(root()/"models/fusion.json"),
                admission_model=str(root()/"models/admission.json"),
                resources=dict(min_available_gib=300, min_data_free_gib=70,
                               min_system_free_gib=100, max_owned_rss_gib=400,
                               runtime_available_floor_gib=8, offline_rss_gib=8))
RECIPES = ("demo", "full", "ablation", "halflife", "group-size", "tolerance", "scale")


def resolve(recipe, config_file=None, overrides=None):
    if recipe not in RECIPES:
        raise ValueError("unknown recipe: "+recipe)
    config = deepcopy(DEFAULTS)
    layers = [(yaml.safe_load((root()/"configs"/(recipe+".yaml")).read_text()) or {}, root())]
    if config_file:
        config_file = resolve_path(config_file)
        layers.append((yaml.safe_load(config_file.read_text()) or {}, config_file.parent))
    layers.append(({k: v for k, v in (overrides or {}).items() if v is not None}, None))
    for supplied, base in layers:
        if not isinstance(supplied, dict):
            raise ValueError("configuration must be a mapping")
        unknown = set(supplied)-set(DEFAULTS)-{"dataset"}
        if unknown:
            raise ValueError("unknown configuration keys: "+", ".join(sorted(unknown)))
        resources = supplied.get("resources", {})
        if not isinstance(resources, dict) or set(resources)-set(DEFAULTS["resources"]):
            raise ValueError("unknown resource keys")
        if any(not isinstance(v, (int, float)) or isinstance(v, bool) or not math.isfinite(v) or v < 0
               for v in resources.values()):
            raise ValueError("resource limits must be finite nonnegative numbers")
        supplied = dict(supplied)
        for name in ("dataset", "fusion_model", "admission_model"):
            if name in supplied:
                supplied[name] = str(resolve_path(supplied[name], base))
        config.update({k: v for k, v in supplied.items() if k != "resources"})
        config["resources"].update(resources)
    config["recipe"] = recipe
    config["dataset"] = str(resolve_path(config.get("dataset", root()/"data"/("demo" if recipe == "demo" else "paper"))))
    for name in ("fusion_model", "admission_model"):
        config[name] = str(resolve_path(config[name]))
    if config["method"] not in ("Arbor", "Monoxide", "CLPA"):
        raise ValueError("method must be Arbor, Monoxide or CLPA")
    for field in ("shards", "nodes_per_shard", "inject_tps", "batch_size", "block_size", "interval_ms", "group_size", "timeout_seconds"):
        if type(config[field]) is not int or config[field] <= 0:
            raise ValueError(field+" must be a positive integer")
    if config["nodes_per_shard"] < 4:
        raise ValueError("PBFT requires at least four nodes per shard in these recipes")
    if not isinstance(config["half_seconds"], (list, tuple)) or len(config["half_seconds"]) != 3 or any(
            not isinstance(x, (int, float)) or isinstance(x, bool) or not math.isfinite(x) or x <= 0
            for x in config["half_seconds"]):
        raise ValueError("three positive half-lives are required")
    if not isinstance(config["tolerance"], (int, float)) or not math.isfinite(config["tolerance"]) or config["tolerance"] < 1:
        raise ValueError("tolerance must be at least one")
    return config


def cases(config):
    recipe = config["recipe"]
    points = []
    if recipe == "scale":
        methods = ["Monoxide", "CLPA", "Arbor"]
        for ki, k in enumerate((4, 8, 12, 16)):
            for li, load in enumerate((50, 75, 100)):
                shift = (ki*3+li) % 3
                for method in methods[shift:]+methods[:shift]:
                    points.append(dict(method=method, shards=k, load_percent=load, inject_tps=200*k*load//100))
    elif recipe == "demo":
        points = [dict(method=method) for method in ("Arbor", "Monoxide", "CLPA")]
    elif recipe == "ablation":
        points = [dict(method="Arbor", ablation=arm) for arm in ("Full", "NoTime", "NoForestLayout", "NoGraphLayout", "NoAdmission")]
    elif recipe == "halflife":
        grid = list(itertools.product((3, 6, 12), (12, 24, 48), (3.5, 7, 14)))
        random.Random(43).shuffle(grid)
        points = [dict(half_seconds=[int(s*3600), int(m*3600), int(l*86400)]) for s, m, l in grid]
    elif recipe == "group-size":
        points = [dict(group_size=b) for b in (16, 32, 64)]
    elif recipe == "tolerance":
        points = [dict(tolerance=e) for e in (1.05, 1.10, 1.20)]
    else:
        points = [{}]
    output = []
    for i, point in enumerate(points, 1):
        case = deepcopy(config)
        case.update(point)
        case["id"] = f"{i:02d}_{case['method']}_K{case['shards']}_R{case['inject_tps']}"
        output.append(case)
    return output
