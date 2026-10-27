"""Compare frozen Python scores, candidate RNG and selection with the Go implementation."""
import os
from pathlib import Path
import subprocess
import tempfile

import numpy as np

from arbor.admission.candidates import sample_candidates
from arbor.admission.model import choose, scores
from arbor.admission.pressure import proportions
from arbor.build import go_tool
from arbor.io import load, root, save


def main():
    model=load(root()/"models/admission.json")
    rng=np.random.default_rng(43)
    rows=[]
    for ordinal in range(32):
        raw=rng.normal(size=(16,20))
        pressure=rng.integers(0,10000,size=16).astype(float)
        probability,_,_=proportions(pressure)
        context="arbor-public-parity"
        candidates,uniforms=sample_candidates(probability,context,ordinal)
        rows.append(dict(raw=raw,pressure=pressure,probabilities=probability,candidates=candidates,
                         uniforms=uniforms,scores=scores(model,raw),action=choose(model,raw,candidates),
                         context=context,ordinal=ordinal))
    with tempfile.TemporaryDirectory() as directory:
        fixture=Path(directory)/"parity.json"
        save(fixture,dict(model=model,cases=rows))
        subprocess.run([go_tool(),"test","-p","2","./supervisor/committee","-run","^TestCostRankerExportParity$","-count=1"],
                       cwd=root()/"native/engine",env=dict(os.environ,SAA_COST_PARITY_FIXTURE=str(fixture)),check=True)
    print("PASS: 32 frozen-model Python/Go score, candidate and action comparisons")


if __name__ == "__main__": main()
