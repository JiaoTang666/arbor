import numpy as np
import pytest

from arbor.forest.source import SourceForest
from arbor.forest.adapter import SlowForestAdapter


def test_binding_order_maturity_and_checkpoint(tmp_path):
    f = SourceForest(2)
    for seq, (a,b,t) in enumerate([(0,1,0),(0,2,1),(3,2,2),(4,4,2),(3,4,3)]):
        f.observe(a,b,t,seq)
    np.testing.assert_array_equal(f.parent[:5], [-1,-1,0,2,-1])
    before = f.followup.copy()
    f.save(tmp_path/"seed")
    copy = SourceForest.load(tmp_path/"seed")
    copy.advance(3602)
    f.advance(3602)
    np.testing.assert_array_equal(copy.mass[:copy.n], f.mass[:f.n])
    np.testing.assert_array_equal(copy.followup[:copy.n], before[:f.n])
    with pytest.raises(ValueError, match="non-contiguous"):
        f.observe(0,2,3603,7)


def test_adapter_keeps_existing_ownership():
    f = SourceForest()
    f.observe(0,1,0,0)
    adapter = SlowForestAdapter(f, np.array([0,1]), 4)
    with pytest.raises(ValueError, match="route changed"):
        adapter.observe(0,2,1,1,3,3)
