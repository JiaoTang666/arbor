import numpy as np
import pytest

from arbor.build import ensure_build
from arbor.history.grouping import aggregate_groups, groups
from arbor.history.kernel import Kernel
from arbor.history.relations import cohort_credit


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
