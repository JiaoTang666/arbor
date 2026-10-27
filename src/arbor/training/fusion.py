"""Three-scale convex mixture fitting."""
import numpy as np
from scipy.optimize import minimize

def fit_simplex(samples,prior=None,penalty=0.01):
    """Convex count quasi-likelihood; three coefficients, shared across edges."""
    count=sum(len(y) for x,y in samples)
    target=sum(float(y.sum()) for x,y in samples)
    norm=max(target,1.)
    center=np.ones(3)/3 if prior is None else np.asarray(prior)
    lam=0. if prior is None else penalty
    def evaluate(a):
        value=0.;gradient=np.zeros(3)
        for x,y in samples:
            pred=x@a+1e-12
            value+=float(np.sum(pred-y*np.log(pred)))
            gradient+=x.T@(1-y/pred)
        diff=a-center
        return value/norm+lam*(diff@diff),gradient/norm+2*lam*diff
    result=minimize(evaluate,center,jac=True,method='SLSQP',bounds=[(0,1)]*3,
        constraints=[dict(type='eq',fun=lambda a:a.sum()-1,jac=lambda a:np.ones(3))],
        options=dict(maxiter=100,ftol=1e-10))
    if not result.success:raise RuntimeError('simplex optimizer: '+result.message)
    a=np.maximum(result.x,0);a/=a.sum()
    return a,dict(rows=count,target_sum=target,iterations=int(result.nit),objective=float(result.fun),
                  success=True,regularization=lam,weights=a.tolist())

def prediction_metrics(pred,y):
    positive=y>0
    dev=pred-y
    dev[positive]+=y[positive]*np.log(y[positive]/np.maximum(pred[positive],1e-12))
    k=min(10000,len(y));top=np.lexsort((np.arange(len(pred)),-pred))[:k]
    return dict(poisson_deviance_per_edge=float(2*dev.mean()),mae=float(np.abs(pred-y).mean()),
                top10000_captured=float(y[top].sum()/y.sum()) if y.sum() else 0,
                predicted_mass=float(pred.sum()),actual_known_mass=float(y.sum()))