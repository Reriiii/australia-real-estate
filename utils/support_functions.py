import numpy as np
import re

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c

def clip_01(s, q=(0.01,0.99), log=False):
    x = np.log10(s) if log else s
    lo, hi = x.quantile(q)
    x = x.clip(lo, hi)
    if log: return (x - x.min()) / (x.max() - x.min())
    return (x - lo) / (hi - lo)

def slug(s): return re.sub(r"[^a-zA-Z0-9]+","_", str(s)).strip("_")