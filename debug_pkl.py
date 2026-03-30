import time
import pandas as pd
t = time.time()
df = pd.read_pickle('/home/lc999/data/rl_backtest/000001.SZ.pkl')
print(f'shape: {df.shape}')
print(f'load time: {time.time()-t:.1f}s')
print(f'mem: {df.memory_usage(deep=True).sum()//1024//1024}MB')
print(f'index type: {type(df.index)}')
if hasattr(df.index, "names"): print(f'index names: {df.index.names}')
