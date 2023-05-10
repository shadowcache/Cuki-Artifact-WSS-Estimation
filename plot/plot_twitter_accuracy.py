import matplotlib.pyplot as plt
import matplotlib
import pandas as pd
import numpy as np
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utils.config as config

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

#./datasets/benchmarks/memory-bmc/accuracy/twitter/summary.csv
path="./datasets/benchmarks/"
bmc_file="./datasets/benchmarks/memory-bmc/accuracy/twitter/summary.csv"
ccf_file="./datasets/benchmarks/memory-ccf/accuracy/twitter/summary_false.csv"
ccfoa_file="./datasets/benchmarks/memory-ccf/accuracy/twitter/summary_true.csv"
mbf_file="./datasets/benchmarks/memory-mbf/accuracy/twitter/summary.csv"
ss_file="./datasets/benchmarks/memory-ss/accuracy/twitter/summary.csv"
swamp_file="./datasets/benchmarks/swamp/accuracy/twitter/summary.csv"

files=[ccf_file,ccfoa_file,mbf_file,bmc_file,swamp_file,ss_file]
methods=['Cuki', 'Cuki(oa)', 'MBF', 'ClockSketch', 'SWAMP', 'SlidingSketch']
df = pd.DataFrame(columns=['Memory', 'Cuki', 'Cuki(oa)', 'MBF', 'ClockSketch', 'SWAMP', 'SlidingSketch'])
for i in range(0,6):
    csv_df = pd.read_csv(files[i],header=None,names=["Memory",methods[i]])
    df[methods[i]] = csv_df[methods[i]].str.replace('%', '').astype(float) / 100
    df["Memory"] = csv_df["Memory"].str.replace('kb','')
    df[['Memory', methods[i]]] = df[['Memory', methods[i]]].apply(pd.to_numeric)
    

fig, ax = plt.subplots(1, 1)
plt.yticks(**config.xyticks_dict)
memoryList = df['Memory'].values
ticks =  np.array([i for i in range(len(memoryList))])
ticks[-1] = ticks[-1] + 0.5
ax.set_xticks(ticks)
ax.set_xticklabels(memoryList, **config.xyticks_dict) 
for method,conf in config.method2config.items():
    if method=='Cuki-OA':
        method='Cuki(oa)'
    ax.plot(ticks, df[method].values, lw=2, markersize=8, **conf)
ax.grid(True)
ax.set_yscale('log')
ax.set_xlabel('Memory (KB)', **config.xylabel_fontdict)   
ax.set_ylabel('ARE', **config.xylabel_fontdict)
plt.legend()
plt.savefig('./plot/figs/twitter_accuracy.pdf', bbox_inches='tight')
print('./plot/figs/twitter_accuracy.pdf')