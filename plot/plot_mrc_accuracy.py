#! python
from re import M, S
import matplotlib.pyplot as plt
import numpy
import numpy as np
import pandas as pd
import sys
import matplotlib.ticker as ticker
import json
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utils.config as config
import matplotlib
matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

mtds = ["ccf","rarcm"]
datasets = ["msr","twitter","ycsb"]
methods = {
    'Cuki(ours)': [],
    'RAR-CM': [],
}
for dataset in datasets:
    path = "/home/atc23/wss-estimation/datasets/benchmarks/mrc-acc/ccf/" + dataset + ".log"
    with open(path,"r") as f:
        lines = f.readlines()
        mae = float(lines[-4].split("MAE: ")[1].split("%")[0])/100
        methods['Cuki(ours)'].append(mae)

for dataset in datasets:
    path = "/home/atc23/wss-estimation/datasets/benchmarks/mrc-acc/rarcm/" + dataset + ".log"
    with open(path,"r") as f:
        lines = f.readlines()
        mae = float(lines[-4].split("MAE: ")[1].split("%")[0])/100
        methods['RAR-CM'].append(mae)

print(methods)


fig_path = './plot/figs/mrc_accuracy.pdf'

def get_figsize(w, h, dpi=100):
    return [w * 0.3937008 * dpi / 100, h * 0.3937008 * dpi / 100]


def load_data2(file):
    data = numpy.array(json.load(open(file, 'r'))["data"]["result"][0]['values'])
    return data.astype(np.float_)


def transform_timestamp(data, _start_ts):
    for i in range(len(data)):
        data[i, 0] -= _start_ts
    return data


plt.rcParams['savefig.dpi'] = 300
plt.rcParams['figure.dpi'] = 300
plt.rcParams["figure.figsize"] = config.small_figsize2()
plt.rc('axes',lw=config.axes_size)

fig, ax = plt.subplots(1, 1)
bar_width = 0.3


x = np.array([i for i in range(3)])
xticks_prop={'multialignment':'right'}
plt.xticks(ticks=x, labels=  ['MSR','Twitter','YCSB'], **xticks_prop,**config.xyticks_dict)
for i in range(len(methods)):
    method = list(methods.keys())[i]
    y = methods[method]
    plt.bar(x -0.5*bar_width + bar_width*i, y, width=bar_width, label=method, alpha=0.8, color=config.colors[i], hatch=config.hatchs[i])

plt.yticks(**config.xyticks_dict)

ax.set_xlabel('Traces', **config.xylabel_fontdict)  
ax.set_ylabel('MAE', **config.xylabel_fontdict)


plt.grid(True, axis='y',linestyle='--', linewidth=0.5, alpha=0.5)

legend_properties = {'weight':'bold'}
ax.legend(loc='upper left',prop=legend_properties,framealpha=0.2)
plt.savefig(fig_path, dpi=300, bbox_inches='tight')
print(fig_path)
