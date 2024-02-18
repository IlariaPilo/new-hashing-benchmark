import json
import sys
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import numpy as np
import os
import re
import functools as ft
from math import log10

plt.rcParams['figure.constrained_layout.use'] = True
# ax.grid(which='major', color=, linewidth=0
plt.rcParams['grid.color'] = '#DDDDDD'
plt.rcParams['grid.linewidth'] = 0.8

# =============================== SETUP =============================== #

# Check if the filename argument is provided
if len(sys.argv) < 2:
    print("\n\033[1;96m\tpython print_figures.py <result.json OR result.csv> \033[0m")
    print("Produces different types of figures, given a <result.json OR result.csv> file.\n")
    sys.exit(1)

script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)
BASE_DIR = os.path.abspath(f'{script_directory}/..')+'/'

# Get the filename from the command-line argument
file_path = sys.argv[1]
prefix, ext = os.path.splitext(os.path.basename(file_path))

prefix = BASE_DIR + 'figs/' + prefix

# ============================= UTILITIES ============================= #

SHAPES_FN = {
    'RMI': 's',
    'PGM': 's',
    'Murmur': 'D',
    'BitMWHC': 'o',
    'RadixSpline': 's',
    'MWHC': 'o',
    'RecSplit': 'o',
    'AquaHash': 'D',
    'XXHash': 'D',
    'MultiplyPrime': 'D',
    'FibonacciPrime': 'D'
}
SHAPES_STRUCT = {
    'RMI-Chain': 's',
    'RadixSpline-Chain': 's',
    'RMI-Sort': 'D'
}
SHAPES_DS = {
    'gap_10': '^',
    'uniform': 'D',
    'fb': 's',
    'osm': '<',
    'wiki': 'o',
    'normal': '>',
    'variance_x2': '^',
    'variance_x4': 'v',
    'variance_half': '<',
    'variance_quarter': '>'
}
F_MAP = {
    'Breakeven': -1,
    'RMI': 0,
    'RadixSpline': 1,
    'PGM': 2,
    'Murmur': 3,
    'MultiplyPrime': 4,
    'FibonacciPrime': 5,
    'XXHash': 6,
    'AquaHash': 7,
    'MWHC': 8,
    'BitMWHC': 9,
    'RecSplit': 10
}
LAB_MAP = {
    'RMI-CHAIN': 0,
    'MULT-CHAIN': 3,
    'MWHC-CHAIN': 6,
    'RMI-LINEAR': 1,
    'MULT-LINEAR': 4,
    'MWHC-LINEAR': 7,
    'RMI-LP': 1,
    'MULT-LP': 4,
    'MWHC-LP': 7,
    'RMI-CUCKOO': 2,
    'MULT-CUCKOO': 5,
    'MWHC-CUCKOO': 8
}
    
COLORS = {}
COLORS_STRUCT = {}

CHAINED = 0
LINEAR = 1
CUCKOO = 2  

def get_fn_name(label):
    label = label.lower()
    if 'coro_rmi' in label:
        return 'RMICoro'
    if 'rmi' in label:
        return 'RMI'
    if 'pgm' in label:
        return 'PGM'
    if 'murmur' in label:
        return 'Murmur'
    if 'bitmwhc' in label:
        return 'BitMWHC'
    if 'radix_spline' in label:
        return 'RadixSpline'
    if 'mwhc' in label:
        return 'MWHC'
    if 'recsplit' in label:
        return 'RecSplit'
    if 'aqua' in label:
        return 'AquaHash'
    if 'xxh' in label:
        return 'XXHash'
    if 'multhash' in label:
        return 'MultiplyPrime'
    if 'fibonacci' in label:
        return 'FibonacciPrime'
    return None
  
def get_table_type(label):
    label = label.lower()
    if 'linear' in label:
        return 'linear'
    if 'chained' in label:
        return 'chained'
    if 'cuckoo' in label:
        return 'cuckoo'
    
def get_table_label(function, table):
    if function == 'MultiplyPrime':
        function = 'mult'
    if table == 'linear':
        table = 'lp'
    if table == 'chained':
        table = 'chain'
    return f'{function}-{table}'

def get_struct_type(label):
    label = label.lower()
    if 'sort' in label:
        return 'RMI-Sort'
    if 'radix' in label:
        return 'RadixSpline-Chain'
    if 'rmi' in label:
        return 'RMI-Chain'
    
def get_rmi_models(label):
    pattern = r'rmi_hash_(\d+)(?::|$)'
    match = re.search(pattern, label)
    try:
        models = int(match.group(1))
    except:
        models = 0
    return models

def prepare_fn_colormap():
    functions = ['RMI','RadixSpline','PGM','Murmur','MultiplyPrime','FibonacciPrime','XXHash','AquaHash','MWHC','BitMWHC','RecSplit'][::-1]
    cmap = plt.get_cmap('turbo')
    # Create a dictionary of unique colors based on the number of labels
    col_dict = {functions[f_i]: cmap(col_i) for f_i, col_i in enumerate(np.linspace(0, 1, len(functions)))}
    col_struct = {'RMI-Chain': col_dict['RMI'], 'RadixSpline-Chain': col_dict['PGM'], 'RMI-Sort': col_dict['BitMWHC']}
    return col_dict, col_struct

def groupby_helper(df, static_cols, variable_cols):
    return df.groupby(static_cols)[variable_cols].mean().reset_index()

def sort_labels(labels, handles, map = F_MAP):
    pairs = list(zip(labels, handles))
    sorted_pairs = sorted(pairs, key=lambda pair: map[pair[0]])
    return zip(*sorted_pairs)

# ===================================================================== #

# ------- collision plot ------- #
def collisions(df):
    datasets = ['gap_10','uniform','wiki','fb']
    df = df[df["label"].str.lower().str.contains("collision")].copy(deep=True)
    if df.empty:
        return
    # Back-compatibility
    if 'load_factor_%' in df.columns:
        df = df[df['load_factor_%']==0]
        if df.empty:
            return
    # Group by
    df = groupby_helper(df, ['dataset_name','label','function'], ['dataset_size','collisions','tot_time_s'])
    df['throughput_M'] = df.apply(lambda x : x['dataset_size']/(x['tot_time_s']*10**6), axis=1)
    df = df.sort_values(by='throughput_M')
    
    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    fig, axes = plt.subplots(1, num_subplots, figsize=(8.5, 2))  # Adjust figsize as needed
    
    # Create a single legend for all subplots
    legend_labels = []
    
    # Iterate through the groups and create subplots
    i = 0
    for name_ds in datasets:
        group_ds = df[df['dataset_name']==name_ds]
        g_fn = group_ds.groupby('function')
        ax = axes[i]
        i += 1
        
        # Plot the data for the current group
        for name_fs, group_fn in g_fn:
            # Plot the data and collect labels for the legend
            ax.plot(group_fn['throughput_M'], group_fn['collisions']/group_fn['dataset_size'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            if name_fs not in legend_labels:
                legend_labels.append(name_fs)
        
        # Customize the plot
        ax.set_title(f'{name_ds}')
        # Format the y-axis to display only two digits after the decimal point
        ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
        ax.set_ylim([0, 1])
        #ax.set_xlim([0, 150])
        ax.grid(True)
    
    labels, handles = sort_labels(legend_labels, [line for line in fig.axes[0].lines])
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=labels, ncol=len(legend_labels)//2+len(legend_labels)%2, bbox_to_anchor=(0.5, 1.28))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Hash Computation Throughput (Million operations/s)')
    laby = fig.supylabel('Ratio of Colliding Keys')

    #plt.show()
    fig.savefig(f'{prefix}_collisions.png', bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

# ------- collision RMI plot ------- #
def collisions_rmi(df):
    datasets = ['gap_10','uniform','wiki','fb']
    df = df[df["label"].str.lower().str.contains("collision")].copy(deep=True)
    df = df[df["function"] == "RMI"]
    if df.empty:
        return
    # Back-compatibility
    if 'load_factor_%' in df.columns:
        df = df[df['load_factor_%']==0]
        if df.empty:
            return
    # Group by
    df = groupby_helper(df, ['dataset_name','label','function'], ['dataset_size','collisions','tot_time_s'])
    df["models"] = df["label"].apply(lambda x : get_rmi_models(x))
    df = df.sort_values(by='models')
 
    fig, axes = plt.subplots(1, len(datasets), figsize=(8.5, 2))

    for i,name_ds in enumerate(datasets):
        group_ds = df[df['dataset_name']==name_ds]
        g_fn = group_ds.groupby('function')
        ax = axes[i]

        ticks = group_ds['models'].apply(lambda x : log10(x))
        ax.plot(ticks, group_ds['collisions']/group_ds['dataset_size'], color=COLORS['RMI'], marker=SHAPES_FN['RMI'])
        
        # Customize the plot
        ax.set_title(f'{name_ds}')
        tick_labels = [f"$10^{int(tick)}$" for tick in ticks]
        ax.set_xticks(ticks[1::2], tick_labels[1::2])
        ax.set_yticks([0,0.25,0.5,0.75,1],['0','0.25','0.5','0.75','1'])
        # Format the y-axis to display only two digits after the decimal point
        ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
        ax.set_ylim([0, 1.05])
        ax.grid(True)

    # Set a common label for x and y axes
    labx = fig.supxlabel('RMI number of submodels')
    laby = fig.supylabel('Ratio of\nColliding Keys', va='center', ha='center')

    fig.savefig(f'{prefix}_collisions_RMI.png', bbox_extra_artists=(labx,laby,), bbox_inches='tight')

# ------- gaps plot ------- #
def gaps(df):
    df = df[df["label"].str.lower().str.contains("gaps")].copy(deep=True)
    if df.empty:
        return
    fig = plt.figure(figsize=(4, 3))
    datasets = df['dataset_name'].unique()
    for _d_ in datasets:
        if _d_ == 'seq':
            continue
        _df_ = df[df['dataset_name'] == _d_]
        # Assert that the DataFrame has only one row
        assert _df_.shape[0] == 1
        y = _df_['count'].values[0]
        y = y[:min(len(y)+1,6)]
        x = np.arange(min(len(y),6))
        plt.plot(x, y, marker=SHAPES_DS[_d_], markersize=4, label=_d_)

    plt.xlabel('Gap value')
    plt.ylabel('Count')
    plt.grid(True)
    fig.legend(loc='upper right', bbox_to_anchor=(0.995, 0.945))
    fig.savefig(f'{prefix}_gaps.png')

# ------- probe ------- #
def probe(df, pareto=False):
    datasets = ['gap_10','normal','wiki','osm','fb']
    df = df[df["label"].str.lower().str.contains("probe")].copy(deep=True)
    if df.empty:
        return
    # remove failed experiments
    df = df[df["insert_fail_message"]=='']
    # check we are not in the high load factor case
    df = df[df['load_factor_%']<1000]
    if df.empty:
        return

    # Back-compatibility
    if 'probe_type' in df.columns:
        # filter for probe distribution type
        probe_type = '80-20' if pareto else 'uniform'
        df = df[df['probe_type']==probe_type]
        if df.empty:
            return
    elif pareto:
        return
    
    # Group by
    df = groupby_helper(df, ['dataset_name','label','function','load_factor_%'], ['dataset_size','probe_elem_count','tot_time_probe_s'])
    df['throughput_M'] = df.apply(lambda x : x['probe_elem_count']/(x['tot_time_probe_s']*10**6), axis=1)
    df['table_type'] = df['label'].apply(lambda x : get_table_type(x))
    df = df.sort_values(by='load_factor_%')
    
    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    fig, axes = plt.subplots(3, num_subplots, figsize=(10, 5))  # Adjust figsize as needed
    
    # Create a single legend for all subplots
    legend_labels = []
    
    i = 0
    # for each dataset
    for name_ds in datasets:
        group_ds = df[df['dataset_name']==name_ds]
        g_fn = group_ds.groupby('function')
        ax = axes[:,i]
        i += 1
        
        # for each function
        for name_fs, group_fn in g_fn:
            # Chained
            chain = group_fn[group_fn['table_type'] == 'chained']
            ax[CHAINED].plot(chain['load_factor_%'], chain['throughput_M'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            # Linear
            lin = group_fn[group_fn['table_type'] == 'linear']
            ax[LINEAR].plot(lin['load_factor_%'], lin['throughput_M'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            # Cuckoo
            cuck = group_fn[group_fn['table_type'] == 'cuckoo']
            ax[CUCKOO].plot(cuck['load_factor_%'], cuck['throughput_M'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            if name_fs not in legend_labels:
                legend_labels.append(name_fs)
        
        # Customize the plot
        ax[CHAINED].set_title(f'{name_ds}')
        # Format the y-axis to display only two digits after the decimal point
        # ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
        # ax.set_ylim([0, 1])
        #ax.set_xlim([0, 150])
        ax[CHAINED].set_xlim([0,200])
        ax[LINEAR].set_xlim([20,80])
        ax[CUCKOO].set_xlim([70,100])
        ax[CHAINED].set_xticks([0,50,100,150,200], ['0','50','100','150','200'])
        ax[LINEAR].set_xticks([20,35,50,65,80], ['20','35','50','65','80'])
        ax[CUCKOO].set_xticks([70,85,100], ['70', '85','100'])

        ax[CHAINED].set_ylim([0,8])
        ax[LINEAR].set_ylim([0,8])
        ax[CUCKOO].set_ylim([0,8])
        ax[CHAINED].set_yticks([0,2,4,6,8], ['','','','',''])
        ax[LINEAR].set_yticks([0,2,4,6,8], ['','','','',''])
        ax[CUCKOO].set_yticks([0,2,4,6,8], ['','','','',''])

        ax[CHAINED].grid(True)
        ax[LINEAR].grid(True)
        ax[CUCKOO].grid(True)
    
    axes[CHAINED,0].set_ylabel('(A)', rotation=0, ha='right', va="center")
    axes[LINEAR,0].set_ylabel('(B)', rotation=0, ha='right', va="center")
    axes[CUCKOO,0].set_ylabel('(C)', rotation=0, ha='right', va="center")
    axes[CHAINED,0].set_yticks([0,2,4,6,8], ['0','2','4','6','8'])
    axes[LINEAR,0].set_yticks([0,2,4,6,8], ['0','2','4','6','8'])
    axes[CUCKOO,0].set_yticks([0,2,4,6,8], ['0','2','4','6','8'])

    labels, handles = sort_labels(legend_labels, [line for line in fig.axes[0].lines])
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=labels, ncol=len(legend_labels)//2+len(legend_labels)%2, bbox_to_anchor=(0.5, 1.12))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Load Factor (%)')
    laby = fig.supylabel('Probe Throughput (Million operations/s)')

    #plt.show()
    name = prefix + '_probe' + ('_pareto.png' if pareto else '.png')
    fig.savefig(name, bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

# ------- probe ------- #
def probe_high_lf(df, pareto=False):
    datasets = ['gap_10','normal','wiki','osm','fb']
    df = df[df["label"].str.lower().str.contains("probe")].copy(deep=True)
    if df.empty:
        return
    # remove failed experiments
    df = df[df["insert_fail_message"]=='']
    # time to split!
    df = df[df['load_factor_%']>=1000]
    if df.empty:
        return
    
    # Back-compatibility
    if 'probe_type' in df.columns:
        # filter for probe distribution type
        probe_type = '80-20' if pareto else 'uniform'
        df = df[df['probe_type']==probe_type]
        if df.empty:
            return
    elif pareto:
        return
    
    # Group by
    df = groupby_helper(df, ['dataset_name','label','function','load_factor_%'], ['dataset_size','probe_elem_count','tot_time_probe_s'])
    df['throughput_M'] = df.apply(lambda x : x['probe_elem_count']/(x['tot_time_probe_s']*10**6), axis=1)
    df['table_type'] = df['label'].apply(lambda x : get_table_type(x))
    # get only chained
    df = df[df['table_type']=='chained']
    df['load_factor'] = df['load_factor_%']/100
    df = df.sort_values(by='load_factor')
    
    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    fig, axes = plt.subplots(1, num_subplots, figsize=(10.5, 2))  # Adjust figsize as needed
    # Create a single legend for all subplots
    legend_labels = []
    lf = df['load_factor'].unique()
    
    # for each dataset
    for i,name_ds in enumerate(datasets):
        group_ds = df[df['dataset_name']==name_ds]
        g_fn = group_ds.groupby('function')
        ax = axes[i]
        
        # for each function
        for name_fs, group_fn in g_fn:
            ax.plot(group_fn['load_factor'], group_fn['throughput_M'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            if name_fs not in legend_labels:
                legend_labels.append(name_fs)
        
        # Customize the plot
        ax.set_title(f'{name_ds}')
        #ax.set_xscale('log')
        ax.set_xticks(lf, [f'{int(_lf_)}' for _lf_ in lf])
        
        ax.set_ylim([0,1.55])
        ax.set_yticks([0,0.5,1,1.5], ['','','',''])
        ax.grid(True)
        
    axes[0].set_yticks([0,0.5,1,1.5], ['0','0.5','1','1.5'])

    labels, handles = sort_labels(legend_labels, [line for line in fig.axes[0].lines])
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.18))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Load Factor')
    laby = fig.supylabel('Probe Throughput\n(Million operations/s)', ha='center', va="center")

    name = prefix + '_probe_high_lf' + ('_pareto.png' if pareto else '.png')
    fig.savefig(name, bbox_extra_artists=(lgd,labx,laby), bbox_inches='tight')

def probe_all_lf(df, pareto=False):
    datasets = ['gap_10','wiki','fb','osm','normal']
    ylims = [[0,8],[0,8],[0,6],[0,6],[0,6]]
    yticks = [[0,2,4,6,8],[0,2,4,6,8],[0,2,4,6],[0,2,4,6],[0,2,4,6]]
    df = df[df["label"].str.lower().str.contains("probe")].copy(deep=True)
    if df.empty:
        return
    # remove failed experiments
    df = df[df["insert_fail_message"]=='']
    # time to split!
    df_high = df[df['load_factor_%']>=1000]
    df_low = df[df['load_factor_%']<1000]
    if df_high.empty or df_low.empty:
        return
    
    # Back-compatibility
    if 'probe_type' in df.columns:
        # filter for probe distribution type
        probe_type = '80-20' if pareto else 'uniform'
        df = df[df['probe_type']==probe_type]
        if df.empty:
            return
    elif pareto:
        return
    
    # Group by
    df = groupby_helper(df, ['dataset_name','label','function','load_factor_%'], ['dataset_size','probe_elem_count','tot_time_probe_s'])
    df['throughput_M'] = df.apply(lambda x : x['probe_elem_count']/(x['tot_time_probe_s']*10**6), axis=1)
    df['table_type'] = df['label'].apply(lambda x : get_table_type(x))
    # get only chained tables
    df = df[df['table_type']=='chained']
    df['load_factor'] = df['load_factor_%']/100
    df = df.sort_values(by='load_factor')

    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    num_rows = num_subplots//2 + num_subplots%2
    is_odd = (num_subplots%2)==1

    fig = plt.figure(figsize=(7,6))
    gs = gridspec.GridSpec(num_rows*2,4, figure=fig)
    
    # Create a single legend for all subplots
    legend_labels = []
    
    # for each dataset
    for i,name_ds in enumerate(datasets):
        group_ds = df[df['dataset_name']==name_ds]
        g_fn = group_ds.groupby('function')
        ax = []
        # if it's the last and they are odd
        if i==(num_subplots-1) and is_odd:
            ax = fig.add_subplot(gs[(i//2)*2:(i//2*2+2), 1:3])
        else: 
            ax = fig.add_subplot(gs[(i//2)*2:(i//2*2+2), (i%2)*2:(i%2*2+2)])
        
        # for each function
        for name_fs, group_fn in g_fn:
            ax.plot(group_fn['load_factor'], group_fn['throughput_M'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            if name_fs not in legend_labels:
                legend_labels.append(name_fs)
        
        # Customize the plot
        ax.set_title(f'{name_ds}')
        ax.set_xscale('log')
        ax.set_ylim(ylims[i])
        ax.set_yticks(yticks[i], [f'{int(t)}' for t in yticks[i]])
        ax.set_xticks([.25,.5,1,2,10,20,50,100],['.25','.5','1','2','10','20','50','100'])
        ax.grid(True)
    labels, handles = sort_labels(legend_labels, [line for line in fig.axes[0].lines])
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.06))
    # Set a common label for x and y axes
    labx = fig.supxlabel('Load Factor')
    laby = fig.supylabel('Probe Throughput (Million operations/s)')
    name = prefix + '_probe_all_lf' + ('_pareto.png' if pareto else '.png')
    fig.savefig(name, bbox_extra_artists=(lgd,labx,laby), bbox_inches='tight')

# ------- insert ------- #
def insert(df, pareto=False):
    datasets = ['wiki','fb']
    df = df[df["label"].str.lower().str.contains("probe")].copy(deep=True)
    if df.empty:
        return
    # remove failed experiments
    df = df[df["insert_fail_message"]=='']
    # check we are not in the high load factor case
    df = df[df['load_factor_%']<1000]
    if df.empty:
        return

    # Back-compatibility
    if 'probe_type' in df.columns:
        # filter for probe distribution type
        probe_type = '80-20' if pareto else 'uniform'
        df = df[df['probe_type']==probe_type]
        if df.empty:
            return
    elif pareto:
        return

    # Group by
    df = groupby_helper(df, ['dataset_name','label','function','load_factor_%'], ['dataset_size','insert_elem_count','tot_time_insert_s'])
    df['throughput_M'] = df.apply(lambda x : x['insert_elem_count']/(x['tot_time_insert_s']*10**6), axis=1)
    df['table_type'] = df['label'].apply(lambda x : get_table_type(x))
    df = df.sort_values(by='load_factor_%')
    
    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    fig, axes = plt.subplots(3, num_subplots, figsize=(4.5, 5))  # Adjust figsize as needed
    
    # Create a single legend for all subplots
    legend_labels = []
    
    i = 0
    for name_ds in datasets:
        group_ds = df[df['dataset_name']==name_ds]
        g_fn = group_ds.groupby('function')
        ax = axes[:,i]
        i += 1
        
        # for each function
        for name_fs, group_fn in g_fn:
            # Chained
            chain = group_fn[group_fn['table_type'] == 'chained']
            ax[CHAINED].plot(chain['load_factor_%'], chain['throughput_M'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            # Linear
            lin = group_fn[group_fn['table_type'] == 'linear']
            ax[LINEAR].plot(lin['load_factor_%'], lin['throughput_M'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            # Cuckoo
            cuck = group_fn[group_fn['table_type'] == 'cuckoo']
            ax[CUCKOO].plot(cuck['load_factor_%'], cuck['throughput_M'], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            if name_fs not in legend_labels:
                legend_labels.append(name_fs)
        
        # Customize the plot
        ax[CHAINED].set_title(f'{name_ds}')
        # Format the y-axis to display only two digits after the decimal point
        # ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
        # ax.set_ylim([0, 1])
        #ax.set_xlim([0, 150])
        ax[CHAINED].set_xlim([0,200])
        ax[LINEAR].set_xlim([20,80])
        ax[CUCKOO].set_xlim([70,100])
        ax[CHAINED].set_xticks([0,50,100,150,200], ['0','50','100','150','200'])
        ax[LINEAR].set_xticks([20,35,50,65,80], ['20','35','50','65','80'])
        ax[CUCKOO].set_xticks([70,85,100], ['70', '85','100'])

        ax[CHAINED].set_ylim([0,8])
        ax[LINEAR].set_ylim([0,8])
        ax[CUCKOO].set_ylim([0,8])
        ax[CHAINED].set_yticks([0,2,4,6,8], ['','','','',''])
        ax[LINEAR].set_yticks([0,2,4,6,8], ['','','','',''])
        ax[CUCKOO].set_yticks([0,2,4,6,8], ['','','','',''])

        ax[CHAINED].grid(True)
        ax[LINEAR].grid(True)
        ax[CUCKOO].grid(True)
    
    axes[CHAINED,0].set_ylabel('(A)', rotation=0, ha='right', va="center")
    axes[LINEAR,0].set_ylabel('(B)', rotation=0, ha='right', va="center")
    axes[CUCKOO,0].set_ylabel('(C)', rotation=0, ha='right', va="center")
    axes[CHAINED,0].set_yticks([0,2,4,6,8], ['0','2','4','6','8'])
    axes[LINEAR,0].set_yticks([0,2,4,6,8], ['0','2','4','6','8'])
    axes[CUCKOO,0].set_yticks([0,2,4,6,8], ['0','2','4','6','8'])

    labels, handles = sort_labels(legend_labels, [line for line in fig.axes[0].lines])
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=labels, ncol=len(legend_labels)//2+len(legend_labels)%2, bbox_to_anchor=(0.5, 1.12))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Load Factor (%)')
    laby = fig.supylabel('Insert Throughput (Million operations/s)')

    #plt.show()
    name = prefix + '_insert' + ('_pareto.png' if pareto else '.png')
    fig.savefig(name, bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

# ------- build plot ------- #
def build(df):
    df = df[df["label"].str.lower().str.contains("build")].copy(deep=True)
    if df.empty:
        return
    # Group by
    df = groupby_helper(df, ['actual_size','dataset_name', 'function', 'label'], ['build_time_s'])
    fig = plt.figure(figsize=(3, 2))
    functions = df['function'].unique()
    for _f_ in functions:
        _df_ = df[df['function'] == _f_]
        plt.plot(_df_['actual_size'], _df_['build_time_s'], color=COLORS[_f_], marker=SHAPES_FN[_f_], markersize=4, label=_f_)

    plt.xscale('log')
    plt.yscale('log')
    labx = plt.xlabel('Dataset Size (number of entries)')
    laby = plt.ylabel('Build Time (s)')

    labels, handles = sort_labels(functions, [line for line in fig.axes[0].lines])
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=labels, ncol=len(functions)//2+len(functions)%2, bbox_to_anchor=(0.5, 1.3))
    plt.grid(True)
    fig.savefig(f'{prefix}_build.png', bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

# ------- distribution plot ------- #
def distribution(df):
    df = df[df["label"].str.lower().str.contains("collision")].copy(deep=True)
    if df.empty:
        return
    # Back-compatibility
    if 'load_factor_%' not in df.columns:
        return
    df = df[df['load_factor_%']!=0]
    if df.empty:
        return
    # Group by
    df = groupby_helper(df, ['dataset_name','label','function','load_factor_%'], ['dataset_size','collisions','tot_time_s'])
    df = df.sort_values(by='load_factor_%')
    fig = plt.figure(figsize=(3, 2))
    datasets = df['dataset_name'].unique()
    for _d_ in datasets:
        _df_ = df[df['dataset_name'] == _d_]
        plt.plot(_df_['load_factor_%'], _df_['collisions']/_df_['dataset_size'], marker=SHAPES_DS[_d_], markersize=4, label=_d_)

    labx = plt.xlabel('Load Factor (%)')
    laby = plt.ylabel('Ratio of Colliding Keys')
    plt.gca().yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
    lgd = fig.legend(loc='upper center', labels=datasets, ncol=2, bbox_to_anchor=(0.5, 1.4))
    plt.grid(True)
    plt.ylim([0, 1])
    plt.yticks([0,0.25,0.50,0.75,1], ['0.00','0.25','0.50','0.75','1.00']) 
    plt.xlim([0, 100])
    xticks = df['load_factor_%'].unique()
    xticks_lab = [f'{x}' for x in xticks]
    plt.xticks(xticks,xticks_lab) 
    fig.savefig(f'{prefix}_distribution.png', bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

# -------- perf probe -------- # 
def perf_probe(df, no_mwhc=False, pareto=False):
    # Some definitions
    GAP10 = 0
    FB = 1
    counters = ['L1-misses', 'LLC-misses']
    counters_label = ['L1 misses', 'LLC misses']

    df = df.copy(deep=True)
    df['label'] = df.apply(lambda x : f"{x['function']}-{x['table']}".upper(), axis=1)
    
    def get_map(x):
        if isinstance(x, pd.Series):
            return x.apply(lambda i : LAB_MAP[i])
        return LAB_MAP[x]
    df = df.sort_values(by='label', key=lambda l: get_map(l))

    # Prepare the colors
    labels = df['label'].unique()
    cmap = plt.get_cmap('coolwarm')
    # Create a dictionary of unique colors based on the number of labels
    colors = {labels[-(lab_i+1)]: cmap(col_i) for lab_i, col_i in enumerate(np.linspace(0, 1, len(labels)))}

    if no_mwhc:
        df = df[df['function']!='mwhc']
    # Back-compatibility
    if 'probe' in df.columns:
        # filter for probe distribution type
        probe_type = '80-20' if pareto else 'uniform'
        df = df[df['probe']==probe_type]
        if df.empty:
            return
    elif pareto:
        return
    
    # average
    df = groupby_helper(df, ['function','table','dataset','label'], ['cycles','kcycles','instructions','L1-misses','LLC-misses','branch-misses','task-clock','scale','IPC','CPUs','GHz'])
    # Create a single figure with multiple subplots in a row
    num_subplots = len(counters)
    fig, axes = plt.subplots(2, num_subplots, figsize=(5, 5))  # Adjust figsize as needed
    
    # Create a single legend for all subplots
    legend_labels = []
    handles = []

    # For each label
    for name_lab in labels:
        group_lab = df[df['label']==name_lab]
        legend_labels.append(name_lab)
        gap10 = group_lab[group_lab['dataset']=='gap10']
        fb = group_lab[group_lab['dataset']=='fb']
        # For each counter
        for i, count in enumerate(counters):
            ax = axes[:,i]
            # gap10
            h = ax[GAP10].bar(gap10['label'], gap10[count], color=colors[name_lab], label=name_lab)
            if i == 0:
                handles.append(h)
            # fb
            ax[FB].bar(fb['label'], fb[count], color=colors[name_lab], label=name_lab)

            ax[GAP10].set_title(f'{counters_label[i]}')
            ax[GAP10].set_xticks([], [])
            ax[FB].set_xticks([], [])
            ax[GAP10].grid(True)
            ax[FB].grid(True)
        
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='lower center', labels=legend_labels, ncol=len(legend_labels)//3, bbox_to_anchor=(0.5, 1))

    axes[GAP10,0].set_ylabel('gap_10', rotation=0, ha='right', va="center")
    axes[FB,0].set_ylabel('fb', rotation=0, ha='right', va="center")

    # Set a common label for y ax
    laby = fig.supylabel('Normalized Performance Counter')

    #plt.show()
    name = prefix + ('_no_mwhc' if no_mwhc else '') + ('_pareto' if pareto else '') + '.png'
    fig.savefig(name, bbox_extra_artists=(lgd,laby,), bbox_inches='tight')

# -------- perf join -------- # 
def perf_join(df):
    JOIN_10_25 = 0
    JOIN_25_25 = 1

    datasets = ('wiki','fb')
    counters = ['L1-misses', 'LLC-misses']
    counters_label = ['L1 misses', 'LLC misses']

    df = df.copy(deep=True)
    df['label'] = df.apply(lambda x : f"{x['function']}-{x['table']}".upper(), axis=1)

    # prepare the datasets
    def rename_cols(suffix):
        def _rename_cols_(col):
            if col in ['sizes', 'dataset', 'label']:
                return col
            else:
                return col + '-' + suffix 
        return _rename_cols_

    proj_col = ['sizes', 'dataset', 'label'] + counters
    # select, project and rename
    df_sort = df[df['phase']=='sort'][proj_col].rename(columns=rename_cols('sort'))
    df_build = df[df['phase']=='insert'][proj_col].rename(columns=rename_cols('build'))
    df_probe = df[df['phase']=='join'][proj_col].rename(columns=rename_cols('probe'))
    dfs = [df_sort, df_build, df_probe]
    # join!
    df_join = ft.reduce(lambda left, right: pd.merge(left, right, on=['sizes', 'dataset', 'label']), dfs)
    def get_map(x):
        if isinstance(x, pd.Series):
            return x.apply(lambda i : LAB_MAP[i])
        return LAB_MAP[x]
    df_join = df_join.sort_values(by='label', key=lambda l: get_map(l))

    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
        
    # Create a single legend for all subplots
    legend_labels = ['Sort', 'Build', 'Probe']

    for c_i, c in enumerate(counters):
        fig, axes = plt.subplots(num_subplots, 2, figsize=(9, 5))  # Adjust figsize as needed
        
        handles = []
        i = 0
        for name_ds in datasets:
            group_ds = df_join[df_join['dataset']==name_ds]
            
            ax = axes[i,:]
            i += 1
            
            # 10x25
            join_10_25 = group_ds[group_ds['sizes'] == '10Mx25M']
            # Create the upper part of the bar
            h3 = ax[JOIN_10_25].bar(range(len(join_10_25['label'])), join_10_25[c+'-sort'], color='tab:green', alpha=0.8, label='Sort')
            h1 = ax[JOIN_10_25].bar(range(len(join_10_25['label'])), join_10_25[c+'-build'], color='tab:blue', alpha=0.8, bottom=join_10_25[c+'-sort'], label='Build')
            # Create the lower part of the bar, starting from the top of the upper part
            h2 = ax[JOIN_10_25].bar(range(len(join_10_25['label'])), join_10_25[c+'-probe'], color='tab:orange', alpha=0.8, bottom=join_10_25[c+'-sort']+join_10_25[c+'-build'], label='Probe')

            # 25x25
            join_25_25 = group_ds[group_ds['sizes'] == '25Mx25M']
            ax[JOIN_25_25].bar(range(len(join_25_25['label'])), join_10_25[c+'-sort'], color='tab:green', alpha=0.8, label='Sort')
            ax[JOIN_25_25].bar(range(len(join_25_25['label'])), join_10_25[c+'-build'], color='tab:blue', alpha=0.8, bottom=join_10_25[c+'-sort'], label='Build')
            ax[JOIN_25_25].bar(range(len(join_25_25['label'])), join_10_25[c+'-probe'], color='tab:orange', alpha=0.8, bottom=join_10_25[c+'-sort']+join_10_25[c+'-build'], label='Probe')

            # Customize the plot
            ax[JOIN_10_25].set_title(f'{name_ds} (10Mx25M)')
            ax[JOIN_25_25].set_title(f'{name_ds} (25Mx25M)')

            if i == 1:
                handles.append(h3)
                handles.append(h1)
                handles.append(h2)
                
            ax[JOIN_10_25].set_xticks(range(len(join_10_25['label'])), join_10_25['label'], rotation=35, ha='right', va='top')
            ax[JOIN_25_25].set_xticks(range(len(join_25_25['label'])), join_25_25['label'], rotation=35, ha='right', va='top')

            # Format the y-axis to display only two digits after the decimal point
            # ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))

            ax[JOIN_10_25].grid(True)
            ax[JOIN_25_25].grid(True)

        # Add a single legend to the entire figure with labels on the same line
        lgd = fig.legend(handles=handles, loc='upper center', labels=legend_labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.07))

        # Set a common label for x and y axes
        laby = fig.supylabel(f'Normalized {counters_label[c_i]}')

        #plt.show()
        name = prefix + '_' + c + '.png'
        fig.savefig(name, bbox_extra_artists=(lgd,laby,), bbox_inches='tight')

    # do avg plot
    fig, axes = plt.subplots(len(counters), 2, figsize=(9, 5))  # Adjust figsize as needed
    handles = []
    i = 0
    for name_ds in datasets:
        group_ds = df_join[df_join['dataset']==name_ds]
        df_merge = groupby_helper(group_ds, ['dataset','label'], [f'{c}-sort' for c in counters] + [f'{c}-build' for c in counters] + [f'{c}-probe' for c in counters])
        df_merge = df_merge.sort_values(by='label', key=lambda l: get_map(l))
        ax = axes[i]
        ax[0].set_ylabel(name_ds, rotation=0, ha='right', va="center")
        i += 1
        for c_i, c in enumerate(counters):
            # Create the upper part of the bar
            h3 = ax[c_i].bar(range(len(df_merge['label'])), df_merge[c+'-sort'], color='tab:green', alpha=0.7, label='Sort')
            h1 = ax[c_i].bar(range(len(df_merge['label'])), df_merge[c+'-build'], color='tab:blue', alpha=0.7, bottom=df_merge[c+'-sort'], label='Build')
            # Create the lower part of the bar, starting from the top of the upper part
            h2 = ax[c_i].bar(range(len(df_merge['label'])), df_merge[c+'-probe'], color='tab:orange', alpha=0.7, bottom=df_merge[c+'-sort']+df_merge[c+'-build'], label='Probe')

            if i == 1:
                ax[c_i].set_title(counters_label[c_i])
                handles.append(h3)
                handles.append(h1)
                handles.append(h2)
                
            ax[c_i].set_xticks(range(len(df_merge['label'])), df_merge['label'], rotation=35, ha='right', va='top')
            ax[c_i].grid(True)

    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=legend_labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.07))

    #plt.show()
    laby = fig.supylabel('Normalized Performance Counter')
    name = prefix + '_' + 'size-avg.png'
    fig.savefig(name, bbox_extra_artists=(lgd,laby,), bbox_inches='tight')    

# -------- point+range -------- # 
def point_range(df, pareto=False):
    datasets = ['wiki','fb']
    df = df[df["label"].str.lower().str.contains("range")].copy(deep=True)
    if df.empty:
        return
    # remove failed experiments
    df = df[df["insert_fail_message"]=='']

    # Back-compatibility
    if 'probe_type' in df.columns:
        # filter for probe distribution type
        probe_type = '80-20' if pareto else 'uniform'
        df = df[df['probe_type']==probe_type]
        if df.empty:
            return
    elif pareto:
        return

    # Group by
    df = groupby_helper(df, ['dataset_name','label','function','range_size','point_query_%'], ['dataset_size','probe_elem_count','tot_time_probe_s'])
    df['throughput_M'] = df.apply(lambda x : x['probe_elem_count']/(x['tot_time_probe_s']*10**6), axis=1)
    df['struct'] = df['label'].apply(lambda x : get_struct_type(x))

    structs = df['struct'].unique()

    # Separate the df in two parts: point and range
    df_point = df[df['range_size']==0]
    df_range = df[df['range_size']!=0]

    # Sort the datasets
    df_point = df_point.sort_values(by='point_query_%')
    df_range = df_range.sort_values(by='range_size')
    
    # Create a single figure with multiple subplots in a row
    POINT = 0
    RANGE = 1
    num_subplots = len(datasets)

    # create the subfigures and subplots
    fig = plt.figure(figsize=(5, 4))
    subfigs = fig.subfigures(2, 1, height_ratios=[1.1, 1], hspace=.05)

    axs_point = subfigs[POINT].subplots(1, num_subplots)
    axs_point = axs_point.flatten()
    lg1 = subfigs[POINT].supxlabel('Percentage of Point Queries')
    #lg2 = subfigs[POINT].supylabel('Throughput \n(Million operations/s)')

    axs_range = subfigs[RANGE].subplots(1, num_subplots)
    axs_range = axs_range.flatten()
    lg3 = subfigs[RANGE].supxlabel('Range Query Size')
    #lg4 = subfigs[RANGE].supylabel('Throughput \n(Million operations/s)')
    
    # Create a single legend for all subplots
    legend_labels = []

    # for each dataset
    for i, _d_ in enumerate(datasets):
        _point_ = df_point[df_point['dataset_name']==_d_]
        _range_ = df_range[df_range['dataset_name']==_d_]
        for _s_ in structs:
            # Point
            point_s = _point_[_point_['struct']==_s_]
            axs_point[i].plot(point_s['point_query_%'], point_s['throughput_M'], color=COLORS_STRUCT[_s_], marker=SHAPES_STRUCT[_s_], label=_s_)
            # Range
            range_s = _range_[_range_['struct']==_s_]
            axs_range[i].plot(range_s['range_size'], range_s['throughput_M'], color=COLORS_STRUCT[_s_], marker=SHAPES_STRUCT[_s_], label=_s_)
            if _s_ not in legend_labels:
                legend_labels.append(_s_)
        
        # Customize the plot
        axs_point[i].set_title(f'{_d_}')
        axs_point[i].set_xlim([0,100])
        axs_point[i].set_xticks([0,25,50,75,100],['0','25','50','75','100'])
        axs_range[i].set_xscale('log', base=2)
        axs_range[i].set_xticks([1,8,64,1024],['1','8','64','1024'])

        axs_point[i].grid(True)
        axs_range[i].grid(True)
    
    axs_point[0].set_ylabel('Throughput \n(Million operations/s)')
    axs_range[0].set_ylabel('Throughput \n(Million operations/s)')
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=[line for line in fig.axes[0].lines], loc='upper center', labels=legend_labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.1))

    #plt.show()
    name = prefix + '_range' + ('_pareto.png' if pareto else '.png')
    fig.savefig(name, bbox_extra_artists=(lgd,lg1,lg3,), bbox_inches='tight')

# -------- join -------- #
def join(df):
    JOIN_10_25 = 0
    JOIN_25_25 = 1

    datasets = ('wiki','fb')
    df = df[df["label"].str.lower().str.contains("join")].copy(deep=True)
    if df.empty:
        return
    # remove failed experiments
    df = df[df["has_failed"]==False]

    # Group by
    df['table_type'] = df['label'].apply(lambda x : get_table_type(x))
    df['table_label'] = df.apply(lambda x : get_table_label(x['function'],x['table_type']).upper(), axis=1)
    df = groupby_helper(df, ['dataset_name','table_label','function','join_size'], ['tot_time_sort_s', 'tot_time_build_s', 'tot_time_join_s'])
    def get_map(x):
        if isinstance(x, pd.Series):
            return x.apply(lambda i : LAB_MAP[i])
        return LAB_MAP[x]
    df = df.sort_values(by='table_label', key=lambda l: get_map(l))

    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    fig, axes = plt.subplots(num_subplots, 2, figsize=(9, 5))  # Adjust figsize as needed
    
    # Create a single legend for all subplots
    legend_labels = ['Sort', 'Build', 'Probe']
    handles = []
    
    i = 0
    for name_ds in datasets:
        group_ds = df[df['dataset_name']==name_ds]
        ax = axes[i]
        i += 1
        
        # 10x25
        join_10_25 = group_ds[group_ds['join_size'] == '(10Mx25M)']
        # Create the upper part of the bar
        h3 = ax[JOIN_10_25].bar(range(len(join_10_25['table_label'])), join_10_25['tot_time_sort_s'], color='tab:green', label='Sort')
        h1 = ax[JOIN_10_25].bar(range(len(join_10_25['table_label'])), join_10_25['tot_time_build_s'], color='tab:blue', bottom=join_10_25['tot_time_sort_s'], label='Build')
        # Create the lower part of the bar, starting from the top of the upper part
        h2 = ax[JOIN_10_25].bar(range(len(join_10_25['table_label'])), join_10_25['tot_time_join_s'], color='tab:orange', bottom=join_10_25['tot_time_build_s']+join_10_25['tot_time_sort_s'], label='Probe')

        # 25x25
        join_25_25 = group_ds[group_ds['join_size'] == '(25Mx25M)']
        ax[JOIN_25_25].bar(range(len(join_25_25['table_label'])), join_25_25['tot_time_sort_s'], color='tab:green', label='Sort')
        ax[JOIN_25_25].bar(range(len(join_25_25['table_label'])), join_25_25['tot_time_build_s'], color='tab:blue', bottom=join_25_25['tot_time_sort_s'], label='Build')
        ax[JOIN_25_25].bar(range(len(join_25_25['table_label'])), join_25_25['tot_time_join_s'], color='tab:orange', bottom=join_25_25['tot_time_build_s']+join_25_25['tot_time_sort_s'], label='Probe')

        # Customize the plot
        ax[JOIN_10_25].set_title(f'{name_ds} (10Mx25M)')
        ax[JOIN_25_25].set_title(f'{name_ds} (25Mx25M)')

        if i == 1:
            handles.append(h3)
            handles.append(h1)
            handles.append(h2)
            
        ax[JOIN_10_25].set_xticks(range(len(join_10_25['table_label'])), join_10_25['table_label'], rotation=35, ha='right', va='top')
        ax[JOIN_25_25].set_xticks(range(len(join_25_25['table_label'])), join_25_25['table_label'], rotation=35, ha='right', va='top')

        # Format the y-axis to display only two digits after the decimal point
        # ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))

        ax[JOIN_10_25].grid(True)
        ax[JOIN_25_25].grid(True)

    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=legend_labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.07))

    # Set a common label for x and y axes
    laby = fig.supylabel('Running Time (s)')

    #plt.show()
    name = prefix + '_join.png'
    fig.savefig(name, bbox_extra_artists=(lgd,laby,), bbox_inches='tight')

# -------- coroutines probe -------- #
# available which_y : time_gain, throughput_sequential, throughput_interleaved
def coro_probe(df, pareto=False, which_y = 'time_gain'):
    datasets = ['gap_10','normal','wiki','osm','fb']
    df = df[df["label"].str.lower().str.contains("coro:")].copy(deep=True)
    if df.empty:
        return
    # remove failed experiments
    df = df[df["insert_fail_message"]=='']
    # remove double prefetch experiments
    df = df[df['function']!='RMICoro']
    if df.empty:
        return
    
    # get models
    df['models'] = df['function_name'].apply(lambda x : get_rmi_models(x))

    label_y = ''
    ylim = []
    ylabels = []
    if which_y == 'time_gain':
        label_y = 'Time Gain\n(Sequential / Interleaved)'
        ylim = [0, 4.5]
        ylabels = [1,2,3,4]
    elif which_y == 'throughput_sequential':
        label_y = 'Sequential Throughput\n(Million operations/s)'
        ylim = [-1, 24]
        ylabels = [0,8,16,24]
    elif which_y == 'throughput_interleaved':
        label_y = 'Interleaved Throughput\n(Million operations/s)'
        ylim = [-0.5, 8]
        ylabels = [0,2,4,6,8]
    else:
        # not supported
        return

    # Back-compatibility
    if 'probe_type' in df.columns:
        # filter for probe distribution type
        probe_type = '80-20' if pareto else 'uniform'
        df = df[df['probe_type']==probe_type]
        if df.empty:
            return
    elif pareto:
        return
    
    # Group by
    df = groupby_helper(df, ['dataset_name','label','function','load_factor_%','n_coro','models'], ['dataset_size','probe_elem_count','tot_for_time_interleaved_s', 'tot_for_time_sequential_s'])
    df['time_gain'] = df['tot_for_time_sequential_s']/df['tot_for_time_interleaved_s']
    df['throughput_sequential'] = df['probe_elem_count']/(df['tot_for_time_sequential_s']*10**6)
    df['throughput_interleaved'] = df['probe_elem_count']/(df['tot_for_time_interleaved_s']*10**6)
    df['load_factor'] = df['load_factor_%']/100
    df = df.sort_values(by='load_factor')
    
    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    fig, axes = plt.subplots(1, num_subplots, figsize=(12,2))

    def format_ticks(value, _):
        if value % 1 == 0:
            # If the number is an integer, return it without the decimal part
            return "{:d}".format(int(value))
        else:
            # If the number is a float, check for leading zero and format accordingly
            str_value = "{:.2f}".format(value).rstrip('0')
            if str_value.startswith("0"):
                return str_value[1:]
            return str_value
    
    # Create a single legend for all subplots
    legend_labels = []
    
    i = 0
    # for each dataset
    for name_ds in datasets:
        group_ds = df[df['dataset_name']==name_ds]
        # keep only default models
        if name_ds == 'fb':
            group_ds = group_ds[(group_ds['models']==10000000) | (group_ds['models']==0)]
        if name_ds == 'wiki':
            group_ds = group_ds[(group_ds['models']==1000) | (group_ds['models']==0)]
        g_fn = group_ds.groupby('function')
        ax = axes[i]
        i += 1
        
        # for each function
        for name_fs, group_fn in g_fn:
            ax.plot(group_fn['load_factor'], group_fn[which_y], color=COLORS[name_fs], marker=SHAPES_FN[name_fs], label=name_fs)
            if name_fs not in legend_labels:
                legend_labels.append(name_fs)

        # plot ideal trend
        if which_y == 'time_gain':
            ax.axhline(y=1, color='black', linestyle='--', alpha=0.5)
        
        # Customize the plot
        ax.set_title(f'{name_ds}')
        # Format the y-axis to display only two digits after the decimal point
        # ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
        ax.set_ylim(ylim)
        ax.set_yticks(ylabels, ['' for _ in ylabels])
        ax.set_xscale('log')
        ticks = list(group_ds['load_factor'].unique())
        try:
            ticks.remove(70)
        except:
            pass
        ax.set_xticks(ticks, [format_ticks(t, None) for t in ticks])
        ax.grid(True)
    
    axes[0].set_yticks(ylabels, [f'{l}' for l in ylabels])

    labels, handles = sort_labels(legend_labels, [line for line in fig.axes[0].lines[:len(legend_labels)]])
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=handles, loc='upper center', labels=labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.2))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Load Factor')
    laby = fig.supylabel(label_y, va='center', ha='center')

    #plt.show()
    name = prefix + '_probe_'+ which_y + ('_pareto.png' if pareto else '.png')
    fig.savefig(name, bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

# -------- coroutines rmi -------- #
# available which_y : time_gain, throughput_sequential, throughput_interleaved
def coro_rmi(df, which_y = 'time_gain'):
    datasets = ['gap_10','fb']
    df = df[df["label"].str.lower().str.contains("coro-rmi:")].copy(deep=True)
    if df.empty:
        return
    
    label_y = ''
    ylim = []
    ylabels = []
    if which_y == 'time_gain':
        label_y = 'Time Gain\n(Sequential / Interleaved)'
        ylim = [0.13, 1.05]
        ylabels = [0.25,0.50,0.75,1]
    elif which_y == 'throughput_sequential':
        label_y = 'Sequential Throughput\n(Million operations/s)'
        ylim = [20, 170]
        ylabels = [20,60,100,140]
    elif which_y == 'throughput_interleaved':
        label_y = 'Interleaved Throughput\n(Million operations/s)'
        ylim = [20, 35]
        ylabels = [20,25,30,35]
    else:
        # not supported
        return
    
    # Group by
    df = groupby_helper(df, ['dataset_name','label','function', 'n_coro'], ['dataset_size','tot_sequential_time_s','tot_interleaved_time_s'])
    df["models"] = df["label"].apply(lambda x : get_rmi_models(x))
    df['time_gain'] = df['tot_sequential_time_s']/df['tot_interleaved_time_s']
    df['throughput_sequential'] = df['dataset_size']/(df['tot_sequential_time_s']*10**6)
    df['throughput_interleaved'] = df['dataset_size']/(df['tot_interleaved_time_s']*10**6)
    df = df.sort_values(by='models')

    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    fig, axes = plt.subplots(1, num_subplots, figsize=(4.5,2))

    i = 0
    # for each dataset
    for name_ds in datasets:
        group_ds = df[df['dataset_name']==name_ds]
        ax = axes[i]
        i += 1
        # make ticks
        ticks = group_ds['models'].apply(lambda x : log10(x))
        tick_labels = [f"$10^{int(tick)}$" for tick in ticks]
        ax.set_xticks(ticks[1::2], tick_labels[1::2])
        ax.set_ylim(ylim)
        ax.set_yticks(ylabels, ['' for _ in ylabels])
        # plot ideal trend
        if which_y == 'time_gain':
            ax.axhline(y=1, color='black', linestyle='--', alpha=0.5)
        # for each function
        ax.plot(ticks, group_ds[which_y], color=COLORS['RMI'], marker=SHAPES_FN['RMI'])
        # Customize the plot
        ax.set_title(f'{name_ds}')
        ax.grid(True)
        
    
    axes[0].set_yticks(ylabels, [f'{l}' for l in ylabels])
    # Set a common label for x and y axes
    labx = fig.supxlabel('RMI number of submodels')
    laby = fig.supylabel(label_y, va='center', ha='center')

    #plt.show()
    name = prefix + '_rmi_' + which_y + '.png'
    fig.savefig(name, bbox_extra_artists=(labx,laby,), bbox_inches='tight')

# =============================== MAINS =============================== #l

def main_json():
    # Load benchmarks from the file
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
        # convert json results to dataframe
        df = pd.json_normalize(data, 'benchmarks')
        # compute function names
        df["function"] = df.apply(lambda x: get_fn_name(x['label']) if get_fn_name(x['label']) else get_fn_name(x['function_name']), axis=1)
        collisions(df)
        collisions_rmi(df)
        distribution(df)
        gaps(df)
        probe(df)
        probe(df, pareto=True)
        probe_high_lf(df)
        probe_all_lf(df)
        insert(df)
        insert(df, pareto=True)
        build(df)
        point_range(df)
        point_range(df, pareto=True)
        join(df)
        coro_probe(df, which_y='time_gain')
        coro_probe(df, which_y='throughput_interleaved')
        coro_probe(df, which_y='throughput_sequential')
        coro_rmi(df, which_y='time_gain')
        coro_rmi(df, which_y='throughput_interleaved')
        coro_rmi(df, which_y='throughput_sequential')

def main_csv():
    df = pd.read_csv(file_path)
    if 'phase' in df.columns:
        # put 0 instead of NaN
        df = df.fillna(0)
        perf_join(df)
    else:
        perf_probe(df)
        perf_probe(df, pareto=True)

#----------------------------#
COLORS, COLORS_STRUCT = prepare_fn_colormap()

if __name__ == '__main__':
    if ext == '.json':
        main_json()
    elif ext == '.csv':
        main_csv()
    else:
        print(f'Sorry, extension {ext} is not supported :c')
        sys.exit(1)
