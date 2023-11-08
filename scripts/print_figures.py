import json
import sys
import pandas as pd
import matplotlib.pyplot as plt
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
    
COLORS = {}
COLORS_STRUCT = {}

CHAINED = 0
LINEAR = 1
CUCKOO = 2  

def get_fn_name(label):
    label = label.lower()
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
    pattern = r'rmi_hash_(\d+):'
    match = re.search(pattern, label)
    return int(match.group(1))

def prepare_fn_colormap():
    functions = ['RMI','RadixSpline','PGM','Murmur','MultiplyPrime','FibonacciPrime','XXHash','AquaHash','MWHC','BitMWHC','RecSplit'][::-1]
    cmap = plt.get_cmap('turbo')
    # Create a dictionary of unique colors based on the number of labels
    col_dict = {functions[f_i]: cmap(col_i) for f_i, col_i in enumerate(np.linspace(0, 1, len(functions)))}
    col_struct = {'RMI-Chain': col_dict['RMI'], 'RadixSpline-Chain': col_dict['PGM'], 'RMI-Sort': col_dict['BitMWHC']}
    return col_dict, col_struct

def groupby_helper(df, static_cols, variable_cols):
    return df.groupby(static_cols)[variable_cols].mean().reset_index()

def sort_labels(labels, handles):
    pairs = list(zip(labels, handles))
    sorted_pairs = sorted(pairs, key=lambda pair: F_MAP[pair[0]])
    return zip(*sorted_pairs)

# ===================================================================== #

# ------- collision plot ------- #
def collisions(df):
    datasets = ['gap_10','uniform','normal','wiki','fb']
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
    fig, axes = plt.subplots(1, num_subplots, figsize=(10, 2))  # Adjust figsize as needed
    
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

    uniform = df[df["dataset_name"] == "uniform"]
    normal = df[df["dataset_name"] == "normal"]
    
    fig, axes = plt.subplots(1, 2, figsize=(5, 2))

    uni_ticks = uniform['models'].apply(lambda x : log10(x))
    normal_ticks = normal['models'].apply(lambda x : log10(x))

    # axes[0].set_xscale('log')
    # axes[1].set_xscale('log')
    axes[0].plot(uni_ticks, uniform['collisions']/uniform['dataset_size'], color=COLORS['RMI'], marker=SHAPES_FN['RMI'])
    axes[1].plot(normal_ticks, normal['collisions']/normal['dataset_size'], color=COLORS['RMI'], marker=SHAPES_FN['RMI'])        

    # ticks
    uni_tick_labels = [f"$10^{int(tick)}$" for tick in uni_ticks]
    normal_tick_labels = [f"$10^{int(tick)}$" for tick in normal_ticks]
    axes[0].set_xticks(uni_ticks[1::2], uni_tick_labels[1::2])
    axes[1].set_xticks(normal_ticks[1::2], normal_tick_labels[1::2])
    
    axes[0].set_title('uniform')
    axes[1].set_title('normal')
    axes[0].yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
    axes[1].yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
    axes[0].set_ylim([0, 1])
    axes[1].set_ylim([0, 1])
    axes[0].grid(True)
    axes[1].grid(True)

    # Set a common label for x and y axes
    labx = fig.supxlabel('RMI number of submodels')
    laby = fig.supylabel('Ratio of\nColliding Keys')

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
        plt.plot(x, y, marker=SHAPES_DS[_d_], markersize=4, label=_d_) # MARKER TODO

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

# ------- insert ------- #
def insert(df, pareto=False):
    datasets = ['wiki','fb']
    df = df[df["label"].str.lower().str.contains("probe")].copy(deep=True)
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
    df = df.sort_values(by='label')

    # Prepare the colors
    labels = df['label'].unique()
    cmap = plt.get_cmap('coolwarm')
    # Create a dictionary of unique colors based on the number of labels
    colors = {labels[lab_i]: cmap(col_i) for lab_i, col_i in enumerate(np.linspace(0, 1, len(labels)))}

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

    g_lab = df.groupby('label')
    # For each label
    for name_lab, group_lab in g_lab:
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
    laby = fig.supylabel('Performance Counter Ratio')

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
    df = df.sort_values(by='label')

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

    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)

    for c_i, c in enumerate(counters):
        fig, axes = plt.subplots(num_subplots, 2, figsize=(9, 5))  # Adjust figsize as needed
        
        # Create a single legend for all subplots
        legend_labels = ['Sort', 'Build', 'Probe']
        handles = []
        
        i = 0
        for name_ds in datasets:
            group_ds = df_join[df_join['dataset']==name_ds]
            ax = axes[:,i]
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
    df = df.sort_values(by='table_label')

    # Create a single figure with multiple subplots in a row
    num_subplots = len(datasets)
    fig, axes = plt.subplots(num_subplots, 2, figsize=(9, 5))  # Adjust figsize as needed
    
    # Create a single legend for all subplots
    legend_labels = ['Sort', 'Build', 'Probe']
    handles = []
    
    i = 0
    for name_ds in datasets:
        group_ds = df[df['dataset_name']==name_ds]
        ax = axes[:,i]
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
        insert(df)
        insert(df, pareto=True)
        build(df)
        point_range(df)
        point_range(df, pareto=True)
        join(df)

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
