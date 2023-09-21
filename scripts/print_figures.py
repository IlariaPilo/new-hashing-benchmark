import json
import sys
import pandas as pd
import matplotlib.pyplot as plt
import os
import re
from math import log10

plt.rcParams['figure.constrained_layout.use'] = True

# =============================== SETUP =============================== #

# Check if the filename argument is provided
if len(sys.argv) < 2:
    print("\n\033[1;96m\tpython print_figures.py <result.json> \033[0m")
    print("Produces different types of figures, given a <result.json> file.\n")
    sys.exit(1)

script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)
BASE_DIR = os.path.abspath(f'{script_directory}/..')+'/'

# Get the filename from the command-line argument
file_path = sys.argv[1]
prefix, _ = os.path.splitext(os.path.basename(file_path))

prefix = BASE_DIR + 'figs/' + prefix

# ===================================================================== #

# ------- function names ------- #
SHAPES = {
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
COLORS = {
    'RMI': '#a50026',
    'PGM': '#f46d43',
    'Murmur': '#fdae61',
    'BitMWHC': '#4575b4',
    'RadixSpline': '#d73027',
    'MWHC': '#74add1',
    'RecSplit': '#313695',
    'AquaHash': '#e0f3f8',
    'XXHash': '#abd9e9',
    'MultiplyPrime': '#fee08b',
    'FibonacciPrime': '#ffffbf'    
}

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
    
def get_rmi_models(label):
    pattern = r'rmi_hash_(\d+):'
    match = re.search(pattern, label)
    return int(match.group(1))

# ------- collision plot ------- #
def collisions(df):
    df = df[df["label"].str.lower().str.contains("collision")].copy(deep=True)
    # Group the DataFrame by the 'dataset_name' column
    g_ds = df.groupby('dataset_name')
    
    # Create a single figure with multiple subplots in a row
    num_subplots = len(g_ds)
    fig, axes = plt.subplots(1, num_subplots, figsize=(12, 2))  # Adjust figsize as needed
    
    # Create a single legend for all subplots
    legend_labels = []
    
    # Iterate through the groups and create subplots
    for i, (name_ds, group_ds) in enumerate(g_ds):
        g_fn = group_ds.groupby('function')
        ax = axes[i]
        
        # Plot the data for the current group
        for name_fs, group_fn in g_fn:
            # Plot the data and collect labels for the legend
            ax.plot(group_fn['throughput_M'], group_fn['collisions']/group_fn['data_elem_count'], color=COLORS[name_fs], marker=SHAPES[name_fs], label=name_fs)
            if name_fs not in legend_labels:
                legend_labels.append(name_fs)
        
        # Customize the plot
        ax.set_title(f'{name_ds}')
        # Format the y-axis to display only two digits after the decimal point
        ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.2f'))
        ax.set_ylim([0, 1])
        ax.set_xlim([0, 150])
        ax.grid(True)
    
    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=[line for line in fig.axes[0].lines], loc='upper center', labels=legend_labels, ncol=len(legend_labels)//2+len(legend_labels)%2, bbox_to_anchor=(0.5, 1.33))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Hash Computation Throughput (Million operations/s)')
    laby = fig.supylabel('Ratio of Colliding Keys')

    fig.savefig(f'{prefix}_collisions.png', bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

# ------- collision RMI plot ------- #
def collisions_rmi(df):
    df = df[df["label"].str.lower().str.contains("collision")].copy(deep=True)
    df = df[df["function"] == "RMI"]
    df["models"] = df["label"].apply(lambda x : get_rmi_models(x))

    uniform = df[df["dataset_name"] == "uniform"]
    normal = df[df["dataset_name"] == "normal"]
    
    fig, axes = plt.subplots(1, 2, figsize=(5, 2))

    uni_ticks = uniform['models'].apply(lambda x : log10(x))
    normal_ticks = normal['models'].apply(lambda x : log10(x))

    # axes[0].set_xscale('log')
    # axes[1].set_xscale('log')
    axes[0].plot(uni_ticks, uniform['collisions']/uniform['data_elem_count'], color=COLORS['RMI'], marker=SHAPES['RMI'])
    axes[1].plot(normal_ticks, normal['collisions']/normal['data_elem_count'], color=COLORS['RMI'], marker=SHAPES['RMI'])        

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


# =============================== MAIN =============================== #

# Load benchmarks from the file
with open(file_path, 'r') as json_file:
    data = json.load(json_file)
    # convert json results to dataframe
    df = pd.json_normalize(data, 'benchmarks')
    # compute function names
    df["function"] = df["label"].apply(lambda x : get_fn_name(x))
    # compute throughput
    df['throughput_M'] = df.apply(lambda x : x['data_elem_count']/(x['tot_time_s']*10**6), axis=1)
    collisions_rmi(df)