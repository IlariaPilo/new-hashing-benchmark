import json
import pandas as pd
import matplotlib.pyplot as plt
from math import log10
import os
import re

plt.rcParams['figure.constrained_layout.use'] = True
# ax.grid(which='major', color=, linewidth=0
plt.rcParams['grid.color'] = '#DDDDDD'
plt.rcParams['grid.linewidth'] = 0.8

# =============================== SETUP =============================== #

script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)
BASE_DIR = os.path.abspath(f'{script_directory}/..')+'/'

FILE_NORMAL = BASE_DIR + 'output/bonette/coroutines-probe.json'
FILE_RMI = BASE_DIR + 'output/bonette/coroutines-probe_rmi.json'
FILE_MERGED = ''

out_ratio = BASE_DIR + 'figs/coroutines_cmp_ratio.png'
out_throughput = BASE_DIR + 'figs/coroutines_cmp_throughput.png'

# ============================= UTILITIES ============================= #

def get_rmi_models(label):
    pattern = r'rmi_hash_(\d+)'
    match = re.search(pattern, label)
    return int(match.group(1))

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
  
# def groupby_helper(df, static_cols, variable_cols):
#     return df.groupby(static_cols)[variable_cols].mean().reset_index()

def load():
    f_names = []
    # Load benchmarks from the file
    if FILE_MERGED == '':
        f_names = [FILE_NORMAL, FILE_RMI]
    else:
        f_names = [FILE_MERGED]
    bms = []
    for file_path in f_names:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            # convert json results to dataframe
            df = pd.json_normalize(data, 'benchmarks')
            # compute function names
            df["function"] = df['function_name'].apply(lambda x: get_fn_name(x))
            # remove everything that is not RMI
            df = df[df['function'].str.lower().str.contains("rmi")]
            df["models"] = df["function_name"].apply(lambda x : get_rmi_models(x))
            df['load_factor'] = df['load_factor_%']/100
            bms.append(df)
    if len(bms) == 2:
        # done
        return bms[0], bms[1]
    # else, we need to split
    df = bms[0]
    df_normal = df[df['function']=='RMI']
    df_rmi = df[df['function']=='RMICoro']
    return df_normal, df_rmi

def join(df_normal, df_rmi):
    on_cols = ['dataset_name', 'load_factor', 'models']
    proj_cols = ['dataset_name', 'load_factor', 'probe_elem_count', 'models', 'tot_for_time_sequential_s', 'tot_for_time_interleaved_s_x', 'tot_for_time_interleaved_s_y']
    df = pd.merge(df_normal, df_rmi, on=on_cols)
    df['probe_elem_count'] = (df['probe_elem_count_x']+df['probe_elem_count_y'])/2
    df['tot_for_time_sequential_s'] = (df['tot_for_time_sequential_s_x']+df['tot_for_time_sequential_s_y'])/2
    df = df[proj_cols]
    mapper = {
        'tot_for_time_interleaved_s_x' : 'tot_for_time_interleaved_s_normal',
        'tot_for_time_interleaved_s_y' : 'tot_for_time_interleaved_s_rmi',
        'dataset_name' : 'dataset_name', 
        'load_factor' : 'load_factor', 
        'probe_elem_count' : 'probe_elem_count', 
        'models' : 'models', 
        'tot_for_time_sequential_s' : 'tot_for_time_sequential_s'
    }
    return df.rename(columns=mapper)

def print_ratio_imgs(df):
    datasets = ['gap_10','normal','wiki','osm','fb']
    num_subplots = len(datasets)
    # compute throughputs and ratios
    df['time_gain_normal'] = df['tot_for_time_sequential_s']/df['tot_for_time_interleaved_s_normal']
    df['time_gain_rmi'] = df['tot_for_time_sequential_s']/df['tot_for_time_interleaved_s_rmi']
    df['throughput_sequential'] = df['probe_elem_count']/(df['tot_for_time_sequential_s']*10**6)
    df['throughput_interleaved_normal'] = df['probe_elem_count']/(df['tot_for_time_interleaved_s_normal']*10**6)
    df['throughput_interleaved_rmi'] = df['probe_elem_count']/(df['tot_for_time_interleaved_s_rmi']*10**6)
    # sort
    df = df.sort_values(by='load_factor')
    # common things
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
    #                                  #
    # ############ RATIOS ############ #
    #                                  #
    # Create a single figure with multiple subplots in a row
    legend_labels = ['Classic RMI', 'RMICoro']
    ylabels = [0,1,2,3,4,5]
    fig, axes = plt.subplots(1, num_subplots, figsize=(12,2))
    i = 0
    # for each dataset
    for name_ds in datasets:
        group_ds = df[df['dataset_name']==name_ds]
        ax = axes[i]
        i += 1
        ax.plot(group_ds['load_factor'], group_ds['time_gain_normal'], color='tab:blue', alpha=0.7, marker='s')
        ax.plot(group_ds['load_factor'], group_ds['time_gain_rmi'], color='tab:red', alpha=0.7, marker='s')
        # plot ideal trend
        ax.axhline(y=1, color='black', linestyle='--', alpha=0.5)
        
        # Customize the plot
        models = group_ds["models"].iloc[0]
        models_exp = log10(models)
        models_label = f"$10^{int(models_exp)}$" if models_exp>1 else f'{models}'
        ax.set_title(f'{name_ds} [{models_label}]')
        ax.set_ylim([0, 5])
        ax.set_yticks(ylabels, ['' for _ in ylabels])
        ax.set_xscale('log')
        ticks = group_ds['load_factor'].unique()
        ax.set_xticks(ticks, [format_ticks(t, None) for t in ticks])
        ax.grid(True)
    
    axes[0].set_yticks(ylabels, [f'{l}' for l in ylabels])

    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=[line for line in fig.axes[0].lines[:len(legend_labels)]], loc='upper center', labels=legend_labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.2))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Load Factor')
    laby = fig.supylabel('Time Gain\n(Sequential / Interleaved)', va='center', ha='center')

    fig.savefig(out_ratio, bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

    #                                   #
    # ########## THROUGHPUTS ########## #
    #                                   #
    # Create a single figure with multiple subplots in a row
    legend_labels = ['Sequential', 'Interleaved [Classic RMI]', 'Interleaved [RMICoro]']
    #datasets = ['gap_10','normal','wiki','osm','fb']
    ylims = [[-1,25], [-1,25], [-1,25], [-0.4,10], [-0.4,10]]
    yticks = [[0,5,10,15,20,25], [0,5,10,15,20,25], [0,5,10,15,20,25], [0,2,4,6,8,10], [0,2,4,6,8,10]]
    ylabels = [['0','5','10','15','20','25'], ['','','','','',''], ['','','','','',''], ['0','2','4','6','8','10'], ['','','','','','']]
    #ylabels = [0,1,2,3,4,5]
    fig, axes = plt.subplots(1, num_subplots, figsize=(12,2))
    # for each dataset
    for i,name_ds in enumerate(datasets):
        group_ds = df[df['dataset_name']==name_ds]
        ax = axes[i]
        ax.plot(group_ds['load_factor'], group_ds['throughput_sequential'], color='tab:green', alpha=0.7, marker='s')
        ax.plot(group_ds['load_factor'], group_ds['throughput_interleaved_normal'], color='tab:blue', alpha=0.7, marker='s')
        ax.plot(group_ds['load_factor'], group_ds['throughput_interleaved_rmi'], color='tab:red', alpha=0.7, marker='s')
        
        # Customize the plot
        models = group_ds["models"].iloc[0]
        models_exp = log10(models)
        models_label = f"$10^{int(models_exp)}$" if models_exp>1 else f'{models}'
        ax.set_title(f'{name_ds} [{models_label}]')
        ax.set_ylim(ylims[i])
        ax.set_yticks(yticks[i], ylabels[i])
        ax.set_xscale('log')
        ticks = group_ds['load_factor'].unique()
        ax.set_xticks(ticks, [format_ticks(t, None) for t in ticks])
        ax.grid(True)

    # Add a single legend to the entire figure with labels on the same line
    lgd = fig.legend(handles=[line for line in fig.axes[0].lines[:len(legend_labels)]], loc='upper center', labels=legend_labels, ncol=len(legend_labels), bbox_to_anchor=(0.5, 1.2))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Load Factor')
    laby = fig.supylabel('Probe Throughput\n(Million operations/s)', va='center', ha='center')

    fig.savefig(out_throughput, bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')


if __name__ == '__main__':
    df_normal, df_rmi = load()
    df = join(df_normal, df_rmi)
    print_ratio_imgs(df)

    
