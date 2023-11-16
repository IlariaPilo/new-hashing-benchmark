import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

plt.rcParams['figure.constrained_layout.use'] = True
# ax.grid(which='major', color=, linewidth=0
plt.rcParams['grid.color'] = '#DDDDDD'
plt.rcParams['grid.linewidth'] = 0.8

# =============================== SETUP =============================== #

script_path = os.path.abspath(__file__)
script_directory = os.path.dirname(script_path)
BASE_DIR = os.path.abspath(f'{script_directory}/..')+'/'

FILE_1 = BASE_DIR + 'output/bonette/join_1.json'
FILE_2 = BASE_DIR + 'output/bonette/join_2.json'
FILE_4 = BASE_DIR + 'output/bonette/join_4.json'
FILE_5 = BASE_DIR + 'output/bonette/join_5.json'
FILE_6 = BASE_DIR + 'output/bonette/join_6.json'
FILE_8 = BASE_DIR + 'output/bonette/join_8.json'
FILE_16 = BASE_DIR + 'output/bonette/join_16.json'
FILE_24 = BASE_DIR + 'output/bonette/join_24.json'

t_numbers = [1,2,4,8,16,24]
f_names = [FILE_1, FILE_2, FILE_4, FILE_8, FILE_16, FILE_24]
bm_dict = {}

prefix = BASE_DIR + 'figs/join_cmp'

# ============================= UTILITIES ============================= #

SHAPES_TAB = {
    'linear': 'h',
    'chained': '>',
    'cuckoo': 'P'
}
COLORS = {}

ALPHA_FN = {
    'RMI': 0.8,
    'MWHC': 0.8,
    'MultiplyPrime': 0.8
}
COLOR_TAB = {
    'linear': 'tab:red',
    'chained': 'tab:blue',
    'cuckoo': 'tab:green'
}
F_MAP = {
    'Ideal Trend': -1,
    'RMI-CHAIN': 0,
    'MULT-CHAIN': 3,
    'MWHC-CHAIN': 6,
    'RMI-LP': 1,
    'MULT-LP': 4,
    'MWHC-LP': 7,
    'RMI-CUCKOO': 2,
    'MULT-CUCKOO': 5,
    'MWHC-CUCKOO': 8
}
def get_map(x):
    if isinstance(x, pd.Series):
        return x.apply(lambda i : F_MAP[i])
    return F_MAP[x]

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

def groupby_helper(df, static_cols, variable_cols):
    return df.groupby(static_cols)[variable_cols].mean().reset_index()

def sort_labels(labels, handles):
    pairs = list(zip(labels, handles))
    sorted_pairs = sorted(pairs, key=lambda pair: F_MAP[pair[0]])
    return zip(*sorted_pairs)

def prepare_fn_colormap():
    functions = ['RMI','RadixSpline','PGM','Murmur','MultiplyPrime','FibonacciPrime','XXHash','AquaHash','MWHC','BitMWHC','RecSplit'][::-1]
    cmap = plt.get_cmap('coolwarm')
    # Create a dictionary of unique colors based on the number of labels
    col_dict = {functions[f_i]: cmap(col_i) for f_i, col_i in enumerate(np.linspace(0, 1, len(functions)))}
    ret_dict = {
        'RMI-CHAIN': col_dict[functions[-1]],
        'MULT-CHAIN': col_dict[functions[-5]],
        'MWHC-CHAIN': col_dict[functions[-9]],
        'RMI-LP': col_dict[functions[-2]],
        'MULT-LP': col_dict[functions[-6]],
        'MWHC-LP': col_dict[functions[-10]],
        'RMI-CUCKOO': col_dict[functions[-3]],
        'MULT-CUCKOO': col_dict[functions[-7]],
        'MWHC-CUCKOO': col_dict[functions[-11]]
    }
    return ret_dict

def load():
    # Load benchmarks from the file
    bms = []
    for file_path in f_names:
        with open(file_path, 'r') as json_file:
            data = json.load(json_file)
            # convert json results to dataframe
            df = pd.json_normalize(data, 'benchmarks')
            bms.append(df)
    return {key: value for key,value in list(zip(t_numbers,bms))}

def print_ratio_img():
    # first, get the base values
    bm1 = bm_dict[1]
    sort1 = bm1['tot_time_sort_s']
    build1 = bm1['tot_time_build_s']
    probe1 = bm1['tot_time_join_s']
    # now, compute the ratios
    for key in bm_dict:
        bm = bm_dict[key]
        # compute function names
        bm["function"] = bm['function_name'].apply(lambda x: get_fn_name(x))
        # Add ratios
        bm['ratio_sort'] = (sort1/bm['tot_time_sort_s']).replace(np.nan, 0)
        bm['ratio_build'] = build1/bm['tot_time_build_s']
        bm['ratio_probe'] = probe1/bm['tot_time_join_s']
        # remove failed experiments
        bm = bm[bm["has_failed"]==False]
        bm['table_type'] = bm['label'].apply(lambda x : get_table_type(x))
        bm['table_label'] = bm.apply(lambda x : get_table_label(x['function'],x['table_type']).upper(), axis=1)
        bm = groupby_helper(bm, ['table_label','table_type','function','join_size'], ['ratio_sort', 'ratio_build', 'ratio_probe'])
        bm = bm.sort_values(by='table_label', key=lambda x: get_map(x))
        bm_dict[key] = bm

    labels = bm_dict[1]['table_label'].unique()
    labels = sorted(labels, key=lambda l: F_MAP[l])

    # x
    sizes = ['(10Mx25M)','(25Mx25M)']
    # y
    BUILD = 0
    PROBE = 1
    handles = []
    fig, axes = plt.subplots(2, 2, figsize=(7.5, 5))  # Adjust figsize as needed

    for i,size in enumerate(sizes):
        ax = axes[i]
        # base one
        h, = ax[BUILD].plot(t_numbers, t_numbers, '--k', alpha = 0.5)
        ax[PROBE].plot(t_numbers, t_numbers, '--k', alpha = 0.5)
        if i == 0:
            handles.append(h)

        for l in labels:
            # make list of ratios
            ratios = pd.DataFrame()
            for key in bm_dict:
                bm = bm_dict[key]
                df = bm[np.logical_and(bm['join_size']==size, bm['table_label']==l)].copy()
                df['threads'] = key
                ratios = pd.concat([ratios, df], ignore_index=True)

            h, = ax[BUILD].plot(ratios['threads'], ratios['ratio_build'], color=COLORS[l], marker=SHAPES_TAB[ratios['table_type'][0]], label=l)
            ax[PROBE].plot(ratios['threads'], ratios['ratio_probe'], color=COLORS[l], marker=SHAPES_TAB[ratios['table_type'][0]], label=l)
    
            if i == 0:
                handles.append(h)

            ax[BUILD].set_xlim([1,24])
            ax[PROBE].set_xlim([1,24])
            ax[BUILD].set_ylim([0,5.5])
            ax[PROBE].set_ylim([0,22])
            ax[BUILD].set_xticks(t_numbers, [f'{t}' for t in t_numbers])
            ax[PROBE].set_xticks(t_numbers, [f'{t}' for t in t_numbers])

            ax[BUILD].grid(True)
            ax[PROBE].grid(True)

    axes[0,BUILD].set_title('Build')
    axes[0,PROBE].set_title('Probe')

    axes[0,0].set_ylabel('(10Mx25M)')
    axes[1,0].set_ylabel('(25Mx25M)')

    labels.insert(0, 'Ideal Trend')
    lgd = fig.legend(handles=handles, loc='upper center', labels=labels, ncol=len(labels)//2+len(labels)%2, bbox_to_anchor=(0.5, 1.12))

    # Set a common label for x and y axes
    labx = fig.supxlabel('Thread Number')
    laby = fig.supylabel('Time Increment Ratio')

    #plt.show()
    name = prefix + '.png'
    fig.savefig(name, bbox_extra_artists=(lgd,labx,laby,), bbox_inches='tight')

if __name__ == '__main__':
    COLORS = prepare_fn_colormap()
    bm_dict = load()
    print_ratio_img()

    
