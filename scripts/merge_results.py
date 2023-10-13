import argparse
import json
import glob
import os
import pandas as pd
# Thanks ChatGPT for this basic script

def main():
    parser = argparse.ArgumentParser(description="Merge JSON/CSV files containing benchmark results.")
    parser.add_argument("input_files", nargs="+", help="List of input JSON/CSV files to merge")
    parser.add_argument("-o", "--output-file", required=True, help="Output file name, *without extension*")

    args = parser.parse_args()
    output_file_path = args.output_file

    # Expand file patterns using glob if necessary
    expanded_input_files = []
    for input_file in args.input_files:
        if glob.has_magic(input_file):
            expanded_input_files.extend(glob.glob(input_file))
        else:
            expanded_input_files.append(input_file)

    # split json from csv
    json_files = []
    csv_files = []
    for input_file in expanded_input_files:
        _, ext = os.path.splitext(os.path.basename(input_file))
        if ext == 'json':
            json_files.append(input_file)
        elif ext == 'csv':
            csv_files.append(input_file)
        # else, we ignore the file

    # ---------------------------- JSON ---------------------------- #
    # Read the JSON files and merge the arrays
    merged_benchmarks = []
    merged_contexts = []

    for file_path in json_files:
        with open(file_path, "r") as file:
            data = json.load(file)
            benchmarks = data.get("benchmarks", [])
            merged_benchmarks.extend(benchmarks)
            context = data.get("context", {})
            context['file_name'] = file_path
            merged_contexts.append(context)
    
    # Save the Merged JSON
    if json_files:
        with open(output_file_path+'.json', "w") as output_file:
            json.dump({"benchmarks": merged_benchmarks, 'context': merged_contexts}, output_file, indent=4)

    # ---------------------------- CSV ---------------------------- #
    csv_df = pd.DataFrame()
    for file_path in csv_files:
        df = pd.read_csv(file_path)
        if 'probe_type' in df.columns:
            df['probe_type'] = 'uniform'
        if csv_df.empty:
            csv_df.columns = df.columns
        # concatenate
        csv_df = pd.concat([csv_df, df], ignore_index=True)

    if csv_files:
        csv_df.to_csv(output_file_path+'.csv', index=False)
    
if __name__ == "__main__":
    main()
