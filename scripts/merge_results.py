import argparse
import json
import glob
# Thanks ChatGPT for this basic script

def main():
    parser = argparse.ArgumentParser(description="Merge JSON files containing benchmark results.")
    parser.add_argument("input_files", nargs="+", help="List of input JSON files to merge")
    parser.add_argument("-o", "--output-file", required=True, help="Output JSON file name")

    args = parser.parse_args()

    # Expand file patterns using glob if necessary
    expanded_input_files = []
    for input_file in args.input_files:
        if glob.has_magic(input_file):
            expanded_input_files.extend(glob.glob(input_file))
        else:
            expanded_input_files.append(input_file)

    # Read the JSON files and merge the arrays
    merged_benchmarks = []
    merged_contexts = []

    for file_path in args.input_files:
        with open(file_path, "r") as file:
            data = json.load(file)
            benchmarks = data.get("benchmarks", [])
            merged_benchmarks.extend(benchmarks)
            context = data.get("context", {})
            context['file_name'] = file_path
            merged_contexts.append(context)
    
    # Save the Merged JSON
    output_file_path = args.output_file
    with open(output_file_path, "w") as output_file:
        json.dump({"benchmarks": merged_benchmarks, 'context': merged_contexts}, output_file, indent=4)

if __name__ == "__main__":
    main()
