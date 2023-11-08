from collections import Counter

# Function to classify entries as common or uncommon
def classify_entries(entries, threshold):
    common_entries = set()
    uncommon_entries = set()
    common_perc = 0
    uncommon_perc = 0
    
    for entry, count in entries.items():
        if count >= threshold:
            common_entries.add(entry)
            common_perc += 1
        else:
            uncommon_entries.add(entry)
            uncommon_perc += 1
    
    return common_entries, uncommon_entries, common_perc*10**(-6), uncommon_perc*10**(-6)

# Specify the path to your input file
input_file = "output/log.out"

# Set your threshold for classification (e.g., entries appearing 100 times or more are common)
threshold = 100

# Read the entries from the file and count their occurrences
entry_counts = Counter()

with open(input_file, "r") as file:
    for line in file:
        entry = int(line.strip())
        entry_counts[entry] += 1

# Classify entries
common_entries, uncommon_entries, common_perc, uncommon_perc = classify_entries(entry_counts, threshold)

# Print the results
print(f"Common Entries % [20]: {len(common_entries)*10**(-6)}")
print(f"Uncommon Entries % [80]: {len(uncommon_entries)*10**(-6)}")

print(f'% probability of ending up in a common entry [80]: {common_perc}')
print(f'% probability of ending up in an uncommon entry [20]: {uncommon_perc}')
