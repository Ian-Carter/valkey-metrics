import csv

# Input and output file names
input_file = "valkey_project_9.0.tsv"
output_file = "valkey_project_9.0.csv"

# Open TSV and write CSV
with open(input_file, "r", newline="", encoding="utf-8") as tsvfile, \
     open(output_file, "w", newline="", encoding="utf-8") as csvfile:
    reader = csv.reader(tsvfile, delimiter="\t")
    writer = csv.writer(csvfile, delimiter=",")
    for row in reader:
        writer.writerow(row)

print(f"Converted {input_file} → {output_file}")
