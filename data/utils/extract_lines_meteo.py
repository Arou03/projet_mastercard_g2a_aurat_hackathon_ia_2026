import csv
import os
import glob

YEAR_FILTER = ('2020', '2021', '2022', '2023', '2024')

input_dir = './wrk/input/meteo_france'
output_dir = './wrk/tmp/meteo_france'

os.makedirs(input_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)

csv_files = glob.glob(os.path.join(input_dir, '*.csv'))

if not csv_files:
    print(f"No CSV files found in '{input_dir}'.")
    print("If the folder was just created, please place your downloaded CSV files inside it and run the script again.")
else:
    print(f"Found {len(csv_files)} CSV file(s). Starting extraction...\n")
    
    for input_path in csv_files:
        filename = os.path.basename(input_path)
        output_path = os.path.join(output_dir, filename)
        
        print(f"Processing: {filename}...")
        
        with open(input_path, mode='r', encoding='utf-8') as infile, \
             open(output_path, mode='w', encoding='utf-8', newline='') as outfile:
            
            reader = csv.reader(infile, delimiter=';')
            writer = csv.writer(outfile, delimiter=';')
            
            try:
                header = next(reader)
            except StopIteration:
                print(f"  -> Skipping {filename}: File is empty.")
                continue
                
            writer.writerow(header)
            
            try:
                date_idx = header.index('AAAAMMJJ')
            except ValueError:
                print(f"  -> Skipping {filename}: 'AAAAMMJJ' column not found.")
                continue
                
            extracted_count = 0
            for row in reader:
                if len(row) > date_idx:
                    date_str = row[date_idx]
                    
                    # On passe directement la constante (qui est déjà un tuple)
                    if date_str.startswith(YEAR_FILTER):
                        writer.writerow(row)
                        extracted_count += 1
                        
            print(f"  -> Extracted {extracted_count} lines to {output_dir}/{filename}")

    print("\nAll files processed successfully!")