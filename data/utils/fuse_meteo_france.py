import csv
import os
import glob


input_dir = './wrk/tmp/meteo_france'
output_dir = './wrk/output/meteo_france'


output_file = os.path.join(output_dir, 'meteo_france_concat.csv')


os.makedirs(input_dir, exist_ok=True)
os.makedirs(output_dir, exist_ok=True)


csv_files = glob.glob(os.path.join(input_dir, '*.csv'))

if not csv_files:
    print(f"Aucun fichier CSV trouvé dans '{input_dir}'.")
    print("Veuillez y placer vos fichiers (ex: Q_01_previous-1950-2024_RR-T-Vent.csv) et relancer le script.")
else:
    print(f"{len(csv_files)} fichier(s) trouvé(s). Début de la concaténation...\n")
    
    
    with open(output_file, mode='w', encoding='utf-8', newline='') as outfile:
        writer = csv.writer(outfile, delimiter=';')
        
        header_written = False
        total_lines = 0
        
        
        for file_path in csv_files:
            filename = os.path.basename(file_path)
            print(f"Traitement de : {filename}...")
            
            
            
            parts = filename.split('_')
            if len(parts) >= 2:
                dept_code = parts[1]
            else:
                dept_code = "INCONNU" 
                
            
            with open(file_path, mode='r', encoding='utf-8') as infile:
                reader = csv.reader(infile, delimiter=';')
                
                
                try:
                    header = next(reader)
                except StopIteration:
                    print(f"  -> Fichier ignoré : {filename} est vide.")
                    continue
                
                
                if not header_written:
                    header.append('departement_code') 
                    writer.writerow(header)
                    header_written = True
                
                
                lines_in_file = 0
                for row in reader:
                    
                    if len(row) > 0:
                        row.append(dept_code) 
                        writer.writerow(row)
                        lines_in_file += 1
                        total_lines += 1
                        
            print(f"  -> {lines_in_file} lignes ajoutées (Département : {dept_code})")

    print(f"\nConcaténation terminée avec succès !")
    print(f"Fichier final généré : {output_file} (Total : {total_lines} lignes de données)")