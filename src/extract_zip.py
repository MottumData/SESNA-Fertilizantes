import zipfile
import shutil
import os

data_dir = os.path.join(os.getcwd(), 'data')

zip_file = next((fname for fname in os.listdir(data_dir) if fname.endswith('.zip')), None)
if zip_file is not None:
    zip_path = os.path.join(data_dir, zip_file)
    print(zip_file)

    # Extract the specific file from the zip file
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extract('AGEEML_2024411942621.csv', data_dir)  # replace 'your_file_name.txt' with the name of the file you want to extract

    # Find the CSV file and rename it
    csv_file = next((fname for fname in os.listdir(data_dir) if fname.endswith('.csv')), None)
    if csv_file is not None:
        csv_path = os.path.join(data_dir, csv_file)
        new_csv_path = os.path.join(data_dir, 'dataset_inegi.csv')
        shutil.move(csv_path, new_csv_path)

# TODO: Revisión para extraer los datos de INEGI