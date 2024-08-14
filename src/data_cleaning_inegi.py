import pandas as pd
import seaborn as sns
from thefuzz import fuzz
from thefuzz import process
import matplotlib.pyplot as plt
import numpy as np
import os
import glob
import re
import unidecode


def clean_text(text):
    """
    De esta manera tenemos el texto sin espacios blancos extra y sobre todo con todas las palabras con capitalización correcta.
    """
    if pd.isna(text):
        return text
    text = text.strip()  # Eliminate white spaces
    text = text.lower()  # Convert to lowercase
    text = unidecode.unidecode(text)  # Remove accents
    text = re.sub('-.*-', '', text)
    text = re.sub('\s+', ' ', text)  # Eliminate extra white spaces
    text = re.sub('^\s+|\s+?$', '', text)  # Eliminate spaces at the beginning and end
    return text


def load_dataset(path, encoding='utf-8'):
    return pd.read_csv(path, encoding=encoding, dtype={'CVE_ENT': str, 'CVE_MUN': str})


def drop_columns(dataset, columns):
    return dataset.drop(columns, axis=1)


def remove_duplicates(dataset):
    return dataset.drop_duplicates()


def clean_text_column(dataset, columns):
    for column in columns:
        dataset[f'{column}_Clean'] = dataset[column].apply(clean_text)
    return dataset


def rename_columns(dataset):
    return dataset.rename(columns={
        'CVE_ENT': 'CVE_ENT',
        'NOM_ENT': 'Entidad_inegi',
        'CVE_MUN': 'CVE_MUN',
        'NOM_MUN': 'Municipio_inegi',
        'CVE_LOC': 'CVE_LOC',
        'NOM_LOC': 'Localidad_inegi',
        'NOM_ENT_Clean': 'Entidad_c_inegi',
        'NOM_MUN_Clean': 'Municipio_c_inegi',
        'NOM_LOC_Clean': 'Localidad_c_inegi'
    })


def save_dataset(dataset, path):
    dataset.to_csv(path, index=False)


def clean_inegi():
    COLUMNS_TO_DROP = ['MAPA', 'Estatus', 'NOM_ABR', 'AMBITO', 'LATITUD', 'LONGITUD',
                       'LAT_DECIMAL', 'LON_DECIMAL', 'ALTITUD', 'CVE_CARTA', 'POB_MASCULINA',
                       'POB_FEMENINA', 'TOTAL DE VIVIENDAS HABITADAS']

    columns_to_clean = ['NOM_ENT', 'NOM_MUN', 'NOM_LOC']

    paths = {
        '2019': 'data/inegi/dataset_inegi_2019.csv',
        '2020': 'data/inegi/dataset_inegi_2020.csv',
        '2021': 'data/inegi/dataset_inegi_2021.csv',
        '2022': 'data/inegi/dataset_inegi_2022.csv'
    }

    for year, path in paths.items():
        dataset = load_dataset(path, encoding='utf-8')
        dataset = drop_columns(dataset, COLUMNS_TO_DROP)
        dataset = remove_duplicates(dataset)
        dataset = clean_text_column(dataset, columns_to_clean)
        dataset = rename_columns(dataset)
        save_dataset(dataset, f'data/inegi/dataset_inegi_clean_{year}.csv')

    # Procesar el dataset principal
    dataset_inegi = load_dataset('data/inegi/dataset_inegi.csv', encoding='cp1252')
    dataset_inegi = drop_columns(dataset_inegi, COLUMNS_TO_DROP)
    dataset_inegi = remove_duplicates(dataset_inegi)
    dataset_inegi = clean_text_column(dataset_inegi, columns_to_clean)
    dataset_inegi = rename_columns(dataset_inegi)
    save_dataset(dataset_inegi, 'data/inegi/dataset_inegi_clean.csv')


def main():
    clean_inegi()


if __name__ == "__main__":
    main()
