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


# Definición de funciones

def load_datasets(directory):
    # Get a list of all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory, '*.csv'))

    dataframes = []
    for file in csv_files:
        try:
            # Try to read the CSV file with cp1252 encoding
            df = pd.read_csv(file, encoding='cp1252', index_col=0, skiprows=1)
        except UnicodeDecodeError:
            # If it fails, try to read the CSV file with utf-8 encoding
            df = pd.read_csv(file, encoding='utf-8', index_col=0, skiprows=1)
        
        # Print the columns of the current DataFrame
        print(f"Columns in {file}: {df.columns.tolist()}")
        
        dataframes.append(df)

    # Concatenate all DataFrames in the list
    merged_df = pd.concat(dataframes, ignore_index=True)

    return merged_df

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


# Crear una función para encontrar la mejor coincidencia difusa con límites entre 90 y 100 de coincidencia
def fuzzy_merge_prod(df_inegi, df_prod, key1, key2, threshold=96, limit=1):
    """
    df_inegi: DataFrame de la izquierda (el DataFrame principal)
    df_prod: DataFrame de la derecha (el DataFrame con el que se quiere hacer el join)
    key1: Columna de la clave en df_inegi
    key2: Columna de la clave en df_prod
    threshold: Umbral de coincidencia difusa
    limit: Número de coincidencias a encontrar
    """
    s = df_prod[key2].tolist()

    # Encontrar las mejores coincidencias para cada clave en df_inegi
    matches = df_inegi[key1].apply(lambda x: process.extractOne(x, s, score_cutoff=threshold))

    # Crear una columna con las mejores coincidencias
    df_inegi['best_match'] = [match[0] if match else None for match in matches]
    df_inegi['match_score'] = [match[1] if match else None for match in matches]

    # Hacer el merge con las mejores coincidencias
    df_merged = pd.merge(df_inegi, df_prod, left_on='best_match', right_on=key2, how='inner',
                         suffixes=('_inegi', '_prod'))

    return df_merged


# Crear una función para encontrar la mejor coincidencia difusa con límites entre 90 y 100 de coincidencia
def fuzzy_merge_benef2023(df_benef, df_inegi, key1, key2, threshold=90, limit=1):
    s = df_inegi[key2].tolist()

    # Encontrar las mejores coincidencias para cada clave en df_inegi
    matches = df_benef[key1].apply(lambda x: process.extractOne(x, s, score_cutoff=threshold))

    # Crear una columna con las mejores coincidencias
    df_benef['best_match'] = [match[0] if match else None for match in matches]

    df_benef['match_score'] = [match[1] if match else None for match in matches]

    # Hacer el merge con las mejores coincidencias
    df_merged = pd.merge(df_benef, df_inegi, left_on='best_match', right_on=key2, how='left',
                         suffixes=('_benef', '_inegi'))

    return df_merged


def drop_columns(df, columns_to_drop):
    df = df.drop(columns_to_drop, axis=1)
    return df


def drop_duplicates(df):
    df = df.drop_duplicates()
    return df


def save_to_csv(df, filename):
    df.to_csv(filename, index=False)


def clean_inegi_data(dataset_inegi):
    COLUMNS_TO_DROP = ['MAPA', 'Estatus', 'NOM_ABR', 'CVE_LOC', 'NOM_LOC', 'AMBITO', 'LATITUD', 'LONGITUD',
                       'LAT_DECIMAL', 'LON_DECIMAL', 'ALTITUD', 'CVE_CARTA', 'POB_TOTAL',
                       'POB_MASCULINA', 'POB_FEMENINA', 'TOTAL DE VIVIENDAS HABITADAS']
    dataset_inegi = drop_columns(dataset_inegi, COLUMNS_TO_DROP)

    dataset_inegi_clean = drop_duplicates(dataset_inegi)

    # Crear una columna única para la clave de municipio
    dataset_inegi_clean['CVE_MUN_Unique'] = dataset_inegi_clean['CVE_ENT'].astype(str) + '-' + dataset_inegi_clean[
        'CVE_MUN'].astype(str)

    # Estandarizamos la limpieza de los datos en el dataset de INEGI
    dataset_inegi_clean['NOM_ENT_Clean'] = dataset_inegi_clean['NOM_ENT'].apply(clean_text)
    dataset_inegi_clean['NOM_MUN_Clean'] = dataset_inegi_clean['NOM_MUN'].apply(clean_text)

    dataset_inegi_clean["NOM_ENT_Clean"] = dataset_inegi_clean["NOM_ENT_Clean"].astype(str)
    dataset_inegi_clean["NOM_MUN_Clean"] = dataset_inegi_clean["NOM_MUN_Clean"].astype(str)
    dataset_inegi_clean['CVE_ENT'] = dataset_inegi_clean['CVE_ENT'].astype(str)
    dataset_inegi_clean['CVE_MUN'] = dataset_inegi_clean['CVE_MUN'].astype(str)

    return dataset_inegi_clean


def clean_productores_and_benef_data(listado_productores=None, listado_beneficiarios=None):
    if listado_productores is not None:
        # Existing cleaning logic for productores
        Estados_productores = listado_productores[['ESTADO', 'MUNICIPIO']]
        Estados_productores = drop_duplicates(Estados_productores)
        Estados_productores['ESTADO_Clean'] = Estados_productores['ESTADO'].apply(clean_text)
        Estados_productores['MUNICIPIO_Clean'] = Estados_productores['MUNICIPIO'].apply(clean_text)
        Estados_productores["ESTADO_Clean"] = Estados_productores["ESTADO_Clean"].astype(str)
        Estados_productores["MUNICIPIO_Clean"] = Estados_productores["MUNICIPIO_Clean"].astype(str)
        Estados_productores["KEY_prod"] = Estados_productores["ESTADO_Clean"] + "-" + Estados_productores[
            "MUNICIPIO_Clean"]

        Estados_productores = Estados_productores.drop(['ESTADO', 'MUNICIPIO'], axis=1)
        Estados_productores = drop_duplicates(Estados_productores)

        # Additional logic for processing productores data...
        return Estados_productores

    if listado_beneficiarios is not None:
        # Similar cleaning logic for beneficiarios, adjusted as necessary
        # Example:
        Estados_beneficiarios = listado_beneficiarios[['ESTADO', 'MUNICIPIO']]
        Estados_beneficiarios = drop_duplicates(Estados_beneficiarios)
        Estados_beneficiarios['ESTADO_Clean'] = Estados_beneficiarios['ESTADO'].apply(clean_text)
        Estados_beneficiarios['MUNICIPIO_Clean'] = Estados_beneficiarios['MUNICIPIO'].apply(clean_text)
        Estados_beneficiarios["ESTADO_Clean"] = Estados_beneficiarios["ESTADO_Clean"].astype(str)
        Estados_beneficiarios["MUNICIPIO_Clean"] = Estados_beneficiarios["MUNICIPIO_Clean"].astype(str)
        Estados_beneficiarios["KEY_benef"] = Estados_beneficiarios["ESTADO_Clean"] + "-" + Estados_beneficiarios[
            "MUNICIPIO_Clean"]

        Estados_beneficiarios = Estados_beneficiarios.drop(['ESTADO', 'MUNICIPIO'], axis=1)
        Estados_beneficiarios = drop_duplicates(Estados_beneficiarios)
        # Additional logic for processing beneficiarios data...
        return Estados_beneficiarios


def data_cleaning():
    path_dataset_inegi = 'data/inegi/dataset_inegi.csv'
    dataset_inegi = pd.read_csv(path_dataset_inegi, encoding='cp1252' , dtype={'CVE_ENT': str, 'CVE_MUN': str})

    listado_productores = load_datasets('data/productores_autorizados')

    stats = {
        'Número de filas': [listado_productores.shape[0]],
        'Número de columnas': [listado_productores.shape[1]],
        # Add more statistics here if needed
    }
    stats_df = pd.DataFrame(stats)

    stats_df.to_csv('data/productores_autorizados/diccionarios_E1/stats_iniciales_productores.csv', index=False)

    dataset_inegi_clean = clean_inegi_data(dataset_inegi)

    listado_productores = listado_productores.drop(columns=['Unnamed: 8', 'Unnamed: 9', 'Unnamed: 10'])

    # Solo las dos primeras columnas de lista_productores.
    Estados_productores = clean_productores_and_benef_data(listado_productores=listado_productores,
                                                           listado_beneficiarios=None)
    # Primero creemos una columna clave en cada dataset -> INEGI
    dataset_inegi_clean["KEY_inegi"] = dataset_inegi_clean["NOM_ENT_Clean"] + "-" + dataset_inegi_clean["NOM_MUN_Clean"]

    # Aplicar la función de coincidencia difusa
    diccionario = fuzzy_merge_prod(dataset_inegi_clean, Estados_productores, 'KEY_inegi', 'KEY_prod')

    diccionario = diccionario[['CVE_ENT', 'NOM_ENT', 'CVE_MUN', 'NOM_MUN', 'KEY_prod']]
    #diccionario['CVE_ENT'] = diccionario['CVE_ENT'].astype(str)
    #diccionario['CVE_MUN'] = diccionario['CVE_MUN'].astype(str)
    save_to_csv(diccionario, 'data/productores_autorizados/diccionarios_E1/diccionario_prod.csv')

    # Crear una variable KEY en listado de productores y el diccionario para hacer el join
    listado_productores['ESTADO_Clean'] = listado_productores['ESTADO'].apply(clean_text)
    listado_productores['MUNICIPIO_Clean'] = listado_productores['MUNICIPIO'].apply(clean_text)
    listado_productores['Estado-mun-KEY'] = listado_productores['ESTADO_Clean'].astype(str) + '-' + listado_productores[
        'MUNICIPIO_Clean'].astype(str)
    
    diccionario_Sin_VC = diccionario[diccionario["NOM_ENT"] != "Veracruz de Ignacio de la Llave"]
    
    diccionario_Sin_VC.to_csv('data/productores_autorizados/diccionarios_E1/diccionario_prod_sin_VERACRUZ.csv', index=False)

    diccionario_manipulado = pd.read_csv('data/productores_autorizados/diccionarios_E1/diccionario_prod_sin_VERACRUZ.csv' , dtype={'CVE_ENT': str, 'CVE_MUN': str})

    listado_productores_complete = pd.merge(listado_productores, diccionario_manipulado, left_on="Estado-mun-KEY",
                                            right_on="KEY_prod", how='left', suffixes=('_prod', '_inegi'))
    
    print(listado_productores_complete['CVE_ENT'].unique())

    # listado_productores_complete[['CVE_ENT', 'CVE_MUN']] = listado_productores_complete['CVE_MUN_Unique'].str.split('-',expand=True)
    listado_productores_complete = listado_productores_complete[
        ['ESTADO', 'MUNICIPIO', 'NOM_MUN', 'NOM_ENT', 'CVE_ENT', 'CVE_MUN', 'ACUSE', 'APELLIDO PATERNO',
         'APELLIDO MATERNO', 'NOMBRE (S)', 'PAQUETE', 'KEY_prod']]
    print(listado_productores_complete['CVE_ENT'].unique())
    # Los nombres y apellidos paternos y maternos que están vacíos y tengan número de acuse se reemplazarán por 'unknown'
    listado_productores_complete.loc[(listado_productores_complete['APELLIDO PATERNO'].isna()) & (
        listado_productores_complete['ACUSE'].notna()), 'APELLIDO PATERNO'] = 'unknown'
    listado_productores_complete.loc[(listado_productores_complete['APELLIDO MATERNO'].isna()) & (
        listado_productores_complete['ACUSE'].notna()), 'APELLIDO MATERNO'] = 'unknown'
    listado_productores_complete.loc[
        (listado_productores_complete['NOMBRE (S)'].isna()) & (
            listado_productores_complete['ACUSE'].notna()), 'NOMBRE (S)'] = 'unknown'
    
    
    listado_productores_complete = listado_productores_complete.astype({
        'ESTADO': 'str',
        'MUNICIPIO': 'str',
        'ACUSE': 'str',
        'APELLIDO PATERNO': 'str',
        'APELLIDO MATERNO': 'str',
        'NOMBRE (S)': 'str',
        'PAQUETE': 'int',
        'NOM_MUN': 'str',
        'NOM_ENT': 'str',
        'CVE_MUN': 'str',
        'CVE_ENT': 'str',
        'KEY_prod': 'str'

    })
    

    listado_productores_complete = listado_productores_complete.rename(columns={
        'ESTADO': 'estado1',
        'MUNICIPIO': 'municipio1',
        'ACUSE': 'acuse',
        'APELLIDO PATERNO': 'apellido_paterno',
        'APELLIDO MATERNO': 'apellido_materno',
        'NOMBRE (S)': 'nombre_propio',
        'PAQUETE': 'paquete',
        'NOM_MUN': 'municipio',
        'NOM_ENT': 'entidad',
        'CVE_MUN': 'cve_mun',
        'CVE_ENT': 'cve_ent',
        'KEY_prod': 'key_prod'
    })

    listado_productores_complete = listado_productores_complete.drop(columns=['estado1', 'municipio1'])

    listado_productores_complete['id'] = listado_productores_complete.index

    # Assuming df is your DataFrame
    ordered_columns = ['id', 'cve_ent', 'entidad', 'cve_mun', 'municipio', 'acuse', 'apellido_paterno',
                       'apellido_materno', 'nombre_propio', 'paquete', 'key_prod']
    listado_productores_complete = listado_productores_complete.reindex(columns=ordered_columns)

    listado_productores_complete['cve_ent'] = listado_productores_complete['cve_ent'].str.zfill(2)
    listado_productores_complete['cve_mun'] = listado_productores_complete['cve_mun'].str.zfill(3)

    save_to_csv(listado_productores_complete, 'data/listados_completos/listado_productores_complete2023.csv')


def data_cleaning2():
    path_dataset_inegi = 'data/inegi/dataset_inegi.csv'
    dataset_inegi = pd.read_csv(path_dataset_inegi, encoding='cp1252' , dtype={'CVE_ENT': str, 'CVE_MUN': str})

    listado_beneficiarios = load_datasets('data/productores_beneficiarios')

    dataset_inegi_clean = clean_inegi_data(dataset_inegi)

    Estados_beneficiarios = clean_productores_and_benef_data(listado_productores=None,
                                                             listado_beneficiarios=listado_beneficiarios)

    dataset_inegi_clean["KEY_inegi"] = dataset_inegi_clean["NOM_ENT_Clean"] + "-" + dataset_inegi_clean["NOM_MUN_Clean"]

    print(Estados_beneficiarios)

    Estados_beneficiarios = Estados_beneficiarios.drop_duplicates(subset='KEY_benef')

    diccionario = fuzzy_merge_benef2023(Estados_beneficiarios, dataset_inegi_clean, 'KEY_benef', 'KEY_inegi')

    diccionario = diccionario[['CVE_ENT', 'NOM_ENT', 'CVE_MUN', 'NOM_MUN', 'KEY_benef']]
    diccionario['CVE_ENT'] = diccionario['CVE_ENT'].astype(str)
    diccionario['CVE_MUN'] = diccionario['CVE_MUN'].astype(str)
    print(diccionario['CVE_ENT'].unique())

    diccionario.drop_duplicates(subset=['KEY_benef'], inplace=True)

    save_to_csv(diccionario, 'data/productores_beneficiarios/diccionarios_E2/diccionario_benef.csv')

    listado_beneficiarios['ESTADO_Clean'] = listado_beneficiarios['ESTADO'].apply(clean_text)
    listado_beneficiarios['MUNICIPIO_Clean'] = listado_beneficiarios['MUNICIPIO'].apply(clean_text)
    listado_beneficiarios['Estado-mun-KEY'] = listado_beneficiarios['ESTADO_Clean'].astype(str) + '-' + \
                                              listado_beneficiarios['MUNICIPIO_Clean'].astype(str)

    diccionario_verificado_simple = pd.read_csv('data/productores_beneficiarios/diccionarios_E2/Diccionario_Simple.csv')

    listado_beneficiarios_parte_I = pd.merge(listado_beneficiarios, diccionario_verificado_simple,
                                             left_on="Estado-mun-KEY", right_on="KEY_benef", how='left',
                                             suffixes=('_benef', '_inegi'))
    listado_beneficiarios_parte_II = pd.merge(listado_beneficiarios_parte_I, dataset_inegi_clean,
                                              left_on="KEY_benef_Verificado", right_on="KEY_inegi", how='left',
                                              suffixes=('_benef', '_inegi'))
    listado_beneficiarios_parte_II = listado_beneficiarios_parte_II.drop_duplicates(subset=['ACUSE ESTATAL'],
                                                                                    keep='first')
    listado_beneficiarios_parte_II = listado_beneficiarios_parte_II[
        ['ESTADO', 'MUNICIPIO', 'ACUSE ESTATAL', 'APELLIDO PATERNO', 'APELLIDO MATERNO', 'NOMBRE (S)', 'PAQUETE',
         'KEY_benef_Verificado', 'NOM_ENT', 'NOM_MUN', 'CVE_ENT', 'CVE_MUN']]

    listado_beneficiarios_parte_II = listado_beneficiarios_parte_II.astype({
        'ESTADO': 'str',
        'MUNICIPIO': 'str',
        'ACUSE ESTATAL': 'str',
        'APELLIDO PATERNO': 'str',
        'APELLIDO MATERNO': 'str',
        'NOMBRE (S)': 'str',
        'NOM_MUN': 'str',
        'NOM_ENT': 'str',
        'CVE_MUN': 'str',
        'CVE_ENT': 'str',
        'KEY_benef_Verificado': 'str'

    })

    listado_beneficiarios_parte_II = listado_beneficiarios_parte_II.rename(columns={
        'ESTADO': 'estado1',
        'MUNICIPIO': 'municipio1',
        'ACUSE ESTATAL': 'acuse',
        'APELLIDO PATERNO': 'apellido_paterno',
        'APELLIDO MATERNO': 'apellido_materno',
        'NOMBRE (S)': 'nombre_propio',
        'PAQUETE': 'paquete',
        'NOM_MUN': 'municipio',
        'NOM_ENT': 'entidad',
        'CVE_MUN': 'cve_mun',
        'CVE_ENT': 'cve_ent',
        'KEY_benef_Verificado': 'key_benef_verificado'
    })

    listado_beneficiarios_parte_II = listado_beneficiarios_parte_II.drop(columns=['estado1', 'municipio1'])

    listado_beneficiarios_parte_II['id'] = listado_beneficiarios_parte_II.index
    # Assuming df is your DataFrame
    ordered_columns = ['id', 'cve_ent', 'entidad', 'cve_mun', 'municipio', 'acuse', 'apellido_paterno',
                       'apellido_materno', 'nombre_propio', 'paquete', 'key_benef_verificado']
    listado_beneficiarios_parte_II = listado_beneficiarios_parte_II.reindex(columns=ordered_columns)

    listado_beneficiarios_parte_II['cve_ent'] = listado_beneficiarios_parte_II['cve_ent'].str.zfill(2)
    listado_beneficiarios_parte_II['cve_mun'] = listado_beneficiarios_parte_II['cve_mun'].str.zfill(3)

    listado_beneficiarios_parte_II.to_csv('data/listados_completos/listado_beneficiarios_2023.csv', index=False)


def main():
    data_cleaning2()


if __name__ == "__main__":
    main()
