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


# Crear una función para encontrar la mejor coincidencia difusa con límites entre 85 y 100 de coincidencia
def fuzzy_merge_benef2019_2022(df_benef, df_inegi, key1, key2, threshold=85, limit=1):
    """
    df_inegi: DataFrame de la izquierda (el DataFrame principal)
    df_prod: DataFrame de la derecha (el DataFrame con el que se quiere hacer el join)
    key1: Columna de la clave en df_inegi
    key2: Columna de la clave en df_prod
    threshold: Umbral de coincidencia difusa
    limit: Número de coincidencias a encontrar
    """
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


def clean_text_inegi(text):
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


def drop_columns_inegi(dataset_inegi):
    # Eliminamos las columnas que no son de interés
    COLUMNS_TO_DROP = ['MAPA', 'Estatus', 'NOM_ABR', 'AMBITO', 'LATITUD', 'LONGITUD',
                       'LAT_DECIMAL', 'LON_DECIMAL', 'ALTITUD', 'CVE_CARTA', 'POB_MASCULINA',
                       'POB_FEMENINA', 'TOTAL DE VIVIENDAS HABITADAS']
    dataset_inegi = dataset_inegi.drop(COLUMNS_TO_DROP, axis=1, inplace=True)


def dataset_cleaning_inegi(dataset):
    dataset['NOM_ENT_Clean'] = dataset['NOM_ENT'].apply(clean_text_inegi)
    dataset['NOM_MUN_Clean'] = dataset['NOM_MUN'].apply(clean_text_inegi)
    dataset['NOM_LOC_Clean'] = dataset['NOM_LOC'].apply(clean_text_inegi)


def inegi_rename_columns(dataset):
    # Renombrar las columnas
    dataset.rename(columns={
        'CVE_ENT': 'CVE_ENT',
        'NOM_ENT': 'Entidad_inegi',
        'CVE_MUN': 'CVE_MUN',
        'NOM_MUN': 'Municipio_inegi',
        'CVE_LOC': 'CVE_LOC',
        'NOM_LOC': 'Localidad_inegi',
        'NOM_ENT_Clean': 'Entidad_c_inegi',
        'NOM_MUN_Clean': 'Municipio_c_inegi',
        'NOM_LOC_Clean': 'Localidad_c_inegi'
    }, inplace=True)


def create_listados_por_entidad(listado_beneficiarios, entities, prefix, dataframes_dict={}):
    for entity in entities:
        variable_name = f"Localidades_{prefix}_{entity.upper().replace(' ', '_')}"
        dataframe = listado_beneficiarios[listado_beneficiarios["ENTIDAD"] == entity]

        # Check if the DataFrame for this entity already exists in the dictionary
        if variable_name in dataframes_dict:
            # If it exists, concatenate the new DataFrame with the existing one
            dataframes_dict[variable_name] = pd.concat([dataframes_dict[variable_name], dataframe])
        else:
            # If it does not exist, create a new entry in the dictionary
            dataframes_dict[variable_name] = dataframe
        # Define each DataFrame as a separate variable
    for name, df in dataframes_dict.items():
        print(f"{name} defined with shape: {df.shape}")

    return dataframes_dict


def load_inegi_uniqueloc(folder_path):
    """
    Reads all CSV files in the specified folder and returns them as a dictionary of DataFrames.

    Parameters:
    - folder_path: The path to the folder containing the CSV files.

    Returns:
    - A dictionary where keys are the names of the CSV files (without the '.csv' extension) and values are the corresponding DataFrames.
    """
    dataframes_dict = {}

    # List all files in the folder
    files = os.listdir(folder_path)

    # Filter only CSV files
    csv_files = [file for file in files if file.endswith('.csv')]

    # Read each CSV file and add to the dictionary
    for file_name in csv_files:
        file_path = os.path.join(folder_path, file_name)
        # Create a variable name dynamically by removing the '.csv' extension
        var_name = file_name.replace('.csv', '')
        # Read the CSV file and add the DataFrame to the dictionary
        dataframes_dict[var_name] = pd.read_csv(file_path)

    # Optionally print the names of the variables created and the shape of each DataFrame
    for var_name, df in dataframes_dict.items():
        print(f'{var_name}: {df.shape[0]} rows, {df.shape[1]} columns')

    return dataframes_dict


def cleaning_inegi():
    dataset_inegi_clean_2019 = pd.read_csv('data/inegi/dataset_inegi_2019.csv')
    dataset_inegi_clean_2020 = pd.read_csv('data/inegi/dataset_inegi_2020.csv')
    dataset_inegi_clean_2021 = pd.read_csv('data/inegi/dataset_inegi_2021.csv')
    dataset_inegi_clean_2022 = pd.read_csv('data/inegi/dataset_inegi_2022.csv')

    drop_columns_inegi(dataset_inegi_clean_2019)
    drop_columns_inegi(dataset_inegi_clean_2020)
    drop_columns_inegi(dataset_inegi_clean_2021)
    drop_columns_inegi(dataset_inegi_clean_2022)

    dataset_cleaning_inegi(dataset_inegi_clean_2019)
    dataset_cleaning_inegi(dataset_inegi_clean_2020)
    dataset_cleaning_inegi(dataset_inegi_clean_2021)
    dataset_cleaning_inegi(dataset_inegi_clean_2022)

    inegi_rename_columns(dataset_inegi_clean_2019)
    inegi_rename_columns(dataset_inegi_clean_2020)
    inegi_rename_columns(dataset_inegi_clean_2021)
    inegi_rename_columns(dataset_inegi_clean_2022)

    dataset_inegi_clean_2019.to_csv('data/inegi/dataset_inegi_clean_2019.csv', index=False)
    dataset_inegi_clean_2020.to_csv('data/inegi/dataset_inegi_clean_2020.csv', index=False)
    dataset_inegi_clean_2021.to_csv('data/inegi/dataset_inegi_clean_2021.csv', index=False)
    dataset_inegi_clean_2022.to_csv('data/inegi/dataset_inegi_clean_2022.csv', index=False)


def data_cleaning3(dataset_inegi, dataset_benef, prefix):
    dataset_inegi_clean = pd.read_csv(dataset_inegi, dtype={'CVE_ENT': str, 'CVE_MUN': str, 'CVE_LOC': str})
    dataset_benef = pd.read_csv(dataset_benef)

    cleaning_inegi()

    dataset_inegi_clean['CVE_ENT'] = dataset_inegi_clean['CVE_ENT'].astype(str).str.zfill(2)
    dataset_inegi_clean['CVE_MUN'] = dataset_inegi_clean['CVE_MUN'].astype(str).str.zfill(3)
    dataset_inegi_clean['CVE_LOC'] = dataset_inegi_clean['CVE_LOC'].astype(str).str.zfill(4)

    # Obtenemos las localidades únicas en el dataset.
    Municipios = dataset_benef[['ENTIDAD', 'MUNICIPIO']]
    Municipios = Municipios.drop_duplicates()

    Municipios['ENTIDAD_c_benef'] = Municipios['ENTIDAD'].apply(clean_text_inegi)
    Municipios['MUNICIPIO_c_benef'] = Municipios['MUNICIPIO'].apply(clean_text_inegi)

    Municipios['KEY_benef_mun'] = Municipios['ENTIDAD_c_benef'].astype(str) + '-' + Municipios[
        'MUNICIPIO_c_benef'].astype(str)

    dataset_inegi_clean['KEY_inegi_municipio'] = dataset_inegi_clean['Entidad_c_inegi'].astype(str) + '-' + \
                                                 dataset_inegi_clean[
                                                     'Municipio_c_inegi'].astype(str)
    dataset_inegi_clean['KEY_inegi_localidad'] = dataset_inegi_clean['Municipio_c_inegi'].astype(str) + '-' + \
                                                 dataset_inegi_clean['Localidad_c_inegi'].astype(str)

    INEGI_UNIQUEMUN = dataset_inegi_clean.drop(
        columns=["CVE_LOC", "Localidad_inegi", "Localidad_c_inegi", "KEY_inegi_localidad", "POB_TOTAL"])

    INEGI_UNIQUEMUN.drop_duplicates(subset='KEY_inegi_municipio', keep='first', inplace=True)

    diccionario_MUN = fuzzy_merge_benef2019_2022(Municipios, INEGI_UNIQUEMUN, 'KEY_benef_mun', 'KEY_inegi_municipio')

    diccionario_MUN.drop(columns=['ENTIDAD', 'MUNICIPIO', 'ENTIDAD_c_benef', 'MUNICIPIO_c_benef', 'Entidad_c_inegi',
                                  'Municipio_c_inegi'], inplace=True)
    diccionario_MUN.drop_duplicates()

    diccionario_MUN.to_csv(f'data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_MUN_{prefix}.csv',
                           index=False)

    diccionario_MUN_simple = pd.read_csv(
        f'data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_MUN_{prefix}_simple.csv')

    diccionario_MUN_simple = diccionario_MUN_simple.drop_duplicates(subset='KEY_benef_mun', keep='first')

    dataset_benef['ESTADO_Clean'] = dataset_benef['ENTIDAD'].apply(clean_text_inegi)
    dataset_benef['MUNICIPIO_Clean'] = dataset_benef['MUNICIPIO'].apply(clean_text_inegi)

    # Create KEY in listado beneficiarios
    dataset_benef['Estado-mun-KEY'] = dataset_benef['ESTADO_Clean'].astype(str) + '-' + dataset_benef[
        'MUNICIPIO_Clean'].astype(str)

    dataset_benef.dropna(inplace=True)

    diccionario_MUN_simple.drop_duplicates(inplace=True)

    listado_beneficiarios_parte_I = pd.merge(dataset_benef, diccionario_MUN_simple, left_on="Estado-mun-KEY",
                                             right_on="KEY_benef_mun", how='left', suffixes=('_benef', '_inegi'))
    listado_beneficiarios_parte_II = pd.merge(listado_beneficiarios_parte_I, INEGI_UNIQUEMUN,
                                              left_on="KEY_inegi_municipio",
                                              right_on="KEY_inegi_municipio", how='left', suffixes=('_benef', '_inegi'))

    print(listado_beneficiarios_parte_I.shape)
    print(listado_beneficiarios_parte_II.shape)

    listado_beneficiarios_parte_II = listado_beneficiarios_parte_II.drop(
        columns=['ESTADO_Clean', 'MUNICIPIO_Clean', 'Estado-mun-KEY', 'KEY_inegi_municipio', 'Entidad_c_inegi',
                 'Municipio_c_inegi', 'ESTRATIFICACIÓN', 'PROGRAMA', 'COMPONENTE', 'SUBCOMPONENTE', 'APOYO',
                 'ACTIVIDAD', 'ESLABÓN'])

    listado_beneficiarios_parte_II.to_csv(f'data/listados_completos/listado_beneficiarios_{prefix}.csv', index=False)

    listado_beneficiarios_parte_II['ENTIDAD_c_benef'] = listado_beneficiarios_parte_II['ENTIDAD'].apply(
        clean_text_inegi)
    listado_beneficiarios_parte_II['MUNICIPIO_c_benef'] = listado_beneficiarios_parte_II['MUNICIPIO'].apply(
        clean_text_inegi)
    listado_beneficiarios_parte_II['LOCALIDAD_c_benef'] = listado_beneficiarios_parte_II['LOCALIDAD'].apply(
        clean_text_inegi)

    listado_beneficiarios_parte_II['KEY_benef_loc'] = listado_beneficiarios_parte_II['MUNICIPIO_c_benef'].astype(
        str) + '-' + listado_beneficiarios_parte_II['LOCALIDAD_c_benef'].astype(str)
    print('listado_benef: ', listado_beneficiarios_parte_II.isna().sum())

    if prefix == 19:
        listado_beneficiarios_2019_match = pd.read_csv('data/listados_completos/listado_beneficiarios_2019_match.csv')

        listado_beneficiarios_2019_match = listado_beneficiarios_2019_match.drop(columns=['Unnamed: 0'])
        listado_beneficiarios_2019_match['ENTIDAD_c_benef'] = listado_beneficiarios_2019_match['ENTIDAD'].apply(
            clean_text_inegi)
        listado_beneficiarios_2019_match['MUNICIPIO_c_benef'] = listado_beneficiarios_2019_match['MUNICIPIO'].apply(
            clean_text_inegi)
        listado_beneficiarios_2019_match['LOCALIDAD_c_benef'] = listado_beneficiarios_2019_match['LOCALIDAD'].apply(
            clean_text_inegi)

        listado_beneficiarios_2019_match['KEY_benef_loc'] = listado_beneficiarios_2019_match[
                                                                'MUNICIPIO_c_benef'].astype(str) + '-' + \
                                                            listado_beneficiarios_2019_match[
                                                                'LOCALIDAD_c_benef'].astype(str)

        listado_beneficiarios_2019 = listado_beneficiarios_2019_match.copy()
        listado_beneficiarios_2019.drop_duplicates(subset=['KEY_benef_loc'], inplace=True)

        entities = [
            'Puebla', 'México', 'Guanajuato', 'Querétaro',
            'Zacatecas', 'Veracruz de Ignacio de la Llave', 'Hidalgo',
            'Michoacán de Ocampo', 'Oaxaca', 'Colima', 'Chiapas',
            'San Luis Potosí', 'Jalisco', 'Nayarit', 'GUERRERO'
        ]
        listados_beneficiarios_por_entidad = create_listados_por_entidad(listado_beneficiarios_2019, entities, 19)
        inegi_uniquelocs = load_inegi_uniqueloc('data/productores_beneficiarios 2019-2022/diccionarios_E3/2019')

        Localidades_19_GUERRERO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_GUERRERO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_guerrero'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_GUERRERO_parte_I.shape

        Localidades_19_PUEBLA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_PUEBLA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_puebla'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_PUEBLA_parte_I.shape

        Localidades_19_MEXICO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_MÉXICO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_méxico'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_MEXICO_parte_I.shape

        Localidades_19_GUANAJUATO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_GUANAJUATO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_guanajuato'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_GUANAJUATO_parte_I.shape

        Localidades_19_QUERETARO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_QUERÉTARO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_querétaro'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_QUERETARO_parte_I.shape

        Localidades_19_ZACATECAS_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_ZACATECAS'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_zacatecas'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_ZACATECAS_parte_I.shape

        Localidades_19_VERACRUZ_DE_IGNACIO_DE_LA_LLAVE_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_VERACRUZ_DE_IGNACIO_DE_LA_LLAVE'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_veracruz_de_ignacio_de_la_llave'], 'KEY_benef_loc',
            'KEY_inegi_localidad')
        Localidades_19_VERACRUZ_DE_IGNACIO_DE_LA_LLAVE_parte_I.shape

        Localidades_19_HIDALGO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_HIDALGO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_hidalgo'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_HIDALGO_parte_I.shape

        Localidades_19_MICHOACAN_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_MICHOACÁN_DE_OCAMPO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_michoacán_de_ocampo'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_MICHOACAN_parte_I.shape

        Localidades_19_OAXACA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_OAXACA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_oaxaca'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_OAXACA_parte_I.shape

        Localidades_19_COLIMA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_COLIMA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_colima'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_COLIMA_parte_I.shape

        Localidades_19_CHIAPAS_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_CHIAPAS'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_chiapas'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_CHIAPAS_parte_I.shape

        Localidades_19_POTOSI_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_SAN_LUIS_POTOSÍ'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_san_luis_potosí'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_POTOSI_parte_I.shape

        Localidades_19_JALISCO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_JALISCO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_jalisco'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_JALISCO_parte_I.shape

        Localidades_19_NAYARIT_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_19_NAYARIT'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2019_nayarit'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_19_NAYARIT_parte_I.shape

        # Concatenate datasets vertically
        diccionario_LOC_19 = pd.concat([
            Localidades_19_GUERRERO_parte_I,
            Localidades_19_PUEBLA_parte_I,
            Localidades_19_MEXICO_parte_I,
            Localidades_19_GUANAJUATO_parte_I,
            Localidades_19_QUERETARO_parte_I,
            Localidades_19_ZACATECAS_parte_I,
            Localidades_19_VERACRUZ_DE_IGNACIO_DE_LA_LLAVE_parte_I,
            Localidades_19_HIDALGO_parte_I,
            Localidades_19_MICHOACAN_parte_I,
            Localidades_19_OAXACA_parte_I,
            Localidades_19_COLIMA_parte_I,
            Localidades_19_CHIAPAS_parte_I,
            Localidades_19_POTOSI_parte_I,
            Localidades_19_JALISCO_parte_I,
            Localidades_19_NAYARIT_parte_I
        ], axis=0)

        diccionario_LOC_19.drop(columns=['BENEFICIARIO', 'ZONA', 'ENTIDAD', 'MUNICIPIO', 'LOCALIDAD', 'PRODUCTO',
                                         'FECHA', 'MONTO FEDERAL', 'CICLO AGRÍCOLA', 'ENTIDAD_c_benef',
                                         'MUNICIPIO_c_benef', 'LOCALIDAD_c_benef', 'best_match', 'CVE_ENT',
                                         'Entidad_inegi', 'CVE_MUN', 'Municipio_inegi',
                                         'CVE_LOC', 'Localidad_inegi', 'POB_TOTAL', 'Entidad_c_inegi',
                                         'Municipio_c_inegi', 'Localidad_c_inegi'], inplace=True)

        diccionario_LOC_19.to_csv('data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_19.csv',
                                  index=False)

        try:
            diccionario_LOC_19_simple = pd.read_csv(
                'data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_19_simple.csv',
                encoding='cp1252',
                delimiter=';')
            print(diccionario_LOC_19_simple.head())
        except pd.errors.ParserError as e:
            print("Error al leer el archivo CSV:", e)

        diccionario_LOC_19_simple.rename(columns={'ï»¿KEY_benef_loc': 'KEY_benef_loc'}, inplace=True)
        print('key: ', diccionario_LOC_19_simple.columns)
        print('here is the shape: ', listado_beneficiarios_2019_match.shape)

        listado_beneficiarios_2019_match['MUNICIPIO_Clean'] = listado_beneficiarios_2019_match['MUNICIPIO'].apply(
            clean_text_inegi)
        listado_beneficiarios_2019_match['LOCALIDAD_Clean'] = listado_beneficiarios_2019_match['LOCALIDAD'].apply(
            clean_text_inegi)

        # Create KEY in listado beneficiarios
        listado_beneficiarios_2019_match['Municipio-loc-KEY'] = listado_beneficiarios_2019_match[
                                                                    'MUNICIPIO_Clean'].astype(str) + '-' + \
                                                                listado_beneficiarios_2019_match[
                                                                    'LOCALIDAD_Clean'].astype(str)

        print('lietsado: ', listado_beneficiarios_2019_match.shape)
        print('diccionario: ', diccionario_LOC_19_simple.columns)

        diccionario_LOC_19_simple.drop_duplicates(inplace=True)

        listado_beneficiarios_parte_I_localidades = pd.merge(listado_beneficiarios_2019_match,
                                                             diccionario_LOC_19_simple, left_on="Municipio-loc-KEY",
                                                             right_on="KEY_benef_loc", how='left',
                                                             suffixes=('_benef', '_inegi'))

        listado_beneficiarios_parte_I_localidades.drop_duplicates(keep='first', inplace=True)

        print('lietsado: ', listado_beneficiarios_parte_I_localidades.shape)

        INEGI_UNIQUELOC_2019 = dataset_inegi_clean.drop(columns=['Entidad_c_inegi', 'Municipio_c_inegi',
                                                                 'Localidad_c_inegi', 'POB_TOTAL'])
        INEGI_UNIQUELOC_2019 = INEGI_UNIQUELOC_2019.drop_duplicates(subset='KEY_inegi_localidad', keep='first')

        listado_beneficiarios_parte_II_localidades = pd.merge(listado_beneficiarios_parte_I_localidades,
                                                              INEGI_UNIQUELOC_2019,
                                                              left_on="KEY_inegi_localidad",
                                                              right_on="KEY_inegi_localidad", how='left',
                                                              suffixes=('_benef', '_inegi'))

        listado_beneficiarios_parte_II_localidades.drop(columns=['ENTIDAD', 'MUNICIPIO', 'LOCALIDAD', 'ENTIDAD_c_benef',
                                                                 'MUNICIPIO_c_benef', 'LOCALIDAD_c_benef',
                                                                 'KEY_benef_loc_benef',
                                                                 'MUNICIPIO_Clean', 'LOCALIDAD_Clean',
                                                                 'Municipio-loc-KEY',
                                                                 'KEY_benef_loc_inegi', 'match_score',
                                                                 'KEY_inegi_localidad',
                                                                 'KEY_inegi_municipio'], inplace=True)

        listado_beneficiarios_parte_II_localidades.rename(columns={'BENEFICIARIO': 'Nombre del beneficiario',
                                                                   'ZONA': 'Zona del país',
                                                                   'CVE_ENT': 'Clave de entidad',
                                                                   'Entidad_inegi': 'Entidad federativa',
                                                                   'CVE_MUN': 'Clave de municipio',
                                                                   'Municipio_inegi': 'Municipio',
                                                                   'CVE_LOC': 'Clave de localidad',
                                                                   'Localidad_inegi': 'Localidad',
                                                                   'PRODUCTO': 'Producto cultivado',
                                                                   'FECHA': 'Fecha de entrega del beneficio',
                                                                   'MONTO FEDERAL': 'Monto entregado',
                                                                   'CICLO AGRÍCOLA': 'Ciclo agrícola'}, inplace=True)

        listado_beneficiarios_parte_II_localidades.to_csv(
            'data/listados_completos/listado_beneficiarios_2019_localidades.csv', index=False)

    elif prefix == 20:
        listado_beneficiarios_2020_match = pd.read_csv('data/listados_completos/listado_beneficiarios_2020_match.csv')

        listado_beneficiarios_2020_match['ENTIDAD_c_benef'] = listado_beneficiarios_2020_match['ENTIDAD'].apply(
            clean_text_inegi)
        listado_beneficiarios_2020_match['MUNICIPIO_c_benef'] = listado_beneficiarios_2020_match['MUNICIPIO'].apply(
            clean_text_inegi)
        listado_beneficiarios_2020_match['LOCALIDAD_c_benef'] = listado_beneficiarios_2020_match['LOCALIDAD'].apply(
            clean_text_inegi)

        listado_beneficiarios_2020_match['KEY_benef_loc'] = listado_beneficiarios_2020_match[
                                                                'MUNICIPIO_c_benef'].astype(str) + '-' + \
                                                            listado_beneficiarios_2020_match[
                                                                'LOCALIDAD_c_benef'].astype(str)

        listado_beneficiarios_2020 = listado_beneficiarios_2020_match.copy()

        listado_beneficiarios_2020 = listado_beneficiarios_2020.drop(columns=['Unnamed: 0'])
        entities = ['GUERRERO', 'MORELOS', 'TLAXCALA', 'PUEBLA', 'Guerrero', 'Puebla']
        listados_beneficiarios_por_entidad = create_listados_por_entidad(listado_beneficiarios_2020, entities, 20)
        inegi_uniquelocs = load_inegi_uniqueloc('data/productores_beneficiarios 2019-2022/diccionarios_E3/2020')

        Localidades_20_GUERRERO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_20_GUERRERO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2020_guerrero'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_20_GUERRERO_parte_I.shape

        Localidades_20_PUEBLA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_20_PUEBLA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2020_puebla'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_20_PUEBLA_parte_I.shape

        Localidades_20_MORELOS_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_20_MORELOS'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2020_morelos'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_20_MORELOS_parte_I.shape

        Localidades_20_TLAXCALA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_20_TLAXCALA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2020_tlaxcala'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_20_TLAXCALA_parte_I.shape

        # Concatenate datasets vertically
        diccionario_LOC_20 = pd.concat([
            Localidades_20_GUERRERO_parte_I,
            Localidades_20_PUEBLA_parte_I,
            Localidades_20_MORELOS_parte_I,
            Localidades_20_TLAXCALA_parte_I
        ], axis=0)

        diccionario_LOC_20.drop(columns=['BENEFICIARIO', 'ZONA', 'ENTIDAD', 'MUNICIPIO', 'LOCALIDAD', 'PRODUCTO',
                                         'FECHA', 'MONTO FEDERAL', 'CICLO AGRÍCOLA', 'ENTIDAD_c_benef',
                                         'MUNICIPIO_c_benef', 'LOCALIDAD_c_benef', 'best_match', 'CVE_ENT',
                                         'Entidad_inegi', 'CVE_MUN', 'Municipio_inegi',
                                         'CVE_LOC', 'Localidad_inegi', 'POB_TOTAL', 'Entidad_c_inegi',
                                         'Municipio_c_inegi', 'Localidad_c_inegi'], inplace=True)

        diccionario_LOC_20.to_csv('data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_20.csv',
                                  index=False)

        try:
            diccionario_LOC_20_simple = pd.read_csv(
                'data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_20_simple.csv',
                encoding='cp1252',
                delimiter=';')
            print(diccionario_LOC_20_simple.head())
        except pd.errors.ParserError as e:
            print("Error al leer el archivo CSV:", e)

        print('diccionario: ', diccionario_LOC_20_simple.columns)

        diccionario_LOC_20_simple.rename(columns={'ï»¿KEY_benef_loc': 'KEY_benef_loc'}, inplace=True)
        print('here is the shape: ', listado_beneficiarios_2020_match.shape)

        listado_beneficiarios_2020_match['MUNICIPIO_Clean'] = listado_beneficiarios_2020_match['MUNICIPIO'].apply(
            clean_text_inegi)
        listado_beneficiarios_2020_match['LOCALIDAD_Clean'] = listado_beneficiarios_2020_match['LOCALIDAD'].apply(
            clean_text_inegi)

        # Create KEY in listado beneficiarios
        listado_beneficiarios_2020_match['Municipio-loc-KEY'] = listado_beneficiarios_2020_match[
                                                                    'MUNICIPIO_Clean'].astype(str) + '-' + \
                                                                listado_beneficiarios_2020_match[
                                                                    'LOCALIDAD_Clean'].astype(str)
        diccionario_LOC_20_simple.columns = diccionario_LOC_20_simple.columns.str.strip()

        print('lietsado: ', listado_beneficiarios_2020_match.shape)
        print('diccionario: ', diccionario_LOC_20_simple.columns)

        listado_beneficiarios_2020_match.drop_duplicates(keep='first', inplace=True)
        diccionario_LOC_20_simple.drop_duplicates(inplace=True)

        listado_beneficiarios_parte_I_localidades = pd.merge(listado_beneficiarios_2020_match,
                                                             diccionario_LOC_20_simple, left_on="Municipio-loc-KEY",
                                                             right_on="KEY_benef_loc", how='left',
                                                             suffixes=('_benef', '_inegi'))

        listado_beneficiarios_parte_I_localidades.drop_duplicates(keep='first', inplace=True)

        print('lietsado: ', listado_beneficiarios_parte_I_localidades.shape)

        INEGI_UNIQUELOC_2020 = dataset_inegi_clean.drop(columns=['Entidad_c_inegi', 'Municipio_c_inegi',
                                                                 'Localidad_c_inegi', 'POB_TOTAL'])
        INEGI_UNIQUELOC_2020 = INEGI_UNIQUELOC_2020.drop_duplicates(subset='KEY_inegi_localidad', keep='first')

        listado_beneficiarios_parte_II_localidades = pd.merge(listado_beneficiarios_parte_I_localidades,
                                                              INEGI_UNIQUELOC_2020,
                                                              left_on="KEY_inegi_localidad",
                                                              right_on="KEY_inegi_localidad", how='left',
                                                              suffixes=('_benef', '_inegi'))

        print('listado_beneficiarios_parte_II_localidades: ', listado_beneficiarios_parte_II_localidades.columns)

        listado_beneficiarios_parte_II_localidades.drop(columns=['Unnamed: 0', 'ENTIDAD', 'MUNICIPIO', 'LOCALIDAD',
                                                                 'ENTIDAD_c_benef', 'MUNICIPIO_c_benef',
                                                                 'LOCALIDAD_c_benef', 'KEY_benef_loc_benef',
                                                                 'MUNICIPIO_Clean', 'LOCALIDAD_Clean',
                                                                 'Municipio-loc-KEY', 'KEY_benef_loc_inegi',
                                                                 'match_score',
                                                                 'KEY_inegi_localidad', 'KEY_inegi_municipio'],
                                                        inplace=True)

        listado_beneficiarios_parte_II_localidades.rename(columns={'BENEFICIARIO': 'Nombre del beneficiario',
                                                                   'ZONA': 'Zona del país',
                                                                   'CVE_ENT_inegi': 'Clave de entidad',
                                                                   'Entidad_inegi_inegi': 'Entidad federativa',
                                                                   'CVE_MUN_inegi': 'Clave de municipio',
                                                                   'Municipio_inegi_inegi': 'Municipio',
                                                                   'CVE_LOC': 'Clave de localidad',
                                                                   'Localidad_inegi': 'Localidad',
                                                                   'PRODUCTO': 'Producto cultivado',
                                                                   'FECHA': 'Fecha de entrega del beneficio',
                                                                   'MONTO FEDERAL': 'Monto entregado',
                                                                   'CICLO AGRÍCOLA': 'Ciclo agrícola'}, inplace=True)

        listado_beneficiarios_parte_II_localidades.to_csv(
            'data/listados_completos/listado_beneficiarios_2020_localidades.csv', index=False)

    elif prefix == 21:
        listado_beneficiarios_2021 = listado_beneficiarios_parte_II.copy()
        print(listado_beneficiarios_2021.shape)

        entities = ['GUERRERO', 'PUEBLA', 'MORELOS', 'TLAXCALA']
        listados_beneficiarios_por_entidad = create_listados_por_entidad(listado_beneficiarios_2021, entities, 21)
        inegi_uniquelocs = load_inegi_uniqueloc('data/productores_beneficiarios 2019-2022/diccionarios_E3/2021')

        Localidades_21_GUERRERO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_21_GUERRERO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2021_guerrero'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_21_GUERRERO_parte_I.shape

        Localidades_21_PUEBLA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_21_PUEBLA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2021_puebla'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_21_PUEBLA_parte_I.shape

        Localidades_21_MORELOS_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_21_MORELOS'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2021_morelos'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_21_MORELOS_parte_I.shape

        Localidades_21_TLAXCALA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_21_TLAXCALA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2021_tlaxcala'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_21_TLAXCALA_parte_I.shape

        # Concatenate datasets vertically
        diccionario_LOC_21 = pd.concat([
            Localidades_21_GUERRERO_parte_I,
            Localidades_21_PUEBLA_parte_I,
            Localidades_21_MORELOS_parte_I,
            Localidades_21_TLAXCALA_parte_I
        ], axis=0)

        diccionario_LOC_21.drop(columns=['BENEFICIARIO', 'ZONA', 'ENTIDAD', 'MUNICIPIO', 'LOCALIDAD', 'PRODUCTO',
                                         'FECHA', 'MONTO FEDERAL', 'CICLO AGRÍCOLA', 'KEY_benef_mun',
                                         'CVE_ENT_benef', 'Entidad_inegi_benef', 'CVE_MUN_benef',
                                         'Municipio_inegi_benef', 'ENTIDAD_c_benef', 'MUNICIPIO_c_benef',
                                         'LOCALIDAD_c_benef', 'best_match',
                                         'CVE_ENT_inegi', 'Entidad_inegi_inegi', 'CVE_MUN_inegi',
                                         'Municipio_inegi_inegi', 'CVE_LOC', 'Localidad_inegi', 'POB_TOTAL',
                                         'Entidad_c_inegi', 'Municipio_c_inegi', 'Localidad_c_inegi', ], inplace=True)

        diccionario_LOC_21.to_csv('data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_21.csv',
                                  index=False)

        try:
            diccionario_LOC_21_simple = pd.read_csv(
                'data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_21_simple.csv',
                encoding='utf-8',
                delimiter=';')
            print(diccionario_LOC_21_simple.head())
        except pd.errors.ParserError as e:
            print("Error al leer el archivo CSV:", e)

        diccionario_LOC_21_simple.rename(columns={'ï»¿KEY_benef_loc': 'KEY_benef_loc'}, inplace=True)
        print('key: ', diccionario_LOC_21_simple.columns)
        print('here is the shape: ', listado_beneficiarios_parte_II.shape)

        listado_beneficiarios_parte_II['MUNICIPIO_Clean'] = listado_beneficiarios_parte_II['MUNICIPIO'].apply(
            clean_text_inegi)
        listado_beneficiarios_parte_II['LOCALIDAD_Clean'] = listado_beneficiarios_parte_II['LOCALIDAD'].apply(
            clean_text_inegi)

        # Create KEY in listado beneficiarios
        listado_beneficiarios_parte_II['Municipio-loc-KEY'] = listado_beneficiarios_parte_II['MUNICIPIO_Clean'].astype(
            str) + '-' + listado_beneficiarios_parte_II[
                                                                  'LOCALIDAD_Clean'].astype(str)

        print('lietsado: ', listado_beneficiarios_parte_II.shape)
        print('diccionario: ', diccionario_LOC_21_simple.columns)

        listado_beneficiarios_parte_II.drop_duplicates(inplace=True)

        diccionario_LOC_21_simple.drop_duplicates(inplace=True)

        listado_beneficiarios_parte_I_localidades = pd.merge(listado_beneficiarios_parte_II, diccionario_LOC_21_simple,
                                                             left_on="Municipio-loc-KEY",
                                                             right_on="KEY_benef_loc", how='left',
                                                             suffixes=('_benef', '_inegi'))

        listado_beneficiarios_parte_I_localidades.drop_duplicates(keep='first', inplace=True)

        print('lietsado: ', listado_beneficiarios_parte_I_localidades.shape)

        INEGI_UNIQUELOC_2021 = dataset_inegi_clean.drop(columns=['Entidad_c_inegi', 'Municipio_c_inegi',
                                                                 'Localidad_c_inegi', 'POB_TOTAL'])
        INEGI_UNIQUELOC_2021 = INEGI_UNIQUELOC_2021.drop_duplicates(subset='KEY_inegi_localidad', keep='first')

        # TODO: REVISAR ESTA PARTE (ARTURO)
        listado_beneficiarios_parte_II_localidades = pd.merge(listado_beneficiarios_parte_I_localidades,
                                                              INEGI_UNIQUELOC_2021,
                                                              left_on="KEY_inegi_localidad",
                                                              right_on="KEY_inegi_localidad", how='left',
                                                              suffixes=('_benef', '_inegi'))

        listado_beneficiarios_parte_II_localidades.drop(columns=['ENTIDAD', 'MUNICIPIO', 'LOCALIDAD', 'KEY_benef_mun',
                                                                 'CVE_ENT_benef', 'Entidad_inegi_benef',
                                                                 'CVE_MUN_benef',
                                                                 'Municipio_inegi_benef', 'MUNICIPIO_Clean',
                                                                 'LOCALIDAD_Clean',
                                                                 'Municipio-loc-KEY', 'match_score',
                                                                 'KEY_inegi_localidad', 'KEY_inegi_municipio',
                                                                 'ENTIDAD_c_benef', 'MUNICIPIO_c_benef',
                                                                 'LOCALIDAD_c_benef', 'KEY_benef_loc_benef',
                                                                 'KEY_benef_loc_inegi', ], inplace=True)

        listado_beneficiarios_parte_II_localidades.rename(columns={'BENEFICIARIO': 'Nombre del beneficiario',
                                                                   'ZONA': 'Zona del país',
                                                                   'CVE_ENT_inegi': 'Clave de entidad',
                                                                   'Entidad_inegi_inegi': 'Entidad federativa',
                                                                   'CVE_MUN_inegi': 'Clave de municipio',
                                                                   'Municipio_inegi_inegi': 'Municipio',
                                                                   'CVE_LOC': 'Clave de localidad',
                                                                   'Localidad_inegi': 'Localidad',
                                                                   'PRODUCTO': 'Producto cultivado',
                                                                   'FECHA': 'Fecha de entrega del beneficio',
                                                                   'MONTO FEDERAL': 'Monto entregado',
                                                                   'CICLO AGRÍCOLA': 'Ciclo agrícola'}, inplace=True)

        listado_beneficiarios_parte_II_localidades.to_csv(
            'data/listados_completos/listado_beneficiarios_2021_localidades.csv', index=False)

    elif prefix == 22:

        listado_beneficiarios_2022 = listado_beneficiarios_parte_II.copy()
        print(listado_beneficiarios_2022.shape)
        listado_beneficiarios_2022.drop_duplicates(subset=['KEY_benef_loc'], inplace=True)
        print(listado_beneficiarios_2022.shape)

        entities = ['GUERRERO', 'OAXACA', 'PUEBLA', 'MORELOS', 'CHIAPAS', 'DURANGO', 'TLAXCALA', 'NAYARIT', 'ZACATECAS']
        listados_beneficiarios_por_entidad = create_listados_por_entidad(listado_beneficiarios_2022, entities, 22)
        inegi_uniquelocs = load_inegi_uniqueloc('data/productores_beneficiarios 2019-2022/diccionarios_E3/2022')

        Localidades_22_GUERRERO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_GUERRERO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_guerrero'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_GUERRERO_parte_I.shape

        Localidades_22_PUEBLA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_PUEBLA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_puebla'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_PUEBLA_parte_I.shape

        Localidades_22_MORELOS_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_MORELOS'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_morelos'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_MORELOS_parte_I.shape

        Localidades_22_TLAXCALA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_TLAXCALA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_tlaxcala'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_TLAXCALA_parte_I.shape

        Localidades_22_DURANGO_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_DURANGO'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_durango'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_DURANGO_parte_I.shape

        Localidades_22_OAXACA_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_OAXACA'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_oaxaca'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_OAXACA_parte_I.shape

        Localidades_22_NAYARIT_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_NAYARIT'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_nayarit'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_NAYARIT_parte_I.shape

        Localidades_22_CHIAPAS_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_CHIAPAS'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_chiapas'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_CHIAPAS_parte_I.shape

        Localidades_22_ZACATECAS_parte_I = fuzzy_merge_benef2019_2022(
            listados_beneficiarios_por_entidad['Localidades_22_ZACATECAS'],
            inegi_uniquelocs['INEGI_UNIQUELOC_2022_zacatecas'], 'KEY_benef_loc', 'KEY_inegi_localidad')
        Localidades_22_ZACATECAS_parte_I.shape

        diccionario_LOC_22 = pd.concat([
            Localidades_22_GUERRERO_parte_I,
            Localidades_22_PUEBLA_parte_I,
            Localidades_22_MORELOS_parte_I,
            Localidades_22_TLAXCALA_parte_I,
            Localidades_22_DURANGO_parte_I,
            Localidades_22_OAXACA_parte_I,
            Localidades_22_NAYARIT_parte_I,
            Localidades_22_CHIAPAS_parte_I,
            Localidades_22_ZACATECAS_parte_I
        ], axis=0)

        diccionario_LOC_22.drop(columns=['BENEFICIARIO', 'ZONA', 'ENTIDAD', 'MUNICIPIO', 'LOCALIDAD', 'PRODUCTO',
                                         'FECHA', 'MONTO FEDERAL', 'CICLO AGRÍCOLA', 'KEY_benef_mun',
                                         'CVE_ENT_benef', 'Entidad_inegi_benef', 'CVE_MUN_benef',
                                         'Municipio_inegi_benef', 'ENTIDAD_c_benef', 'MUNICIPIO_c_benef',
                                         'LOCALIDAD_c_benef', 'best_match',
                                         'CVE_ENT_inegi', 'Entidad_inegi_inegi', 'CVE_MUN_inegi',
                                         'Municipio_inegi_inegi', 'CVE_LOC', 'Localidad_inegi', 'POB_TOTAL',
                                         'Entidad_c_inegi', 'Municipio_c_inegi', 'Localidad_c_inegi', ], inplace=True)

        diccionario_LOC_22.to_csv('data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_22.csv',
                                  index=False)

        try:
            diccionario_LOC_22_simple = pd.read_csv(
                'data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_22_simple.csv',
                encoding='cp1252',
                delimiter=';')
            # diccionario_LOC_22_simple.drop(columns=['ValidaciÃ³n', 'Propuesta'], inplace=True)
            print(diccionario_LOC_22_simple.head())
        except pd.errors.ParserError as e:
            print("Error al leer el archivo CSV:", e)

        diccionario_LOC_22_simple.rename(columns={'ï»¿KEY_benef_loc': 'KEY_benef_loc'}, inplace=True)

        print('here is the shape: ', listado_beneficiarios_parte_II.shape)

        listado_beneficiarios_parte_II['MUNICIPIO_Clean'] = listado_beneficiarios_parte_II['MUNICIPIO'].apply(
            clean_text_inegi)
        listado_beneficiarios_parte_II['LOCALIDAD_Clean'] = listado_beneficiarios_parte_II['LOCALIDAD'].apply(
            clean_text_inegi)

        # Create KEY in listado beneficiarios
        listado_beneficiarios_parte_II['Municipio-loc-KEY'] = listado_beneficiarios_parte_II['MUNICIPIO_Clean'].astype(
            str) + '-' + listado_beneficiarios_parte_II['LOCALIDAD_Clean'].astype(str)
        
        diccionario_LOC_22_simple.drop_duplicates(inplace=True)

        listado_beneficiarios_parte_I_localidades = pd.merge(listado_beneficiarios_parte_II, diccionario_LOC_22_simple,
                                                             left_on="Municipio-loc-KEY",
                                                             right_on="KEY_benef_loc", how='left',
                                                             suffixes=('_benef', '_inegi'))

        listado_beneficiarios_parte_I_localidades.drop_duplicates(keep='first', inplace=True)

        print('listado_benef: ', listado_beneficiarios_parte_I_localidades.isna().sum())

        INEGI_UNIQUELOC_2022 = dataset_inegi_clean.drop(columns=['Entidad_c_inegi', 'Municipio_c_inegi',
                                                                 'Localidad_c_inegi', 'POB_TOTAL'])
        INEGI_UNIQUELOC_2022 = INEGI_UNIQUELOC_2022.drop_duplicates(subset='KEY_inegi_localidad', keep='first')

        # TODO: REVISAR ESTA PARTE (ARTURO)
        listado_beneficiarios_parte_II_localidades = pd.merge(listado_beneficiarios_parte_I_localidades,
                                                              INEGI_UNIQUELOC_2022,
                                                              left_on="KEY_inegi_localidad",
                                                              right_on="KEY_inegi_localidad", how='left',
                                                              suffixes=('_benef', '_inegi'))

        listado_beneficiarios_parte_II_localidades.drop(columns=['ENTIDAD', 'MUNICIPIO', 'LOCALIDAD', 'KEY_benef_mun',
                                                                 'CVE_ENT_benef', 'Entidad_inegi_benef',
                                                                 'CVE_MUN_benef',
                                                                 'Municipio_inegi_benef', 'MUNICIPIO_Clean',
                                                                 'LOCALIDAD_Clean',
                                                                 'Municipio-loc-KEY', 'match_score',
                                                                 'KEY_inegi_localidad', 'KEY_inegi_municipio',
                                                                 'ENTIDAD_c_benef', 'MUNICIPIO_c_benef',
                                                                 'LOCALIDAD_c_benef', 'KEY_benef_loc_benef',
                                                                 'KEY_benef_loc_inegi', ], inplace=True)

        listado_beneficiarios_parte_II_localidades.rename(columns={'BENEFICIARIO': 'Nombre del beneficiario',
                                                                   'ZONA': 'Zona del país',
                                                                   'CVE_ENT_inegi': 'Clave de entidad',
                                                                   'Entidad_inegi_inegi': 'Entidad federativa',
                                                                   'CVE_MUN_inegi': 'Clave de municipio',
                                                                   'Municipio_inegi_inegi': 'Municipio',
                                                                   'CVE_LOC': 'Clave de localidad',
                                                                   'Localidad_inegi': 'Localidad',
                                                                   'PRODUCTO': 'Producto cultivado',
                                                                   'FECHA': 'Fecha de entrega del beneficio',
                                                                   'MONTO FEDERAL': 'Monto entregado',
                                                                   'CICLO AGRÍCOLA': 'Ciclo agrícola'}, inplace=True)

        listado_beneficiarios_parte_II_localidades.to_csv(
            'data/listados_completos/listado_beneficiarios_2022_localidades.csv', index=False)


def main():
    data_cleaning3('data/inegi/dataset_inegi_clean_2022.csv',
                   'data/productores_beneficiarios 2019-2022/fertilizantes_2022.csv', 22)


if __name__ == "__main__":
    main()
