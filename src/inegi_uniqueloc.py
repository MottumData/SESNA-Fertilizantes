import pandas as pd

def procesar_datos_inegi_por_estado(path_dataset_inegi, estado, output_path):
    # Leer los datos desde el archivo CSV
    dataset_inegi = pd.read_csv(path_dataset_inegi)
    
    # Crear columnas de clave única para municipio y localidad
    dataset_inegi['KEY_inegi_municipio'] = dataset_inegi['Entidad_c_inegi'].astype(str) + '-' + dataset_inegi['Municipio_c_inegi'].astype(str)
    dataset_inegi['KEY_inegi_localidad'] = dataset_inegi['Municipio_c_inegi'].astype(str) + '-' + dataset_inegi['Localidad_c_inegi'].astype(str)
    
    # Filtrar datos por el estado especificado
    datos_estado = dataset_inegi[dataset_inegi['Entidad_inegi'] == estado]
    
    # Eliminar filas duplicadas basadas en Entidad_inegi, Municipio_inegi y Localidad_inegi
    datos_estado = datos_estado.drop_duplicates()
    
    # Ordenar el DataFrame por Entidad_inegi, Municipio_inegi y Localidad_inegi
    df_sorted = datos_estado.sort_values(by=['Entidad_inegi', 'Municipio_inegi', 'Localidad_inegi'])
    
    # Identificar duplicados basados en Entidad_inegi, Municipio_inegi y Localidad_inegi
    duplicados = df_sorted[df_sorted.duplicated(subset=['Entidad_inegi', 'Municipio_inegi', 'Localidad_inegi'], keep=False)]
    print("Número de rows con base en Entidad, Municipio y Localidad: ", duplicados.shape)
    
    # Contar duplicados agrupados por Entidad_inegi, Municipio_inegi y Localidad_inegi
    conteo_duplicados = duplicados.groupby(['Entidad_inegi', 'Municipio_inegi', 'Localidad_inegi']).size().reset_index(name='Numero_Duplicados')
    
    # Ordenar por el número de duplicados en orden descendente
    conteo_duplicados = conteo_duplicados.sort_values(by='Numero_Duplicados', ascending=False)
    
    # Mostrar el conteo de duplicados ordenado por count
    print(conteo_duplicados)
    total_duplicados = conteo_duplicados['Numero_Duplicados'].sum()
    print("Total de duplicados: ", total_duplicados)
    
    # Seleccionar la localidad con mayor número de habitantes para cada combinación única de municipio-localidad
    datos_estado = datos_estado.loc[datos_estado.groupby(['Municipio_inegi', 'Localidad_inegi'])['POB_TOTAL'].idxmax()]
    
    # Eliminar la columna de clave única de municipio
    datos_estado.drop(columns=["KEY_inegi_municipio"], inplace=True)
    
    # Eliminar duplicados basados en Municipio_inegi y Localidad_inegi
    datos_estado.drop_duplicates(subset=['Municipio_inegi', 'Localidad_inegi'], inplace=True)
    
    # Guardar el DataFrame resultante en un archivo CSV
    datos_estado.to_csv(output_path, index=False, encoding='utf-8')

def generate_uniqueloc():
    # Ejemplo de uso de la función para Guerrero
    path_dataset_inegi_2019 = 'data/inegi/dataset_inegi_clean_2019.csv'

    estados1 = ['Puebla', 'México', 'Guanajuato', 'Querétaro', 'Zacatecas', 'Veracruz de Ignacio de la Llave', 'Hidalgo', 'Michoacán de Ocampo', 'Oaxaca', 'Colima', 'Chiapas', 'San Luis Potosí', 'Jalisco', 'Nayarit', 'Guerrero']

    # Procesar los datos para cada estado
    for estado in estados1:
        output_path = f'data/productores_beneficiarios 2019-2022/diccionarios_E3/2019/INEGI_UNIQUELOC_2019_{estado.lower().replace(" ", "_")}.csv'
        procesar_datos_inegi_por_estado(path_dataset_inegi_2019, estado, output_path)

    # Ejemplo de uso de la función para Guerrero
    path_dataset_inegi_2020 = 'data/inegi/dataset_inegi_clean_2020.csv'

    estados2 = ['Guerrero', 'Morelos', 'Tlaxcala', 'Puebla']

    # Procesar los datos para cada estado
    for estado in estados2:
        output_path = f'data/productores_beneficiarios 2019-2022/diccionarios_E3/2020/INEGI_UNIQUELOC_2020_{estado.lower().replace(" ", "_")}.csv'
        procesar_datos_inegi_por_estado(path_dataset_inegi_2020, estado, output_path)

    # Ejemplo de uso de la función para Guerrero
    path_dataset_inegi_2021 = 'data/inegi/dataset_inegi_clean_2021.csv'

    estados3 = ['Guerrero', 'Morelos', 'Tlaxcala', 'Puebla']

    # Procesar los datos para cada estado
    for estado in estados3:
        output_path = f'data/productores_beneficiarios 2019-2022/diccionarios_E3/2021/INEGI_UNIQUELOC_2021_{estado.lower().replace(" ", "_")}.csv'
        procesar_datos_inegi_por_estado(path_dataset_inegi_2021, estado, output_path)

    # Ejemplo de uso de la función para Guerrero
    path_dataset_inegi_2022 = 'data/inegi/dataset_inegi_clean_2022.csv'

    estados4 = ['Guerrero', 'Oaxaca', 'Puebla', 'Morelos', 'Chiapas', 'Durango', 'Tlaxcala', 'Nayarit', 'Zacatecas']

    # Procesar los datos para cada estado
    for estado in estados4:
        output_path = f'data/productores_beneficiarios 2019-2022/diccionarios_E3/2022/INEGI_UNIQUELOC_2022_{estado.lower().replace(" ", "_")}.csv'
        procesar_datos_inegi_por_estado(path_dataset_inegi_2022, estado, output_path)

def main():
    generate_uniqueloc()
    
if __name__ == "__main__":
    main()