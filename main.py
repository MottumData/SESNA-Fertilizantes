import streamlit as st
import subprocess
import logging
import config
import sys
import os
import shutil
import pandas as pd
import base64
from io import StringIO
from src.dataset_download import download_datasets
from src.data_cleaning_and_merge import data_cleaning, data_cleaning2, load_datasets
from src.scrape_urls import scrape_urls, scrape_xlsx
from src.data_cleaning_and_merge_e3 import data_cleaning3
from streamlit_option_menu import option_menu as om
import requests
from src.inegi_uniqueloc import generate_uniqueloc
from src.data_cleaning_inegi import clean_inegi
import time

# Incluir estas líneas en cada script para registrar los logs
logger = logging.getLogger("Fertilizantes")
logger.setLevel(logging.INFO)


def session_state_with_love_mottum(unique_id):
    # Construct a unique session state key based on the provided unique_id
    key = f'love_mottum_displayed_{unique_id}'

    # Check if this unique key is in the session state and initialize it if not
    if key not in st.session_state:
        st.session_state[key] = False

    # Always check the session state and display the motto if it has been shown before
    # This ensures the motto remains visible across all page navigations
    if st.session_state[key]:
        display_love_mottum()
    else:
        # The first time the motto is to be displayed, set the session state to True
        display_love_mottum()
        st.session_state[key] = True


def convert_xlsx_to_csv_in_directory(directory_path):
    # List all .xlsx files in the directory
    xlsx_files = [f for f in os.listdir(directory_path) if f.endswith('.xlsx')]
    total_files = len(xlsx_files)

    if total_files == 0:
        st.write("No .xlsx files found to convert.")
        return

    # Initialize the progress bar
    progress_bar = st.progress(0)

    for index, filename in enumerate(xlsx_files, start=1):
        # Construct full file path
        file_path = os.path.join(directory_path, filename)
        # Read the .xlsx file
        df = pd.read_excel(file_path)
        # Construct the .csv filename
        csv_filename = filename.replace('.xlsx', '.csv')
        csv_file_path = os.path.join(directory_path, csv_filename)
        # Save as .csv
        df.to_csv(csv_file_path, index=False)
        print(f"Converted {filename} to {csv_filename}")

        # Update the progress bar
        progress_bar.progress(index / total_files)

    st.write("All files have been converted.")


def display_love_mottum():
    """Displays the 'Made with love' motto and logo."""
    st.markdown("<br>", unsafe_allow_html=True)
    cols = st.columns([1, 1, 1])  # Create three columns
    inner_cols = cols[2].columns([1, 1, 1, 1])  # Create four columns inside the third main column
    inner_cols[0].markdown(
        "<p style='text-align: center; font-family: Manrope; padding-top: 12px; white-space: nowrap;'>Made with love</p>",
        unsafe_allow_html=True)  # Center the text, change the font, and add padding
    inner_cols[2].image('docs/images/mottum2.png', use_column_width=True)


def clear_directory(directory):
    # Clear the directory of all files and subdirectories
    for filename in os.listdir(directory):
        if filename == '.gitkeep':  # Skip the .gitkeep file
            continue
        file_path = os.path.join(directory, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')


def upload_manually(page_id, tab):
    # Check if the required files are present
    if os.path.exists(f'data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_{tab}_simple.csv'):
        st.success(
            "Hay un diccionario con los nombres de municipios validados. Si desea volver a ejecutar el "
            "proceso, descarga a continuación el diccionario de localidades.")
    else:
        st.warning(
            "Es necesrio subir el diccionario de localidades con los nombres de localidades validados para "
            "poder continuar con el proceso.")

    with open(f'data/productores_beneficiarios 2019-2022/diccionarios_E3/diccionario_LOC_{tab}.csv', "rb") as file:
        st.markdown("Descargar diccionario de localidades para validar:")
        cols = st.columns([2, 1, 2])
        cols[1].download_button(
            label="Descargar",
            data=file,
            file_name=f"diccionario_LOC_{tab}.csv",
            mime="text/csv",
        )

    uploaded_file = st.file_uploader(f"Suba el diccionario manual para empezar el proceso.",
                                     key=f'file_uploader_intro_{page_id}')
    if uploaded_file is not None:
        # Check if the file is a CSV (or similar) before trying to read it
        if uploaded_file.name.endswith('.csv'):
            # Directly save the uploaded file to avoid modifying its content
            with open(os.path.join('data', f"diccionario_LOC_{tab}_simple.csv"), "wb") as f:
                f.write(uploaded_file.getvalue())
            st.success(f"El archivo {uploaded_file.name} ha sido subido con éxito.")
        else:
            st.error("¡El archivo tiene que ser un .csv para continuar con el proceso!")


def data_cleaning_function(dataset, tab):
    # Thus function executes the process depending on the tab you are in, in this case it handles the data cleaning process.
    if dataset == 'Productores_autorizados_2023':
        if tab == '3':
            data_cleaning()
            st.success(
                "El proceso de limpieza de datos ha terminado. En la siguiente pestaña puede proceder con la descarga de los datos estandarizados.")
    elif dataset == 'Beneficiarios_fertilizantes_2023':
        data_cleaning2()
        st.success(
            "El proceso de limpieza de datos ha terminado. En la siguiente pestaña puede proceder con la descarga de los datos estandarizados.")

    elif dataset == 'Beneficiarios_fertilizantes_2019-2022':
        if tab == '2019':
            data_cleaning3('data/inegi/dataset_inegi_clean_2019.csv',
                           'data/productores_beneficiarios 2019-2022/fertilizantes_2019.csv', 19)
            st.success(
                "El proceso de limpieza de datos ha terminado. En la siguiente pestaña puede proceder con la descarga de los datos estandarizados.")
        elif tab == '2020':
            data_cleaning3('data/inegi/dataset_inegi_clean_2020.csv',
                           'data/productores_beneficiarios 2019-2022/listado_beneficiarios_fertilizantes_2020.csv', 20)
            st.success(
                "El proceso de limpieza de datos ha terminado. En la siguiente pestaña puede proceder con la descarga de los datos estandarizados.")
        elif tab == '2021':
            data_cleaning3('data/inegi/dataset_inegi_clean_2021.csv',
                           'data/productores_beneficiarios 2019-2022/listado_beneficiarios_fertilizantes_2021.csv', 21)
            st.success(
                "El proceso de limpieza de datos ha terminado. En la siguiente pestaña puede proceder con la descarga de los datos estandarizados.")
        elif tab == '2022':
            data_cleaning3('data/inegi/dataset_inegi_clean_2022.csv',
                           'data/productores_beneficiarios 2019-2022/listado_beneficiarios_fertilizantes_2022.csv', 22)
            st.success(
                "El proceso de limpieza de datos ha terminado. En la siguiente pestaña puede proceder con la descarga de los datos estandarizados.")


def show_intro(page_id):
    # This function shows the introduction screen for each tab.
    if st.session_state.main_page == 'Productores autorizados 2023':
        st.markdown((
            """
                La siguiente aplicacion ha sido desarrollada para [SESNA](https://www.sesna.gob.mx/).
                El propósito de esta aplicación es la descarga, limpieza y unión de las bases de datos
                publicadas en la siguiente URL: [Programa de Fertilizantes 2023 Listados Autorizados](https://www.datos.gob.mx/busca/dataset/programa-de-fertilizantes-2023-listados-autorizados).
            """
        ))
        uploaded_file = st.file_uploader(f"Suba el diccionario manual para empezar el proceso. {page_id}",
                                         key=f'file_uploader_intro_{page_id}')
        if uploaded_file is not None:
            # Check if the file is a CSV (or similar) before trying to read it
            if uploaded_file.name.endswith('.csv'):
                # Directly save the uploaded file to avoid modifying its content
                with open(os.path.join('data', uploaded_file.name), "wb") as f:
                    f.write(uploaded_file.getvalue())
                st.success(f"El archivo {uploaded_file.name} ha sido subido con éxito.")
            else:
                st.error("¡El archivo tiene que ser un .csv para continuar con el proceso!")

    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2023':
        st.markdown((
            """
                La siguiente aplicacion ha sido desarrollada para [SESNA](https://www.sesna.gob.mx/).
                El propósito de esta aplicación es la descarga, limpieza y unión de las bases de datos
                publicadas en la siguiente URL: [Programa de Fertilizantes 2023 Beneficiarios Autorizados](https://www.datos.gob.mx/busca/dataset/programa-de-fertilizantes-2023-listados-de-beneficiarios).
            """
        ))
        uploaded_file = st.file_uploader(f"Suba el diccionario manual para empezar el proceso. {page_id}",
                                         key=f'file_uploader_intro_{page_id}')
        if uploaded_file is not None:
            # Check if the file is a CSV (or similar) before trying to read it
            if uploaded_file.name.endswith('.csv'):
                # Directly save the uploaded file to avoid modifying its content
                with open(os.path.join('data', uploaded_file.name), "wb") as f:
                    f.write(uploaded_file.getvalue())
                st.success(f"El archivo {uploaded_file.name} ha sido subido con éxito.")
            else:
                st.error("¡El archivo tiene que ser un .csv para continuar con el proceso!")

    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2019-2022':
        st.markdown((
            """
                La siguiente aplicacion ha sido desarrollada para [SESNA](https://www.sesna.gob.mx/).
                El propósito de esta aplicación es la descarga, limpieza y unión de las bases de datos
                publicadas en los siguientes URLs para el Programa de Fertilizantes 2019-2022 [Beneficiarios 2019](https://datos.gob.mx/busca/dataset/programa-fertilizantes-2019), [Beneficiarios 2020](https://datos.gob.mx/busca/dataset/programa-fertilizantes-2020), [Beneficiarios 2021](https://datos.gob.mx/busca/dataset/programa-fertilizantes-2021), [Beneficiarios 2022](https://datos.gob.mx/busca/dataset/programa-fertilizantes-2022).
            """
        ))

        st.markdown("<br>", unsafe_allow_html=True)

        st.info("En este caso procedemos primero con la limpieza de los datasets de inegi "
                "por un lado y la generación de datasets de inegi por estado, asegurese de "
                "ejecutar este boton antes de proceder al siguiente paso.")

        st.markdown("<br>", unsafe_allow_html=True)

        cols_button = st.columns([1, 1, 1])
        if cols_button[1].button('Limpieza de Inegi/Generación de inegi_uniquelocs',
                                 key=f'start_process_button_{page_id}'):
            with st.spinner(
                    'Ejecutando scripts... Esto puede tardar unos minutos. No cambie de pestaña hasta que el proceso haya acabado!'):
                progress_bar = st.progress(0)
                clean_inegi()
                progress_bar.progress(1 / 2)
                generate_uniqueloc()
                progress_bar.progress(1 / 1)

    session_state_with_love_mottum('footer')


def start_process(page_id):
    # This function starts the process of downloading the datasets.
    if st.session_state.main_page == 'Productores autorizados 2023':
        cols_button = st.columns([1, 1, 1])
        if cols_button[1].button('Descargar Listado Autorizados2023.', key=f'start_process_button_{page_id}'):
            data_download(
                url="https://www.datos.gob.mx/busca/dataset/programa-de-fertilizantes-2023-listados-autorizados",
                download_destination_folder="data/productores_autorizados")
        session_state_with_love_mottum('footer1')
    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2023':
        cols_button = st.columns([1, 1, 1])
        if cols_button[1].button('Descargar Listado Beneficiarios2023.', key=f'start_process_button_{page_id}'):
            data_download(
                url="https://www.datos.gob.mx/busca/dataset/programa-de-fertilizantes-2023-listados-de-beneficiarios",
                download_destination_folder="data/productores_beneficiarios")
        session_state_with_love_mottum('footer2')
    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2019-2022':
        st.text("Es igual correr este proceso para 2019 que para 2022. Todos los datasets de los años")
        st.text("2019, 2020, 2021 y 2022 serán descargados desde cualquier pestaña de año.")
        cols_button = st.columns([1, 1, 1])
        if cols_button[1].button('Descargar Listado Beneficiarios2019-2022.', key=f'start_process_button_{page_id}'):
            data_download(download_destination_folder="data/productores_beneficiarios 2019-2022")
        session_state_with_love_mottum('footer22')


def data_download(download_destination_folder, url=None, progress_callback=None,
                  urls=None):
    # This function downloads the datasets from the URLs provided.
    if not os.path.exists('data'):
        os.makedirs('data')
        print("Directory 'data' missing, creating data directory.")
    if not os.path.exists('data/productores_autorizados'):
        os.makedirs('data/productores_autorizados')
        print("Directory 'data/productores_autorizados' missing, creating data/productores_autorizados.")
    if not os.path.exists('data/productores_beneficiarios'):
        os.makedirs('data/productores_beneficiarios')
        print("Directory 'data/productores_beneficiarios' missing, creating data/productores_beneficiarios.")
    if not os.path.exists('data/productores_beneficiarios 2019-2022'):
        os.makedirs('data/productores_beneficiarios 2019-2022')
        print(
            "Directory 'data/productores_beneficiarios 2019-2022' missing, creating data/productores_beneficiarios 2019-2022.")

    # clear_directory('data/productores_autorizados')
    # clear_directory('data/productores_beneficiarios')

    results = []
    datasets = []

    urls = ["https://datos.gob.mx/busca/dataset/programa-fertilizantes-2019",
            "https://datos.gob.mx/busca/dataset/programa-fertilizantes-2020",
            "https://datos.gob.mx/busca/dataset/programa-fertilizantes-2021",
            "https://datos.gob.mx/busca/dataset/programa-fertilizantes-2022"]
    with st.spinner(
            'Ejecutando scripts... Esto puede tardar unos minutos. No cambie de pestaña hasta que el proceso haya acabado!'):
        if st.session_state.main_page == 'Productores autorizados 2023':
            download_urls = scrape_urls(url)
        elif st.session_state.main_page == 'Beneficiarios fertilizantes 2023':
            download_urls = scrape_urls(url)
        elif st.session_state.main_page == 'Beneficiarios fertilizantes 2019-2022':
            download_urls = []
            for url in [urls[0], urls[1], urls[2], urls[3]]:
                download_urls += scrape_xlsx(url)
        # Correctly initialize the progress bar with 0%
        # Calculate each step's progress increment based on the number of URLs
        progress_bar = st.progress(0)

        print('download_urls:', download_urls)
        print('results:', results)

        # Define the progress callback function
        def progress_callback(progress):
            progress_bar.progress(progress)

        result = download_datasets(download_urls, download_destination_folder, progress_callback)

        # Update progress to 100% after download_datasets completes
        # Explicitly set progress to 100% after all processing

        good_count = result['good_count']
        good_urls = result['good_urls']
        failed_count = result['failed_count']
        failed_urls = result['failed_urls']

        if good_count > 0:
            st.write(f"{good_count} datasets se han descargado de forma exitosa.")
            st.selectbox("URLs de los datasets descargados con éxito:", good_urls)
        else:
            st.write(
                "No se pudo descargar ningún dataset de: ${download_urls[0]}")

        if failed_count > 0:
            st.write(f"Falló la descarga de {failed_count} datasets.")
            st.selectbox("URLs de los datasets que fallaron al descargar:", failed_urls)
        else:
            st.write("Todos los datasets de la URL han sido descargados de forma exitosa.\n")

        all_urls = good_urls + failed_urls
        statuses = ['TRUE' if url in good_urls else 'FALSE' for url in all_urls]
        dataset = pd.DataFrame({
            'id': range(1, len(all_urls) + 1),
            'url': all_urls,
            'estado_de_descarga': statuses
        })

        # Convert the DataFrame to a CSV file
        csv = dataset.to_csv(index=False)
        b64 = base64.b64encode(csv.encode()).decode()  # some strings <-> bytes conversions necessary here
        href = f'<a href="data:file/csv;base64,{b64}" download="dataset.csv">Estado de descargas de URL</a>'

        # Create a download button for the CSV file
        st.markdown(href, unsafe_allow_html=True)
        if st.session_state.main_page == 'Beneficiarios fertilizantes 2019-2022':
            logger.info("Fin de Ejecución")
            st.write(
                "Pasando archivos .xlsx a .csv... Esto puede tardar unos minutos. No cambie de pestaña hasta que el proceso haya acabado...")
            convert_xlsx_to_csv_in_directory('data/productores_beneficiarios 2019-2022')
            st.success(
                "El proceso de descarga ha terminado. En la siguiente pestaña puede proceder con la limpieza de los datos."
            )


def process_tab(dataset, year):
    # This function processes the tab for the Beneficiarios fertilizantes 2019-2022 page.
    stats = {
        f'Número de filas_{year}': [dataset.shape[0]],
        f'Número de columnas_{year}': [dataset.shape[1]],
    }
    stats_df = pd.DataFrame(stats)
    stats_df.reset_index(drop=True, inplace=True)

    # Display statistics with custom styling
    st.markdown(f"""
    <style>
    .centered {{
        font-size: 15px;
        font-weight: bold;
    }}
    </style>
    <div class="centered">Current dataset for {year}</div>
    """, unsafe_allow_html=True)
    # TODO Quitar columna de Index
    st.table(stats_df)

    with st.spinner(
            f'Ejecutando scripts para {year}... Esto puede tardar unos minutos. No cambie de pestaña hasta que el proceso haya acabado!'):
        cols_button = st.columns([1, 1, 1])
        if cols_button[1].button(f'Limpieza de datos de Listado Productores{year}.',
                                 key=f'start_cleaning_button{year}'):
            data_cleaning_function(f'Beneficiarios_fertilizantes_2019-2022', year)
            # st.success("El proceso de limpieza de datos ha terminado. En la siguiente pestaña puede proceder con la descarga de los datos estandarizados.")
            session_state_with_love_mottum('footer4')


def clean_data_screen(page_id, tab):
    # This function shows the screen for cleaning the data.
    if st.session_state.main_page == 'Productores autorizados 2023':
        required_files = [
            'data/inegi/dataset_inegi.csv',
            'data/productores_beneficiarios/diccionarios_E2/Diccionario_manual.csv'
        ]

        with open(f'data/productores_autorizados/diccionarios_E1/diccionario_prod_sin_VERACRUZ.csv', "rb") as file:
            st.markdown("Descargar diccionario de localidades para validar:")
            cols = st.columns([2, 1, 2])
            cols[1].download_button(
                label="Descargar",
                data=file,
                file_name="diccionario_prod_sin_VERACRUZ.csv",
                mime="text/csv",
            )

        listado_productores = load_datasets('data/productores_autorizados')

        stats = {
            'Número de filas': [listado_productores.shape[0]],
            'Número de columnas': [listado_productores.shape[1]],
            # Add more statistics here if needed
        }

        stats_df = pd.DataFrame(stats)
        # Display statistics
        st.markdown("""
        <style>
        .centered {
            font-size: 15px; /* Adjust the size as needed */
            font-weight: bold; /* Makes the text bold */
            /* Add more styling as needed */
        }
        </style>
        <div class="centered">Current dataset</div>
        """, unsafe_allow_html=True)

        st.table(stats_df)

        with st.spinner(
                'Ejecutando scripts... Esto puede tardar unos minutos. No cambie de pestaña hasta que el proceso haya acabado!'
        ):
            cols_button = st.columns([1, 1, 1])
            if cols_button[1].button('Limpieza de datos de Listado Productores2023.',
                                    key=f'start_cleaning_button{page_id}'):
                data_cleaning_function('Productores_autorizados_2023', '3')
                # st.success("El proceso de limpieza de datos ha terminado. En la siguiente pestaña puede proceder con la descarga de los datos estandarizados.")
            session_state_with_love_mottum('footer4')

    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2023':
        required_files = [
            'data/inegi/dataset_inegi.csv',
            'data/productores_beneficiarios/diccionarios_E2/Diccionario_Simple.csv'
        ]

        with open(f'data/productores_beneficiarios/diccionarios_E2/diccionario_benef.csv', "rb") as file:
            st.markdown("Descargar diccionario de localidades para validar:")
            cols = st.columns([2, 1, 2])
            cols[1].download_button(
                label="Descargar",
                data=file,
                file_name="diccionario_benef.csv",
                mime="text/csv",
            )

        listado_beneficiarios = load_datasets('data/productores_beneficiarios')

        stats = {
            'Número de filas': [listado_beneficiarios.shape[0]],
            'Número de columnas': [listado_beneficiarios.shape[1]],
            # Add more statistics here if needed
        }
        stats_df = pd.DataFrame(stats)
        # Display statistics
        st.markdown("""
        <style>
        .centered {
            font-size: 15px; /* Adjust the size as needed */
            font-weight: bold; /* Makes the text bold */
            /* Add more styling as needed */
        }
        </style>
        <div class="centered">Current dataset</div>
        """, unsafe_allow_html=True)

        st.table(stats_df)

        with st.spinner(
                'Ejecutando scripts... Esto puede tardar unos minutos. No cambie de pestaña hasta que el proceso haya acabado!'
        ):
            cols_button = st.columns([1, 1, 1])
            if cols_button[1].button('Limpieza de datos de Listado Beneficiarios2023.',
                                     key=f'start_cleaning_button{page_id}'):
                data_cleaning_function('Beneficiarios_fertilizantes_2023', '4')
            session_state_with_love_mottum('footer5')

        missing_files = [file for file in required_files if not os.path.exists(file)]

        if missing_files:
            # Display an error message for each missing file
            for missing_file in missing_files:
                st.error(
                    f"Error: El archivo requerido {missing_file} no ha sido subido todavía, vuelva a la introducción y súbalo desde ahi.")
            return

    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2019-2022':
        if tab == '2019':
            upload_manually(22, 19)
        elif tab == '2020':
            upload_manually(23, 20)
        elif tab == '2021':
            upload_manually(24, 21)
        elif tab == '2022':
            upload_manually(25, 22)

        required_files = [
            'data/dataset_inegi_2019.csv',
            'data/dataset_inegi_2020.csv',
            'data/dataset_inegi_2021.csv',
            'data/dataset_inegi_2022.csv',
            'data/productores_beneficiarios 2019-2022/diccionario_MUN_19_simple.csv',
            'data/productores_beneficiarios 2019-2022/diccionario_MUN_20_simple.csv',
            'data/productores_beneficiarios 2019-2022/diccionario_MUN_21_simple.csv',
            'data/productores_beneficiarios 2019-2022/diccionario_MUN_22_simple.csv',
            'data/productores_beneficiarios 2019-2022/diccionario_LOC_19_simple.csv',
            'data/productores_beneficiarios 2019-2022/diccionario_LOC_20_simple.csv',
            'data/productores_beneficiarios 2019-2022/diccionario_LOC_21_simple.csv',
            'data/productores_beneficiarios 2019-2022/diccionario_LOC_22_simple.csv'
        ]

        if tab == '2019':
            listado_beneficiarios_2019 = pd.read_csv('data/productores_beneficiarios 2019-2022/fertilizantes_2019.csv')
            process_tab(listado_beneficiarios_2019, '2019')
        elif tab == '2020':
            listado_beneficiarios_2020 = pd.read_csv(
                'data/productores_beneficiarios 2019-2022/listado_beneficiarios_fertilizantes_2020.csv')
            process_tab(listado_beneficiarios_2020, '2020')
        elif tab == '2021':
            listado_beneficiarios_2021 = pd.read_csv(
                'data/productores_beneficiarios 2019-2022/listado_beneficiarios_fertilizantes_2021.csv')
            process_tab(listado_beneficiarios_2021, '2021')
        elif tab == '2022':
            listado_beneficiarios_2022 = pd.read_csv(
                'data/productores_beneficiarios 2019-2022/listado_beneficiarios_fertilizantes_2022.csv')
            process_tab(listado_beneficiarios_2022, '2022')


def show_finished(tab):
    # This function shows the finished screen for each tab.
    if st.session_state.main_page == 'Productores autorizados 2023':
        dataset_path = "data/listados_completos/listado_productores_complete2023.csv"
        if os.path.exists(dataset_path):
            st.markdown("""
            <style>
            .centered {
                text-align: center;
                font-size: 20px; /* Adjust the size as needed */
                font-weight: bold; /* Makes the text bold */
                /* Add more styling as needed */
            }
            </style>
            <div class="centered">¡El dataset está listo!</div>
            """, unsafe_allow_html=True)
            # Load dataset to calculate statistics
            df = pd.read_csv(dataset_path)
            # Calculate statistics
            stats = {
                'Número de filas': [df.shape[0]],
                'Número de columnas': [df.shape[1]],
                # Add more statistics here if needed
            }
            stats_df = pd.DataFrame(stats)
            # Display statistics
            st.markdown("""
            <style>
            .centered {
                font-size: 15px; /* Adjust the size as needed */
                font-weight: bold; /* Makes the text bold */
                /* Add more styling as needed */
            }
            </style>
            <div class="centered">Final dataset</div>
            """, unsafe_allow_html=True)

            st.table(stats_df)

            cols = st.columns([1, 2, 1])
            with open(dataset_path, "rb") as file:
                cols[1].download_button(
                    label="Pulsa para acceder al dataset completo.",
                    data=file,
                    file_name="listado_productores_complete2023.csv",
                    mime="text/csv",
                )
            session_state_with_love_mottum('footer6')
        else:
            st.error("¡Necesitas ejecutar el proceso antes de venir a esta pantalla!")
            session_state_with_love_mottum('footer7')

    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2023':
        dataset_path = "data/listados_completos/listado_beneficiarios_2023.csv"
        if os.path.exists(dataset_path):
            st.markdown("""
            <style>
            .centered {
                text-align: center;
                font-size: 20px; /* Adjust the size as needed */
                font-weight: bold; /* Makes the text bold */
                /* Add more styling as needed */
            }
            </style>
            <div class="centered">¡El dataset está listo!</div>
            """, unsafe_allow_html=True)
            # Load dataset to calculate statistics
            df = pd.read_csv(dataset_path)
            # Calculate statistics
            stats = {
                'Número de filas': [df.shape[0]],
                'Número de columnas': [df.shape[1]],
                # Add more statistics here if needed
            }
            stats_df = pd.DataFrame(stats)
            # Display statistics
            st.markdown("""
            <style>
            .centered {
                font-size: 15px; /* Adjust the size as needed */
                font-weight: bold; /* Makes the text bold */
                /* Add more styling as needed */
            }
            </style>
            <div class="centered">Final dataset</div>
            """, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)
            st.table(stats_df)

            cols = st.columns([1, 2, 1])
            with open(dataset_path, "rb") as file:
                cols[1].download_button(
                    label="Pulsa aquí para acceder al dataset completo.",
                    data=file,
                    file_name="listado_beneficiarios_2023.csv",
                    mime="text/csv",
                )
            session_state_with_love_mottum('footer8')

        else:
            st.markdown(
                "<h2 style='text-align: center;'>Necesitas ejecutar el proceso antes de venir a esta pantalla.</h2>",
                unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

            cols = st.columns([1, 1, 1])
            cols[1].image('docs/images/mottum.svg', use_column_width=True)  # Colocar la imagen en la columna del medio

    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2019-2022':
        if tab == '2019':
            dataset_path = "data/listados_completos/listado_beneficiarios_2019_localidades.csv"
        elif tab == '2020':
            dataset_path = "data/listados_completos/listado_beneficiarios_2020_localidades.csv"
        elif tab == '2021':
            dataset_path = "data/listados_completos/listado_beneficiarios_2021_localidades.csv"
        elif tab == '2022':
            dataset_path = "data/listados_completos/listado_beneficiarios_2022_localidades.csv"

        if os.path.exists(dataset_path):
            st.markdown("""
            <style>
            .centered {
                text-align: center;
                font-size: 20px; /* Adjust the size as needed */
                font-weight: bold; /* Makes the text bold */
                /* Add more styling as needed */
            }
            </style>
            <div class="centered">¡El dataset está listo!</div>
            """, unsafe_allow_html=True)
            # Load dataset to calculate statistics
            df = pd.read_csv(dataset_path)
            # Calculate statistics
            stats = {
                'Número de filas': [df.shape[0]],
                'Número de columnas': [df.shape[1]],
                # Add more statistics here if needed
            }
            stats_df = pd.DataFrame(stats)
            # Display statistics
            st.markdown("""
            <style>
            .centered {
                font-size: 15px; /* Adjust the size as needed */
                font-weight: bold; /* Makes the text bold */
                /* Add more styling as needed */
            }
            </style>
            <div class="centered">Final dataset</div>
            """, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)
            st.table(stats_df)

            cols = st.columns([1, 2, 1])
            if tab == '2019':
                with open(dataset_path, "rb") as file:
                    cols[1].download_button(
                        label="Pulsa aquí para acceder al dataset completo.",
                        data=file,
                        file_name=f"listado_beneficiarios_{tab}_localidades.csv",
                        mime="text/csv",
                    )
                session_state_with_love_mottum('footer8')
            elif tab == '2020':
                with open(dataset_path, "rb") as file:
                    cols[1].download_button(
                        label="Pulsa aquí para acceder al dataset completo.",
                        data=file,
                        file_name=f"listado_beneficiarios_{tab}_localidades.csv",
                        mime="text/csv",
                    )
                session_state_with_love_mottum('footer9')
            elif tab == '2021':
                with open(dataset_path, "rb") as file:
                    cols[1].download_button(
                        label="Pulsa aquí para acceder al dataset completo.",
                        data=file,
                        file_name=f"listado_beneficiarios_{tab}_localidades.csv",
                        mime="text/csv",
                    )
                session_state_with_love_mottum('footer10')
            elif tab == '2022':
                with open(dataset_path, "rb") as file:
                    cols[1].download_button(
                        label="Pulsa aquí para acceder al dataset completo.",
                        data=file,
                        file_name=f"listado_beneficiarios_{tab}_localidades.csv",
                        mime="text/csv",
                    )
                session_state_with_love_mottum('footer11')

        else:
            st.markdown(
                "<h2 style='text-align: center;'>Necesitas ejecutar el proceso antes de venir a esta pantalla.</h2>",
                unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

            cols = st.columns([1, 1, 1])
            cols[1].image('docs/images/mottum.svg', use_column_width=True)  # Colocar la imagen en la columna del medio


if __name__ == '__main__':
    # The overall structure definition of the app is as defined here.
    st.markdown('''
                <h1 style='text-align: center; color: black; font-size: 30px;'>Servicio de ingeniería de datos para la extracción,
                transformación y carga del Programa de Fertilizantes para el bienestar.
                </h1> 
                '''
                , unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    # Sidebar
    st.sidebar.image('docs/images/SESNA.png', use_column_width=True)

    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    if 'main_page' not in st.session_state:
        st.session_state.main_page = 'Productores autorizados 2023'
    with st.sidebar:
        st.session_state.main_page = om('Main menu',
                                        ['Productores autorizados 2023', 'Beneficiarios fertilizantes 2023',
                                         'Beneficiarios fertilizantes 2019-2022'],
                                        icons=["list-task", "list-task", "list-task"], menu_icon="cast")
    if st.session_state.main_page == 'Productores autorizados 2023':
        if 'sub_page' not in st.session_state:
            st.session_state.sub_page = '1. Introducción'
        st.session_state.sub_page = st.radio('Productores autorizados 2023',
                                             ['1. Introducción', '2. Descarga y Transformación', '3. Limpieza de datos',
                                              '4. Acceso a las tablas de resultados [.csv]'])
        if st.session_state.sub_page == '1. Introducción':
            show_intro(1)
        elif st.session_state.sub_page == '2. Descarga y Transformación':
            start_process(1)
        elif st.session_state.sub_page == '3. Limpieza de datos':
            clean_data_screen(1, '0')
        elif st.session_state.sub_page == '4. Acceso a las tablas de resultados [.csv]':
            show_finished('1')
    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2023':
        if 'second_sub_page' not in st.session_state:
            st.session_state.second_sub_page = '1. Introducción'
        st.session_state.second_sub_page = st.radio('Beneficiarios fertilizantes 2023',
                                                    ['1. Introducción', '2. Descarga y Transformación',
                                                     '3. Limpieza de datos',
                                                     '4. Acceso a las tablas de resultados [.csv]'])
        if st.session_state.second_sub_page == '1. Introducción':
            show_intro(2)
        elif st.session_state.second_sub_page == '2. Descarga y Transformación':
            start_process(2)
        elif st.session_state.second_sub_page == '3. Limpieza de datos':
            clean_data_screen(2, '0')
        elif st.session_state.second_sub_page == '4. Acceso a las tablas de resultados [.csv]':
            show_finished('2')
    elif st.session_state.main_page == 'Beneficiarios fertilizantes 2019-2022':
        # Crear pestañas para los años
        tabs = st.tabs(['2019', '2020', '2021', '2022'])

        with tabs[0]:
            st.subheader("Beneficiarios fertilizantes 2019")
            if 'third_sub_page_2019' not in st.session_state:
                st.session_state.third_sub_page_2019 = '1. Introducción'
            st.session_state.third_sub_page_2019 = st.radio('Beneficiarios fertilizantes 2019',
                                                            ['1. Introducción', '2. Descarga y Transformación',
                                                             '3. Limpieza de datos',
                                                             '4. Acceso a las tablas de resultados [.csv]'])
            if st.session_state.third_sub_page_2019 == '1. Introducción':
                show_intro(3)
            elif st.session_state.third_sub_page_2019 == '2. Descarga y Transformación':
                start_process(3)
            elif st.session_state.third_sub_page_2019 == '3. Limpieza de datos':
                clean_data_screen(3, '2019')
            elif st.session_state.third_sub_page_2019 == '4. Acceso a las tablas de resultados [.csv]':
                show_finished('2019')

        with tabs[1]:
            st.subheader("Beneficiarios fertilizantes 2020")
            if 'third_sub_page_2020' not in st.session_state:
                st.session_state.third_sub_page_2020 = '1. Introducción'
            st.session_state.third_sub_page_2020 = st.radio('Beneficiarios fertilizantes 2020',
                                                            ['1. Introducción', '2. Descarga y Transformación',
                                                             '3. Limpieza de datos',
                                                             '4. Acceso a las tablas de resultados [.csv]'])
            if st.session_state.third_sub_page_2020 == '1. Introducción':
                show_intro(4)
            elif st.session_state.third_sub_page_2020 == '2. Descarga y Transformación':
                start_process(4)
            elif st.session_state.third_sub_page_2020 == '3. Limpieza de datos':
                clean_data_screen(4, '2020')
            elif st.session_state.third_sub_page_2020 == '4. Acceso a las tablas de resultados [.csv]':
                show_finished('2020')

        with tabs[2]:
            st.subheader("Beneficiarios fertilizantes 2021")
            if 'third_sub_page_2021' not in st.session_state:
                st.session_state.third_sub_page_2021 = '1. Introducción'
            st.session_state.third_sub_page_2021 = st.radio('Beneficiarios fertilizantes 2021',
                                                            ['1. Introducción', '2. Descarga y Transformación',
                                                             '3. Limpieza de datos',
                                                             '4. Acceso a las tablas de resultados [.csv]'])
            if st.session_state.third_sub_page_2021 == '1. Introducción':
                show_intro(5)
            elif st.session_state.third_sub_page_2021 == '2. Descarga y Transformación':
                start_process(5)
            elif st.session_state.third_sub_page_2021 == '3. Limpieza de datos':
                clean_data_screen(5, '2021')
            elif st.session_state.third_sub_page_2021 == '4. Acceso a las tablas de resultados [.csv]':
                show_finished('2021')

        with tabs[3]:
            st.subheader("Beneficiarios fertilizantes 2022")
            if 'third_sub_page_2022' not in st.session_state:
                st.session_state.third_sub_page_2022 = '1. Introducción'
            st.session_state.third_sub_page_2022 = st.radio('Beneficiarios fertilizantes 2022',
                                                            ['1. Introducción', '2. Descarga y Transformación',
                                                             '3. Limpieza de datos',
                                                             '4. Acceso a las tablas de resultados [.csv]'])
            if st.session_state.third_sub_page_2022 == '1. Introducción':
                show_intro(6)
            elif st.session_state.third_sub_page_2022 == '2. Descarga y Transformación':
                start_process(6)
            elif st.session_state.third_sub_page_2022 == '3. Limpieza de datos':
                clean_data_screen(6, '2022')
            elif st.session_state.third_sub_page_2022 == '4. Acceso a las tablas de resultados [.csv]':
                show_finished('2022')

    st.sidebar.markdown(
        """
        La secretaria ejecutiva del Sistema Nacional Anticorrupción 
        (SESNA) es el organismo de apoyo técnico dedicado al combate 
        contra la corrupción en México.
        """
    )

    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    st.sidebar.markdown(
        """
        Esta página ha sido desarrollada por [mottum](https://mottum.io/) con el fin de
        estandarizar, transformar y analizar los datos del Programa de Fertilizantes
        para el bienestar.
        """
    )
    # Initialize session state variables
    st.markdown(
        """
        <style>
            [data-testid=stSidebar] [data-testid=stImage]{
                text-align: center;
                display: block;
                margin-left: auto;
                margin-right: auto;
                width: 100%;
            }
        </style>
        """, unsafe_allow_html=True
    )

    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    st.sidebar.markdown(
        """
        Visita los siguientes links para más información:
        """
    )

    st.sidebar.markdown(
        """
    - [Link al repositorio](https://github.com/MottumData/SESNA-Fertilizantes)
    - [Jupyter](http://localhost:8888/lab)
    """
    )

    st.sidebar.markdown("<br>", unsafe_allow_html=True)

    st.sidebar.markdown(
        '**Financiado por:**',
        unsafe_allow_html=True
    )
    cols = st.columns([1, 1, 1])

    st.sidebar.image('docs/images/UNDP2.png', width=100)

    if 'button_pressed' not in st.session_state:
        st.session_state.button_pressed = False

    st.markdown("<br>", unsafe_allow_html=True)

# image_placeholder.image('docs/images/mottum.svg', width=500)
