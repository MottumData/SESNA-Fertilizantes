# SESNA-Fertilizantes

Esta herramienta digital forma parte de la [Secretaría Ejecutiva del Sistema Nacional de Anticorrupción de la República
de México](https://www.sesna.gob.mx/) (SENA)

El programa de Fertilizantes es un programa a nivel federal, gestionado y coordinado a nivel estatal. Para llevar un
mejor control del impacto de este programa de ayudas sociales, que consiste en el reparto de ayudas de fertilizantes a
los productores, se publica en el portal Datos.gob.mx un listado de productores autorizados y un listado de productores
beneficiarios. (Ver portal de datos abiertos).

El objetivo de la SESNA es crear un padrón de beneficiarios que permita el análisis de este programa tratando cada Entidad, Municipio y Localidad por su código de
INEGI.

Los datasets de beneficiarios y productores autorizados disponen de la localización de los beneficiarios, sin embargos
estos no siempres están codificados según el [Catálogo Único de Claves de Áreas Geoestadísticas Estatales, Municipales y
Localidades](https://www.inegi.org.mx/app/ageeml). El propósito final de este repositorio es estandarizar las claves de
entidades, municipios y localidades según el Catálogo de Claves Únicas.

## Estructura del Proyecto

Este proyecto está estructurado en varios directorios, cada uno con un propósito específico.
Sus funciones son las siguientes:

- `data/`: Contiene los datos de entrada y salida del proyecto. Así como los diccionarios de datos.
- `docs/`: Contiene la documentación del proyecto.
- `logs/`: Contiene los logs o registros generados por el proyecto.
- `src/`: Contiene el código fuente del proyecto.
- `src/notebooks`: Contiene los Jupyter Notebooks.
- `tests/`: Contiene los tests del proyecto.

### Estructura de la carpeta de datos

- `inegi`. Contienen los datos de INEGI para cada año. El corte usado de cada año es el de Diciembre. A excepción
  de `dataset_inegi.csv` que contiene los del último año y mes disponibles, en este caso es Abril de 2024.
- `listados_completos`: Contiene los listados completos de beneficiarios y productores autorizados tras la corrección de
  nombres. Así como los
  beneficiarios de 2019-2023.
- `productores_autorizados`. Contiene los listados de Productores Autorizados descargados directamente
  desde [Secretaría de Agricultura y Desarrollo Rural](https://datos.gob.mx/busca/organization/agricultura) de
  [datos.gob.mx](https://www.datos.gob.mx). Además, contiene los diccionarios.
- `productores_beneficiarios` Contiene los listados de Productores Beneficiarios descargados directamente
  desde [Secretaría de Agricultura y Desarrollo Rural](https://datos.gob.mx/busca/organization/agricultura) de
   [datos.gob.mx](https://www.datos.gob.mx). Además, contiene los diccionarios.
- `productores_beneficiarios 2019-2022` Contiene los listados de Productores Beneficiarios de 2019 a 2022
  descargados directamente
  desde [Secretaría de Agricultura y Desarrollo Rural](https://datos.gob.mx/busca/organization/agricultura) de
  [datos.gob.mx](https://www.datos.gob.mx). Además, contiene los diccionarios para cada año en un directorio adicional.

### Acceso a Datos :open_file_folder:

- Datos de descarga. Son aquellos que tienen automatizada la descarga. En este caso corresponden con los datos de
  Listado de Beneficiarios de 2019 a 2023 y Productores Autorizados.
- Datos de acceso Repositorio. Aquellos que se usan como herramienta dentro del proyecto. En este caso los diccionarios
  para cada entregable y los dataset de INEGI.
- En drive, en la carpeta de datos podemos encontrar la carpeta de data. Si surge algun problema recomendamos que descarguen la carpeta data y la sustituyan por la carpeta data en la raíz del repositorio.

> **Nota:** Para descargar los datos adjuntos en este repositorio es necesario ubicarse en el directorio del proyecto y
> ejecutar el siguiente comando: `git lfs pull`.
>
> Normalmente LFS se instala automáticamente al clonar un repositorio que lo usa, pero si no es así, se puede instalar
> siguiendo las instrucciones de la [documentación oficial](https://git-lfs.com/).

## Ejecución Local :house: :computer:

Se recomienda encarecidamente el uso de un entorno virtual para la ejecución de este proyecto.
Para crear un entorno virtual puede consultar la siguiente documentación:

- [Entornos Virtuales en Python](https://docs.python.org/3/library/venv.html)
- [Entornos Virtuales en Conda](https://docs.conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html)
- [Entornos Virtuales en Pipenv](https://pipenv-es.readthedocs.io/es/stable/basics.html)

Una vez creado el entorno virtual, actívelo y siga los siguientes pasos para ejecutar el proyecto.

1. **Clonar el repositorio**. Desde un terminal con git instalado, ejecute el siguiente comando:

```bash
git clone https://github.com/MottumData/SESNA-Fertilizantes.git
```

2. **Cambiar de directorio**. Una vez clonado el repositorio, cambie al directorio del proyecto:

```bash
cd SESNA-Fertilizantes
```

3. **Descargar datos del repositorio**. (En caso de fallo, comprueba que tengas instalado git LFS, como se indica en [Acceso a Datos](#acceso-a-datos-open_file_folder))

```bash
git lfs pull
```

4. **Instalar dependencias**. Para instalar las dependencias del proyecto, ejecute el siguiente comando:

```bash
pip install -r requirements.txt
```

5. (Opcional) **Ejecutar test**. Para ejecutar los tests del proyecto, ejecute el siguiente comando:

```python
pytest
```

6. **Ejecución del proyecto**. Para ejecutar el proyecto, ejecute el siguiente comando:

```python
streamlit run main.py
```

A continuación se mostrará un mensaje similar al siguiente:

```bash
  You can now view your Streamlit app in your browser.

  Local URL: http://localhost:8501
  Network URL: http://0.0.0.0:8501
```

## Docker :whale:

Para ejecutar el proyecto en un contenedor de Docker, ejecute los siguientes comandos:

1. **Clonar el repositorio**. Desde un terminal con git instalado, ejecute el siguiente comando:

```bash
git clone https://github.com/MottumData/SESNA-Fertilizantes.git
```

2. **Cambiar de directorio**. Una vez clonado el repositorio, cambie al directorio del proyecto:

```bash
cd SESNA-Fertilizantes
```

3. **Construir la imagen de Docker y levantar contenedor**. Ejecute el siguiente comando:

```bash
docker compose up --build # -d para ejecutar en segundo plano
```
Una vez ejecutado el contenedor de Docker, podrá acceder a las interfaces de usuario a través de los siguientes enlaces:

- Interfaz de Streamlit: http://localhost:8502/
- Jupyter Notebooks: http://localhost:8889/
