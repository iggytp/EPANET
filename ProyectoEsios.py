#!/usr/bin/env python
# coding: utf-8
##CARGA DE LIBRERÍAS
import requests
import pandas as pd
import time 
import os
from elasticsearch import Elasticsearch, helpers 


    # # 1. Set up your credentials in a dictionary

token = '7d3f64408f1761c249267d044924fb41d7e5aa73303c9875747a8b02258c7fd6'
headers = dict()
headers['Host'] = 'api.esios.ree.es'
headers['x-api-key'] = token

    # # 2. Request the indicators list

url = 'https://api.esios.ree.es/indicators'
res = requests.get(url, headers = headers)

    # # 3. Get the JSON
data = res.json()

    # # 4. Create a dataframe 
df_indicators = pd.DataFrame(data['indicators'])

    # 4.1 Set the id columns as the index
df_indicators = df_indicators.set_index('id')
    
    # CONSULTAS A LA API ESIOS

    # ID: 1293
    # 
    # DATOS: DEMANDA REAL EN LA PENÍNSULA
    # 
    # INFO: Es el valor real de la demanda de energía eléctrica medida en tiempo real. Los datos representados en este indicador se refieren a datos Peninsulares.

    # Importar time para el delay

def obtener_datos_demanda_real_peninsula():
        # URL del indicador de demanda real
        url_demanda_real_peninsula = "https://api.esios.ree.es/indicators/1293"

        # Lista para almacenar los DataFrames de cada mes
        dataframes = []


        # Rango de años y meses que deseas consultar
        for year in range(2019, 2025):  # Cambia el rango según el periodo deseado (5 años)
            for month in range(1, 13):  # Itera sobre los 12 meses del año
                # Configura las fechas de inicio y fin para cada mes
                start_date = f"{year}-{month:02d}-01T00:00:00Z"
                
                # Determinar el último día del mes para `end_date`
                if month == 12:
                    end_date = f"{year}-10-31T24:59:59Z"
                else:
                    end_date = f"{year}-{month+1:02d}-01T00:00:00Z"
                
                # Solicitar datos para el mes actual
                params = {
                    "start_date": start_date,
                    "end_date": end_date
                }
                response = requests.get(url_demanda_real_peninsula, headers=headers, params=params)
                
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:
                    # Convertir JSON en DataFrame
                    demanda_real_json = response.json()
                    df_month = pd.json_normalize(demanda_real_json['indicator']['values'])  # Ajusta según la estructura de tu JSON
                    dataframes.append(df_month)
                else:
                    # Capturar y mostrar el mensaje de error detallado
                    try:
                        error_message = response.json()  # Intentar obtener el mensaje JSON del error
                    except ValueError:
                        error_message = response.text  # Si no es JSON, mostrar texto sin formato
                    
                    print(f"Error {response.status_code} al obtener datos para {year}-{month:02d}: {error_message}")
                
                # Delay entre cada solicitud de mes
                time.sleep(0.2)  # Pausa de 3 segundos (ajústalo según las políticas de la API)

        # Concatenar todos los DataFrames en uno solo (si se obtuvieron datos)
        if dataframes:
            df_demanda_real_peninsula = pd.concat(dataframes, ignore_index=True)
            # Mostrar el DataFrame resultante
            print(df_demanda_real_peninsula.head())
        else:
            print("No se pudieron obtener datos para ningún mes.")
        df_demanda_real_peninsula.to_csv("demanda_real_peninsula_2019_2024.csv", index=False)
        print("DATOS OBTENIDOS DE DEMANDA REAL PENÍNSULA")


    # ID: 545
    # 
    # DEMANDA PROGRAMADA PENINSULAR
    # 
    # INFO: Es la demanda de energía programada calculada mediante los intercambios internacionales programados y los programas de los grupos de generación a los que se les haya asignado suministro de energía en la casación de los mercados diario e intradiario, así como en los mercados de energías de balance RR y regulación terciaria. Estos dos últimos son gestionados por RE teniendo en cuenta la evolución de la demanda.</p><p>Los datos representados en este indicador se refieren a datos Peninsulares.</p><p><b>Publicación:</b> diariamente a las 0:00 horas con la información del día D y a las 13:30 horas con las tres primeras horas del día D+1. Un valor cada cinco minutos.
    # 
def obtener_datos_demanda_programada_peninsula():
        # URL del indicador de demanda programada
        url_demanda_programada_peninsula = "https://api.esios.ree.es/indicators/545"

        # Lista para almacenar los DataFrames de cada mes
        dataframes = []

        # Rango de años y meses que deseas consultar
        for year in range(2019, 2025):  # Cambia el rango según el periodo deseado (5 años)
            for month in range(1, 13):  # Itera sobre los 12 meses del año
                # Configura las fechas de inicio y fin para cada mes
                start_date = f"{year}-{month:02d}-01T00:00:00Z"
                
                # Determinar el último día del mes para `end_date`
                if month == 12:
                    end_date = f"{year + 1}-01-01T00:00:00Z"  # Cambia al primer día del próximo año
                else:
                    end_date = f"{year}-{month + 1:02d}-01T00:00:00Z"
                
                # Solicitar datos para el mes actual
                params = {
                    "start_date": start_date,
                    "end_date": end_date
                }
                response = requests.get(url_demanda_programada_peninsula, headers=headers, params=params)
                
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:
                    # Convertir JSON en DataFrame
                    demanda_programada_json = response.json()
                    df_month = pd.json_normalize(demanda_programada_json['indicator']['values'])  # Ajusta según la estructura de tu JSON
                    dataframes.append(df_month)
                else:
                    # Capturar y mostrar el mensaje de error detallado
                    try:
                        error_message = response.json()  # Intentar obtener el mensaje JSON del error
                    except ValueError:
                        error_message = response.text  # Si no es JSON, mostrar texto sin formato
                    
                    print(f"Error {response.status_code} al obtener datos para {year}-{month:02d}: {error_message}")
                
                # Delay entre cada solicitud de mes
                time.sleep(0.2)  # Pausa de 1 segundo (ajústalo según las políticas de la API)

        # Concatenar todos los DataFrames en uno solo (si se obtuvieron datos)
        if dataframes:
            df_demanda_programada_peninsula = pd.concat(dataframes, ignore_index=True)
            # Mostrar el DataFrame resultante
            print(df_demanda_programada_peninsula.head())
        else:
            print("No se pudieron obtener datos para ningún mes.")
        df_demanda_programada_peninsula.to_csv("demanda_programada_peninsula_2019_2024.csv", index=False)


    # ID: 544
    # 
    # DEMANDA PREVISTA PENINSULAR
    # 
    # INFO: Es la previsión de consumo que RE elabora teniendo en cuenta los valores de consumo registrados en periodos precedentes similares,
    #  así como otros factores con influencia en el consumo eléctrico como la laboralidad, la climatología y la actividad económica. Los valores cincominutales son representandos en forma de curva aplicando unos parámetros que parabolizan el valor constante horario.</p><p>Los datos representados en este indicador se refieren a datos Peninsulares.</p><p><b>Publicación:</b> diariamente a las 0:00 horas con la información del día D y a las 13:30 horas con las tres primeras horas del día D+1. Un valor cada cinco minutos.

def obtener_datos_demanda_prevista_peninsula():
        # URL del indicador de demanda prevista
        url_demanda_prevista_peninsula = "https://api.esios.ree.es/indicators/544"

        # Lista para almacenar los DataFrames de cada mes
        dataframes = []

        # Rango de años y meses que deseas consultar
        for year in range(2019, 2025):  # Cambia el rango según el periodo deseado (5 años)
            for month in range(1, 13):  # Itera sobre los 12 meses del año
                # Configura las fechas de inicio y fin para cada mes
                start_date = f"{year}-{month:02d}-01T00:00:00Z"
                
                # Determinar el último día del mes para `end_date`
                if month == 12:
                    end_date = f"{year + 1}-01-01T00:00:00Z"  # Cambia al primer día del próximo año
                else:
                    end_date = f"{year}-{month + 1:02d}-01T00:00:00Z"
                
                # Solicitar datos para el mes actual
                params = {
                    "start_date": start_date,
                    "end_date": end_date
                }
                response = requests.get(url_demanda_prevista_peninsula, headers=headers, params=params)
                
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:
                    # Convertir JSON en DataFrame
                    demanda_prevista_json = response.json()
                    df_month = pd.json_normalize(demanda_prevista_json['indicator']['values'])  # Ajusta según la estructura de tu JSON
                    dataframes.append(df_month)
                else:
                    # Capturar y mostrar el mensaje de error detallado
                    try:
                        error_message = response.json()  # Intentar obtener el mensaje JSON del error
                    except ValueError:
                        error_message = response.text  # Si no es JSON, mostrar texto sin formato
                    
                    print(f"Error {response.status_code} al obtener datos para {year}-{month:02d}: {error_message}")
                
                # Delay entre cada solicitud de mes
                time.sleep(0.2)  # Pausa de 1 segundo (ajústalo según las políticas de la API)

        # Concatenar todos los DataFrames en uno solo (si se obtuvieron datos)
        if dataframes:
            df_demanda_prevista_peninsula = pd.concat(dataframes, ignore_index=True)
            # Mostrar el DataFrame resultante
            print(df_demanda_prevista_peninsula.head())
        else:
            print("No se pudieron obtener datos para ningún mes.")
        df_demanda_prevista_peninsula.to_csv("demanda_prevista_peninsula_2019_2024.csv", index=False)

    # ID: 359
    # 
    # Demanda programada PVP Comercializadores de referencia
    # 
    # INFO: Energía correspondiente al programa diario, con desglose por periodos de programación, 
    # que incorpora las modificaciones introducidas en el PDBF para la resolución de las restricciones 
    # técnicas identificadas por criterios de seguridad y para el reequilibrio posterior generación-demanda.</p>
    # <p>En concreto este indicador se refiere a las unidades de programación de tipo Comercializadores de referencia.</p>
    # <p><b>Publicación:</b> diariamente a partir de las 16:00 horas con la información del día D+1.</p>
    # URL del indicador de demanda programada PVP Comercializadores de referencia
def obtener_datos_demanda_programa_comercializadores():
        url_demanda_programada_comercializadores = "https://api.esios.ree.es/indicators/359"

        # Lista para almacenar los DataFrames de cada mes
        dataframes_comercializadores = []

        # Rango de años y meses que deseas consultar
        for year in range(2019, 2025):  # Cambia el rango según el periodo deseado (5 años)
            for month in range(1, 13):  # Itera sobre los 12 meses del año
                # Configura las fechas de inicio y fin para cada mes
                start_date = f"{year}-{month:02d}-01T00:00:00Z"
                
                # Determinar el último día del mes para `end_date`
                if month == 12:
                    end_date = f"{year + 1}-01-01T00:00:00Z"  # Cambia al primer día del próximo año
                else:
                    end_date = f"{year}-{month + 1:02d}-01T00:00:00Z"
                
                # Solicitar datos para el mes actual
                params = {
                    "start_date": start_date,
                    "end_date": end_date
                }
                response = requests.get(url_demanda_programada_comercializadores, headers=headers, params=params)
                
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:
                    # Convertir JSON en DataFrame
                    demanda_programada_json = response.json()
                    df_mes_comercializadores = pd.json_normalize(demanda_programada_json['indicator']['values'])  # Ajusta según la estructura de tu JSON
                    dataframes_comercializadores.append(df_mes_comercializadores)
                else:
                    # Capturar y mostrar el mensaje de error detallado
                    try:
                        error_message = response.json()  # Intentar obtener el mensaje JSON del error
                    except ValueError:
                        error_message = response.text  # Si no es JSON, mostrar texto sin formato
                    
                    print(f"Error {response.status_code} al obtener datos para {year}-{month:02d}: {error_message}")
                
                # Delay entre cada solicitud de mes
                time.sleep(0.1)  # Pausa de 1 segundo (ajústalo según las políticas de la API)

        # Concatenar todos los DataFrames en uno solo (si se obtuvieron datos)
        if dataframes_comercializadores:
            df_demanda_programada_comercializadores = pd.concat(dataframes_comercializadores, ignore_index=True)
            # Mostrar el DataFrame resultante
            print(df_demanda_programada_comercializadores.head())
        else:
            print("No se pudieron obtener datos para ningún mes.")
        df_demanda_programada_comercializadores.to_csv("demanda_programada_comercializadores_2019_2024.csv", index=False)

    # ID: 2052
    # 
    # Demanda real prevista nacional
    # 
    # INFO: 
    # URL del indicador de demanda real prevista nacional
def obtener_datos_demanda_real_prevista_nacional():
        url_demanda_real_prevista_nacional = "https://api.esios.ree.es/indicators/2052"

        # Lista para almacenar los DataFrames de cada mes
        dataframes_real_prevista = []

        # Rango de años y meses que deseas consultar
        for year in range(2019, 2025):  # Cambia el rango según el periodo deseado (5 años)
            for month in range(1, 13):  # Itera sobre los 12 meses del año
                # Configura las fechas de inicio y fin para cada mes
                start_date = f"{year}-{month:02d}-01T00:00:00Z"
                
                # Determinar el último día del mes para `end_date`
                if month == 12:
                    end_date = f"{year + 1}-01-01T00:00:00Z"  # Cambia al primer día del próximo año
                else:
                    end_date = f"{year}-{month + 1:02d}-01T00:00:00Z"
                
                # Solicitar datos para el mes actual
                params = {
                    "start_date": start_date,
                    "end_date": end_date
                }
                response = requests.get(url_demanda_real_prevista_nacional, headers=headers, params=params)
                
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:
                    # Convertir JSON en DataFrame
                    demanda_real_prevista_json = response.json()
                    df_mes_real_prevista = pd.json_normalize(demanda_real_prevista_json['indicator']['values'])  # Ajusta según la estructura de tu JSON
                    dataframes_real_prevista.append(df_mes_real_prevista)
                else:
                    # Capturar y mostrar el mensaje de error detallado
                    try:
                        error_message = response.json()  # Intentar obtener el mensaje JSON del error
                    except ValueError:
                        error_message = response.text  # Si no es JSON, mostrar texto sin formato
                    
                    print(f"Error {response.status_code} al obtener datos para {year}-{month:02d}: {error_message}")
                
                # Delay entre cada solicitud de mes
                time.sleep(0.2)  # Pausa de 1 segundo (ajústalo según las políticas de la API)

        # Concatenar todos los DataFrames en uno solo (si se obtuvieron datos)
        if dataframes_real_prevista:
            df_demanda_real_prevista_nacional = pd.concat(dataframes_real_prevista, ignore_index=True)
            # Mostrar el DataFrame resultante
            print(df_demanda_real_prevista_nacional.head())
        else:
            print("No se pudieron obtener datos para ningún mes.")
        df_demanda_real_prevista_nacional.to_csv("demanda_real_prevista_nacional_2019_2024.csv", index=False)

    # ID: 2053
    # 
    # Demanda real programada nacional
    # 
    # INFO: 
    # URL del indicador de demanda real programada nacional
def obtener_datos_demanda_real_programada_nacional():
        url_demanda_real_programada_nacional = "https://api.esios.ree.es/indicators/2053"

        # Lista para almacenar los DataFrames de cada mes
        dataframes_real_programada = []

        # Rango de años y meses que deseas consultar
        for year in range(2019, 2025):  # Cambia el rango según el periodo deseado (5 años)
            for month in range(1, 13):  # Itera sobre los 12 meses del año
                # Configura las fechas de inicio y fin para cada mes
                start_date = f"{year}-{month:02d}-01T00:00:00Z"
                
                # Determinar el último día del mes para `end_date`
                if month == 12:
                    end_date = f"{year + 1}-01-01T00:00:00Z"  # Cambia al primer día del próximo año
                else:
                    end_date = f"{year}-{month + 1:02d}-01T00:00:00Z"
                
                # Solicitar datos para el mes actual
                params = {
                    "start_date": start_date,
                    "end_date": end_date
                }
                response = requests.get(url_demanda_real_programada_nacional, headers=headers, params=params)
                
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:
                    # Convertir JSON en DataFrame
                    demanda_real_programada_json = response.json()
                    df_mes_real_programada = pd.json_normalize(demanda_real_programada_json['indicator']['values'])  # Ajusta según la estructura de tu JSON
                    dataframes_real_programada.append(df_mes_real_programada)
                else:
                    # Capturar y mostrar el mensaje de error detallado
                    try:
                        error_message = response.json()  # Intentar obtener el mensaje JSON del error
                    except ValueError:
                        error_message = response.text  # Si no es JSON, mostrar texto sin formato
                    
                    print(f"Error {response.status_code} al obtener datos para {year}-{month:02d}: {error_message}")
                
                # Delay entre cada solicitud de mes
                time.sleep(0.2)  # Pausa de 1 segundo (ajústalo según las políticas de la API)

        # Concatenar todos los DataFrames en uno solo (si se obtuvieron datos)
        if dataframes_real_programada:
            df_demanda_real_programada_nacional = pd.concat(dataframes_real_programada, ignore_index=True)
            # Mostrar el DataFrame resultante
            print(df_demanda_real_programada_nacional.head())
        else:
            print("No se pudieron obtener datos para ningún mes.")
        df_demanda_real_programada_nacional.to_csv("demanda_real_programada_nacional_2019_2024.csv", index=False)

    # ID: 10351
    # 
    # Generación T.real renovable
    # 
    # INFO: Generación medida en tiempo real del tipo de producción renovable.</p><p><b>Publicación:</b>cada 10 
    # minutos con la información de las tres últimas horas del día D-1 hasta la hora actual del día D.</p>
    # URL del indicador de generación T. Real renovable
def obtener_datos_generacion_real_renovable():
        url_generacion_real_renovable = "https://api.esios.ree.es/indicators/10351"

        # Lista para almacenar los DataFrames de cada mes
        dataframes_generacion_renovable = []

        # Rango de años y meses que deseas consultar
        for year in range(2019, 2025):  # Cambia el rango según el periodo deseado (5 años)
            for month in range(1, 13):  # Itera sobre los 12 meses del año
                # Configura las fechas de inicio y fin para cada mes
                start_date = f"{year}-{month:02d}-01T00:00:00Z"
                
                # Determinar el último día del mes para `end_date`
                if month == 12:
                    end_date = f"{year + 1}-01-01T00:00:00Z"  # Cambia al primer día del próximo año
                else:
                    end_date = f"{year}-{month + 1:02d}-01T00:00:00Z"
                
                # Solicitar datos para el mes actual
                params = {
                    "start_date": start_date,
                    "end_date": end_date
                }
                response = requests.get(url_generacion_real_renovable, headers=headers, params=params)
                
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:
                    # Convertir JSON en DataFrame
                    generacion_real_renovable_json = response.json()
                    if 'indicator' in generacion_real_renovable_json and 'values' in generacion_real_renovable_json['indicator']:
                        df_mes_generacion_renovable = pd.json_normalize(generacion_real_renovable_json['indicator']['values'])
                        dataframes_generacion_renovable.append(df_mes_generacion_renovable)
                    else:
                        print(f"Datos no encontrados en {year}-{month:02d}. JSON recibido: {generacion_real_renovable_json}")
                else:
                    # Capturar y mostrar el mensaje de error detallado
                    try:
                        error_message = response.json()  # Intentar obtener el mensaje JSON del error
                    except ValueError:
                        error_message = response.text  # Si no es JSON, mostrar texto sin formato
                    
                    print(f"Error {response.status_code} al obtener datos para {year}-{month:02d}: {error_message}")
                
                # Delay entre cada solicitud de mes
                time.sleep(0.2)  # Pausa de 1 segundo (ajústalo según las políticas de la API)

        # Concatenar todos los DataFrames en uno solo (si se obtuvieron datos)
        if dataframes_generacion_renovable:
            df_generacion_real_renovable = pd.concat(dataframes_generacion_renovable, ignore_index=True)
            # Mostrar el DataFrame resultante
            print(df_generacion_real_renovable.head())
        else:
            print("No se pudieron obtener datos para ningún mes.")
        df_generacion_real_renovable.to_csv("generacion_tiemporeal_renovable_2019_2024.csv", index=False)


    # ID: 10352
    # 
    # Generación T.real no renovable
    # 
    # INFO: Generación medida en tiempo real del tipo de producción NO renovable.</p><p><b>Publicación:</b>cada 10 minutos 
    # con la información de las tres últimas horas del día D-1 hasta la hora actual del día D.</p>
    # URL del indicador de generación T. Real no renovable
def obtener_datos_generacion_real_no_renovable():
        url_generacion_real_no_renovable = "https://api.esios.ree.es/indicators/10352"

        # Lista para almacenar los DataFrames de cada mes
        dataframes_generacion_no_renovable = []

        # Rango de años y meses que deseas consultar
        for year in range(2019, 2025):  # Cambia el rango según el periodo deseado (5 años)
            for month in range(1, 13):  # Itera sobre los 12 meses del año
                # Configura las fechas de inicio y fin para cada mes
                start_date = f"{year}-{month:02d}-01T00:00:00Z"
                
                # Determinar el último día del mes para `end_date`
                if month == 12:
                    end_date = f"{year + 1}-01-01T00:00:00Z"  # Cambia al primer día del próximo año
                else:
                    end_date = f"{year}-{month + 1:02d}-01T00:00:00Z"
                
                # Solicitar datos para el mes actual
                params = {
                    "start_date": start_date,
                    "end_date": end_date
                }
                response = requests.get(url_generacion_real_no_renovable, headers=headers, params=params)
                
                # Verificar si la solicitud fue exitosa
                if response.status_code == 200:
                    # Convertir JSON en DataFrame
                    generacion_real_no_renovable_json = response.json()
                    if 'indicator' in generacion_real_no_renovable_json and 'values' in generacion_real_no_renovable_json['indicator']:
                        df_mes_generacion_no_renovable = pd.json_normalize(generacion_real_no_renovable_json['indicator']['values'])
                        dataframes_generacion_no_renovable.append(df_mes_generacion_no_renovable)
                    else:
                        print(f"Datos no encontrados en {year}-{month:02d}. JSON recibido: {generacion_real_no_renovable_json}")
                else:
                    # Capturar y mostrar el mensaje de error detallado
                    try:
                        error_message = response.json()  # Intentar obtener el mensaje JSON del error
                    except ValueError:
                        error_message = response.text  # Si no es JSON, mostrar texto sin formato
                    
                    print(f"Error {response.status_code} al obtener datos para {year}-{month:02d}: {error_message}")
                
                # Delay entre cada solicitud de mes
                time.sleep(0.2)  # Pausa de 1 segundo (ajústalo según las políticas de la API)

        # Concatenar todos los DataFrames en uno solo (si se obtuvieron datos)
        if dataframes_generacion_no_renovable:
            df_generacion_real_no_renovable = pd.concat(dataframes_generacion_no_renovable, ignore_index=True)
            # Mostrar el DataFrame resultante
            print(df_generacion_real_no_renovable.head())
        else:
            print("No se pudieron obtener datos para ningún mes.")
        df_generacion_real_no_renovable.to_csv("generacion_tiempo_real_no_renovable_2019_2024.csv", index=False)

csv_files_functions = {
        "demanda_real_peninsula_2019_2024.csv": "obtener_datos_demanda_real_peninsula",
        "demanda_programada_peninsula_2019_2024.csv": "obtener_datos_demanda_programada_peninsula",
        "demanda_prevista_peninsula_2019_2024.csv": "obtener_datos_demanda_prevista_peninsula",
        "demanda_programada_comercializadores_2019_2024.csv": "obtener_datos_demanda_programa_comercializadores",
        "demanda_real_prevista_nacional_2019_2024.csv": "obtener_datos_demanda_real_prevista_nacional",
        "demanda_real_programada_nacional_2019_2024.csv": "obtener_datos_demanda_real_programada_nacional",
        "generacion_tiemporeal_renovable_2019_2024.csv": "obtener_datos_generacion_real_renovable",
        "generacion_tiempo_real_no_renovable_2019_2024.csv": "obtener_datos_generacion_real_no_renovable"
    }

def lecturaDatosAPI():
        for csv_file, function_name in csv_files_functions.items():
            if not os.path.isfile(csv_file):
                print(f"Archivo {csv_file} no encontrado. Ejecutando {function_name}() para obtener datos.")
                if function_name in globals():
                    globals()[function_name]()
                else:
                    print(f"Función {function_name} no encontrada.")
            else:
                print(f"Archivo {csv_file} encontrado. No se ejecutará {function_name}().")




#######################################ELASTICSEARCH########################################################

def insercionElastic():
    #Conexión a Elasticsearch
    client = Elasticsearch(
        "https://host.docker.internal:9200",
        basic_auth=("elastic", "azTr0bwgJQHkA0sPVI1L"),
        verify_certs=False
    )
    
    # Ruta de la carpeta donde están los archivos CSV
    carpeta_datos = os.getcwd()
    print(carpeta_datos)
    print("Directorio actual:", os.getcwd())

    # Obtiene la lista de archivos CSV en la carpeta y almacénalos en un DataFrame
    archivos_csv = [f for f in os.listdir(carpeta_datos) if f.endswith(".csv")]
    df_archivos = pd.DataFrame(archivos_csv, columns=["nombre_archivo"])

    # Itera sobre cada archivo CSV
    for _, row in df_archivos.iterrows():
        nombre_archivo = row["nombre_archivo"]

        # Usa el nombre del archivo sin extensión como nombre del índice
        indice_name = os.path.splitext(nombre_archivo)[0]

        # Crea el índice si no existe
        if not client.indices.exists(index=indice_name):
            client.indices.create(index=indice_name)
            print(f"Índice {indice_name} creado.")

        # Lee el archivo CSV
        ruta_csv = os.path.join(carpeta_datos, nombre_archivo)
        df = pd.read_csv(ruta_csv)
        
        # Obtiene el número de registros del DataFrame
        num_registros_df = len(df)

        # Consulta Elasticsearch para obtener el número de documentos en el índice
        res = client.count(index=indice_name)
        num_registros_es = res['count']

        print(f"Registros en DataFrame: {num_registros_df}, Registros en Elasticsearch: {num_registros_es}")

        # Si el número de registros es el mismo, no inserta nada
        if num_registros_df == num_registros_es:
            print(f"No se insertaron datos, ya están todos los registros en {indice_name}.")
        else:
            # Prepara los documentos para la inserción en bloque
            actions = [
                {
                    "_index": indice_name,  # Usa el índice basado en el nombre del archivo
                    "_id": f"{indice_name}_{i}",  # Crea un ID único basado en el índice y la fila
                    "_source": row_data.to_dict()  # Inserta la fila completa como documento
                }
                for i, row_data in df.iterrows()
            ]
        
            # Inserta los documentos en Elasticsearch usando bulk
            helpers.bulk(client, actions)
            print(f"Inserción completada para los datos de {indice_name}.")

    carpeta = '/app/csv_clima'  # Carpeta donde están los archivos CSV

    
    # Nombre del índice en Elasticsearch
    indice_name = "clima_2013_2024"

    # Crea el índice si no existe
    if not client.indices.exists(index=indice_name):
        client.indices.create(index=indice_name)
        print(f"Índice {indice_name} creado.")

    # Lista de rangos de archivos a procesar
    ranges = [
        (1, 7),  # clima_1.csv a clima_6.csv
        (7, 11),  # clima_7_1.csv a clima_7_10.csv (pero 7 a 10)
        (8, 16)  # clima_8.csv a clima_15.csv
    ]

    # Itera sobre los rangos definidos
    for start, end in ranges:
        if start == 7:
            comienzo = 1
        else:
            comienzo = start
        for i in range(comienzo, end):
            if start == 7 and i <=10:  # Para clima_7_1 hasta clima_7_10
                archivo = f'clima_7_{i}.csv'  # Ajuste para clima_7_1, clima_7_2, ..., clima_7_10
            else:
                archivo = f'clima_{i}.csv'  # Resto de los archivos

            ruta_completa = os.path.join(carpeta, archivo)  # Combina la carpeta con el archivo

            try:
                # Leer el CSV en un DataFrame
                df = pd.read_csv(ruta_completa)
                
                # Prepara los documentos para la inserción en Elasticsearch
                actions = [
                    {
                        "_index": indice_name,
                        "_id": f"{indice_name}_{i}_{index}",  # Crear ID único basado en el archivo y el índice de la fila
                        "_source": row_data.to_dict()  # Convertir la fila en un diccionario
                    }
                    for index, row_data in df.iterrows()
                ]
                
                # Inserta los documentos en Elasticsearch usando bulk
                helpers.bulk(client, actions)
                print(f"Inserción completada para {archivo}.")
            
            except FileNotFoundError:
                print(f"El archivo {archivo} no se encontró.")

#####################################MAIN#########################################
if __name__ == "__main__":
    lecturaDatosAPI()
    insercionElastic()


    