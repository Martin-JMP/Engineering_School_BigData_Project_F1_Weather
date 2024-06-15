import json
import http.client
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
import findspark
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
import pandas as pd
import subprocess
from elasticsearch import Elasticsearch, helpers, ConnectionError
import time
import logging
import pandas as pd
from elasticsearch import Elasticsearch, helpers
import urllib3
import numpy as np

findspark.init()
current_date = datetime.now().strftime('%Y%m%d')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2023, 1, 1)
}

def parse_time(time_str):
    if time_str == "N/A":
        return None

    if time_str.startswith('+'):
        time_parts = time_str[1:].split(':')
        if len(time_parts) == 2:
            m, s = time_parts
            return float(m) * 60 + float(s)
        else:
            return float(time_str[1:])

    time_parts = time_str.split(':')
    if len(time_parts) == 2:
        m, s = time_parts
        return float(m) * 60 + float(s)
    elif len(time_parts) == 3:
        h, m, s = time_parts
        return float(h) * 3600 + float(m) * 60 + float(s)

    raise ValueError(f"Unexpected time format: {time_str}")

def format_time(seconds):
    if seconds is None:
        return "N/A"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    seconds = seconds % 60

    return f"{hours:02}:{minutes:02}:{seconds:05.2f}"

def extract_data(years, save_dir):

    results_dir = f'{save_dir}/races/{current_date}'
    if not os.path.exists(results_dir):
        os.makedirs(results_dir)

    for year in years:
        races_url = f'http://ergast.com/api/f1/{year}.json'
        response = requests.get(races_url)
        races_data = response.json()

        for race in races_data['MRData']['RaceTable']['Races']:
            round_number = race['round']
            circuit_id = race['Circuit']['circuitId']

            circuit_info_url = f'http://ergast.com/api/f1/circuits/{circuit_id}.json'
            circuit_response = requests.get(circuit_info_url)
            circuit_data = circuit_response.json()
            city = circuit_data['MRData']['CircuitTable']['Circuits'][0]['Location']['locality']
            country = circuit_data['MRData']['CircuitTable']['Circuits'][0]['Location']['country']

            race['city'] = city
            race['country'] = country

            with open(f'{save_dir}/races/{current_date}/races_{year}_{round_number}.json', 'w') as file:
                json.dump(race, file)

            results_url = f'http://ergast.com/api/f1/{year}/{round_number}/results.json'
            pitstops_url = f'http://ergast.com/api/f1/{year}/{round_number}/pitstops.json'

            results_response = requests.get(results_url)
            pitstops_response = requests.get(pitstops_url)

            results_data = results_response.json()
            pitstops_data = pitstops_response.json()

            with open(f'{save_dir}/races/{current_date}/results_{year}_{round_number}.json', 'w') as results_file:
                json.dump(results_data, results_file)

            with open(f'{save_dir}/races/{current_date}/pitstops_{year}_{round_number}.json', 'w') as pitstops_file:
                json.dump(pitstops_data, pitstops_file)

def process_and_save_all_data(raw_dir, formatted_dir, years):
    spark = SparkSession.builder \
        .appName("F1 Data Processing") \
        .getOrCreate()

    all_data = []

    for year in years:
        points_cumulative = {}
        races_files = [f for f in os.listdir(f'{raw_dir}/races/{current_date}') if f.startswith(f'races_{year}') and f.endswith('.json')]
        races_files.sort(key=lambda x: int(x.split('_')[2].split('.')[0]))

        for filename in races_files:
            with open(f'{raw_dir}/races/{current_date}/{filename}', 'r') as file:
                race_data = json.load(file)

            round_number = race_data['round']
            with open(f'{raw_dir}/races/{current_date}/results_{year}_{round_number}.json', 'r') as results_file:
                results_data = json.load(results_file)

            with open(f'{raw_dir}/races/{current_date}/pitstops_{year}_{round_number}.json', 'r') as pitstops_file:
                pitstops_data = json.load(pitstops_file)

            races = results_data['MRData']['RaceTable']['Races']
            if races:
                results = races[0]['Results']
                pitstops = {}
                if 'Races' in pitstops_data['MRData']['RaceTable'] and pitstops_data['MRData']['RaceTable']['Races']:
                    pitstops_list = pitstops_data['MRData']['RaceTable']['Races'][0].get('PitStops', [])
                    for pit in pitstops_list:
                        driver_id = pit['driverId']
                        pitstops[driver_id] = pitstops.get(driver_id, 0) + 1

                first_time_seconds = None
                for result in results:
                    driver_id = result['Driver']['driverId']
                    driver_full_name = f"{result['Driver']['givenName']} {result['Driver']['familyName']}"
                    constructor_name = result['Constructor']['name']
                    points = float(result['points'])
                    points_cumulative[driver_id] = points_cumulative.get(driver_id, 0) + points
                    total_points = points_cumulative[driver_id]
                    position = result.get('position', 'N/A')
                    grid = result['grid']
                    laps = result['laps']
                    status = result['status']
                    time_data = result.get('Time', {}).get('time', 'N/A')
                    if time_data != 'N/A' and not time_data.startswith('+'):
                        if first_time_seconds is None:
                            first_time_seconds = parse_time(time_data)
                        formatted_time = format_time(parse_time(time_data))
                    elif time_data.startswith('+'):
                        if first_time_seconds is not None:
                            additional_seconds = parse_time(time_data)
                            total_seconds = first_time_seconds + additional_seconds
                            formatted_time = format_time(total_seconds)
                        else:
                            formatted_time = "N/A"
                    else:
                        formatted_time = "N/A"

                    fastest_lap_time = result.get('FastestLap', {}).get('Time', {}).get('time', 'N/A')

                    all_data.append({
                        'year': year,
                        'round': round_number,
                        'raceName': race_data['raceName'],
                        'date': race_data['date'],
                        'circuit': race_data['Circuit']['circuitName'],
                        'city': race_data['city'],
                        'country': race_data['country'],
                        'driverId': driver_id,
                        'driverFullName': driver_full_name,
                        'constructorName': constructor_name,
                        'points': points,
                        'totalPoints': total_points,
                        'position': position,
                        'grid': grid,
                        'laps': laps,
                        'status': status,
                        'time': formatted_time,
                        'fastestLapTime': fastest_lap_time,
                        'pitStops': pitstops.get(driver_id, 0)
                    })

    df = spark.createDataFrame(all_data)
    df.coalesce(1).write.mode("overwrite").parquet(f'{formatted_dir}/races/{current_date}/formatted_ergastF1_data.parquet')




station_ids = ["41150", "41020", "94866", "16147", "72202", "08181", "07695", "37864", "71612", "EGTC0", "11165", "07656", "12840", "06490", "06209", "16080", "48694", "47684", "72254", "76679", "83779", "41216", "KVGT0", "58367"]

cities = [
    "Sakhir", "Jeddah", "Melbourne", "Imola", "Miami", "Montmeló", "Monte-Carlo",
    "Baku", "Montreal", "Silverstone", "Spielberg", "Le Castellet", "Budapest",
    "Spa", "Zandvoort", "Monza", "Marina Bay", "Suzuka", "Austin", "Mexico City",
    "São Paulo", "Abu Dhabi", "Las Vegas", "Shanghai"
]

countries = [
    "Bahrain", "Saudi Arabia", "Australia", "Italy", "USA", "Spain", "Monaco",
    "Azerbaijan", "Canada", "UK", "Austria", "France", "Hungary", "Belgium",
    "Netherlands", "Italy", "Singapore", "Japan", "USA", "Mexico", "Brazil",
    "UAE", "USA", "China"
]


#infos api meteo
api_key = "7cf10d395fmsh9b3004ad9282967p11f004jsn940748744818"
api_host = "meteostat.p.rapidapi.com"
start_date = "2022-01-01"
end_date = datetime.now().strftime('%Y-%m-%d')


def fetch_data_for_station(station_id, city_name, country_name, **kwargs):
    conn = http.client.HTTPSConnection(api_host)
    headers = {
        'X-RapidAPI-Key': api_key,
        'X-RapidAPI-Host': api_host
    }
    endpoint = f"/stations/daily?station={station_id}&start={start_date}&end={end_date}"
    conn.request("GET", endpoint, headers=headers)
    res = conn.getresponse()
    data = res.read()
    conn.close()

    if not os.path.exists(f"/home/lucassayag/datalake/raw/meteostat/meteo/{current_date}"):
        os.makedirs(f"/home/lucassayag/datalake/raw/meteostat/meteo/{current_date}")

    try:
        json_data = json.loads(data.decode("utf-8"))
        file_path = os.path.join(f"/home/lucassayag/datalake/raw/meteostat/meteo/{current_date}", f"METEO2_data_{city_name}.csv")

        with open(file_path, mode='w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)
            header = json_data['data'][0].keys() if json_data['data'] else []
            csv_writer.writerow(header)
            for record in json_data['data']:
                csv_writer.writerow(record.values())

        print(f"Data fetched for {city_name} (station {station_id}) and saved at: {file_path}")
    except json.JSONDecodeError:
        print(f"Failed to fetch or decode data for {city_name} (station {station_id}). Response: {data}")

    return file_path

def aggregate_data(**kwargs):
    spark = SparkSession.builder \
        .appName("Weather Data Aggregation") \
        .getOrCreate()

    weather_data = []

    for city_name, country_name in zip(cities, countries):
        file_path = os.path.join(f"/home/lucassayag/datalake/raw/meteostat/meteo/{current_date}", f"METEO2_data_{city_name}.csv")
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            df = df.withColumn("country", lit(country_name)).withColumn("city", lit(city_name))

            weather_data.append(df)

    if weather_data:
        combined_df = weather_data[0]
        for df in weather_data[1:]:
            combined_df = combined_df.union(df)
        combined_df.coalesce(1).write.mode("overwrite").parquet(f'/home/lucassayag/datalake/formatted/meteostat/meteo/{current_date}/METEO2_data_aggregated.parquet')



def combine_parquet_files(f1_data_path, weather_data_path, combined_data_path):
    spark = SparkSession.builder \
        .appName("Combine Parquet Files") \
        .getOrCreate()

    f1_df = spark.read.parquet(f1_data_path)
    weather_df = spark.read.parquet(weather_data_path)

    combined_df = f1_df.join(weather_df, on=['city', 'country', 'date'], how='inner')
    combined_df.coalesce(1).write.mode("overwrite").parquet(combined_data_path)
    print(f"Combined data saved at: {combined_data_path}")


HOME = os.path.expanduser('~')
DATALAKE_ROOT_FOLDER = HOME + "/datalake/"




def usage_data_task(current_day):
    try:
        RACE_DATA_PATH = f'/home/lucassayag/datalake/formatted/meteostat_ergastF1/meteo_races/{current_date}/combined_data.parquet'
        WINS_OUTPUT_FOLDER = f"/home/lucassayag/datalake/usage/analysis_1/wins/{current_day}/"
        FASTEST_LAP_OUTPUT_FOLDER = f"/home/lucassayag/datalake/usage/analysis_2/fastestlap/{current_day}/"
        FILTER_OUTPUT_FOLDER = f"/home/lucassayag/datalake/usage/analysis_3/filter/{current_day}/"
        WEATHER_FOLDER = f"/home/lucassayag/datalake/usage/analysis_4/weather/{current_day}/"
        POINTS_FOLDER = f"/home/lucassayag/datalake/usage/analysis_5/pilotepoints/{current_day}/"
        CONSTRUCTOR_FOLDER = f"/home/lucassayag/datalake/usage/analysis_6/constructor/{current_day}/"
        PITSTOP_FOLDER = f"/home/lucassayag/datalake/usage/analysis_7/pitstop/{current_day}/"


        os.makedirs(WINS_OUTPUT_FOLDER, exist_ok=True)
        os.makedirs(FASTEST_LAP_OUTPUT_FOLDER, exist_ok=True)
        os.makedirs(FILTER_OUTPUT_FOLDER, exist_ok=True)
        os.makedirs(WEATHER_FOLDER, exist_ok=True)
        os.makedirs(POINTS_FOLDER, exist_ok=True)
        os.makedirs(CONSTRUCTOR_FOLDER, exist_ok=True)
        os.makedirs(PITSTOP_FOLDER, exist_ok=True)


        spark = SparkSession.builder.appName("CombineF1Data").getOrCreate()
        sqlContext = SQLContext(spark)

        df_races = sqlContext.read.parquet(RACE_DATA_PATH)
        df_races.createOrReplaceTempView("races")

        wins_df = sqlContext.sql("""
            SELECT driverFullName, year, city, COUNT(*) AS wins
            FROM races
            WHERE position = 1
            GROUP BY driverFullName, year, city
            ORDER BY driverFullName, year, city
        """)

        fastest_lap_df = sqlContext.sql("""
            SELECT year, circuit, city, driverFullName, fastestLapTime
            FROM (
                SELECT year, circuit, city, driverFullName, fastestLapTime,
                       ROW_NUMBER() OVER (PARTITION BY year, circuit ORDER BY fastestLapTime) as rn
                FROM races
                WHERE fastestLapTime IS NOT NULL AND fastestLapTime != 'N/A'
            ) t
            WHERE t.rn = 1
            ORDER BY year, circuit, city
        """)

        filter_df = sqlContext.sql("""
                    SELECT year, city, driverFullName
                    FROM races
                    GROUP BY driverFullName, year, city
                """)

        weather_df = sqlContext.sql("""
                    SELECT year, city, driverFullName,
                           MIN(tmin) as temp_min,
                           MAX(tmax) as temp_max,
                           AVG(tavg) as temp_avg,
                           AVG(prcp) as precipitation,
                           AVG(wspd) as wspd
                    FROM races
                    where year >=2023
                    GROUP BY year, city, driverFullName
                    ORDER BY year, city
                """)

        evo_points_pilote = sqlContext.sql("""
                   SELECT year, date, driverFullName, totalPoints, city
                   FROM races
                   ORDER BY year, date, driverFullName, totalPoints, city
               """)

        evo_points_constructor = sqlContext.sql("""
                           SELECT year, date, driverFullName,ConstructorName, totalPoints, city
                           FROM races
                           ORDER BY year, date,driverFullName, ConstructorName, totalPoints, city
                       """)

        evo_pitstops = sqlContext.sql("""
                                   SELECT year, date, driverFullName, pitStops, city
                                   FROM races
                                   ORDER BY year, date, ConstructorName, pitStops, city
                               """)


        wins_df.write.save(WINS_OUTPUT_FOLDER + "wins.parquet", mode="overwrite")
        fastest_lap_df.write.save(FASTEST_LAP_OUTPUT_FOLDER + "fastest_lap.parquet", mode="overwrite")
        filter_df.write.save(FASTEST_LAP_OUTPUT_FOLDER + "filter.parquet", mode="overwrite")
        weather_df.write.save(WEATHER_FOLDER + "weather_data.parquet", mode="overwrite")
        evo_points_pilote.write.save(POINTS_FOLDER + "pilote_points.parquet", mode="overwrite")
        evo_points_constructor.write.save(CONSTRUCTOR_FOLDER + "constructor_points.parquet", mode="overwrite")
        evo_pitstops.write.save(PITSTOP_FOLDER + "pitstop.parquet", mode="overwrite")


        spark.stop()

    except Exception as e:
        logging.error(f"Error processing data: {e}")
        raise



#index tasks
def index_wins():
    # Désactiver les avertissements de sécurité
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Lire le fichier Parquet
    file_path = r'D:\wins.parquet'
    df = pd.read_parquet(file_path)
    # Configurer Pandas pour afficher toutes les lignes et toutes les colonnes
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Afficher le DataFrame complet
    print(df)

    # Initialiser le client Elasticsearch avec l'authentification de base
    client = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", "*TrARIRX=2kRhxsT8Ui7"),  # Remplacez par vos informations d'identification
        verify_certs=False
    )

    # Fonction pour transformer un DataFrame Pandas en un format compréhensible par Elasticsearch
    def pandas_df_to_elasticsearch(df, index_name):
        records = df.to_dict(orient='records')
        actions = [
            {
                "_index": index_name.lower(),
                "_source": record
            }
            for record in records
        ]
        helpers.bulk(client, actions)

    # Nom de l'index Elasticsearch
    index_name = "index_wins"

    print("début de l'indéxation")

    # Indexer les données
    try:
        pandas_df_to_elasticsearch(df, index_name)
        print("Indexation réussie")
    except helpers.BulkIndexError as e:
        print(f"Erreur lors de l'indexation : {e.errors}")

    # Vérifier si l'index a été créé avec succès et afficher le nombre de documents indexés
    if client.indices.exists(index=index_name.lower()):
        print(f"L'index {index_name.lower()} existe.")
        count = client.count(index=index_name.lower())['count']
        print(f"Nombre de documents dans l'index {index_name.lower()}: {count}")
    else:
        print(f"L'index {index_name.lower()} n'existe pas.")

def index_fastestlap():
    # Désactiver les avertissements de sécurité
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Lire le fichier Parquet
    file_path = r'D:\fatestlap.parquet'
    df = pd.read_parquet(file_path)
    # Configurer Pandas pour afficher toutes les lignes et toutes les colonnes
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Afficher le DataFrame complet
    print(df)

    # Initialiser le client Elasticsearch avec l'authentification de base
    client = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", "*TrARIRX=2kRhxsT8Ui7"),  # Remplacez par vos informations d'identification
        verify_certs=False
    )

    # Fonction pour transformer un DataFrame Pandas en un format compréhensible par Elasticsearch
    def pandas_df_to_elasticsearch(df, index_name):
        records = df.to_dict(orient='records')
        actions = [
            {
                "_index": index_name.lower(),
                "_source": record
            }
            for record in records
        ]
        helpers.bulk(client, actions)

    # Nom de l'index Elasticsearch
    index_name = "index_fatestlap"

    print("début de l'indéxation")

    # Indexer les données
    try:
        pandas_df_to_elasticsearch(df, index_name)
        print("Indexation réussie")
    except helpers.BulkIndexError as e:
        print(f"Erreur lors de l'indexation : {e.errors}")

    # Vérifier si l'index a été créé avec succès et afficher le nombre de documents indexés
    if client.indices.exists(index=index_name.lower()):
        print(f"L'index {index_name.lower()} existe.")
        count = client.count(index=index_name.lower())['count']
        print(f"Nombre de documents dans l'index {index_name.lower()}: {count}")
    else:
        print(f"L'index {index_name.lower()} n'existe pas.")
def index_weather():
    # Désactiver les avertissements de sécurité
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Lire le fichier Parquet
    file_path = r'D:\weather2324.parquet'
    df = pd.read_parquet(file_path)
    # Configurer Pandas pour afficher toutes les lignes et toutes les colonnes
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Afficher le DataFrame complet
    print(df)

    # Initialiser le client Elasticsearch avec l'authentification de base
    client = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", "*TrARIRX=2kRhxsT8Ui7"),  # Remplacez par vos informations d'identification
        verify_certs=False
    )

    # Fonction pour transformer un DataFrame Pandas en un format compréhensible par Elasticsearch
    def pandas_df_to_elasticsearch(df, index_name):
        records = df.to_dict(orient='records')
        actions = [
            {
                "_index": index_name.lower(),
                "_source": record
            }
            for record in records
        ]
        helpers.bulk(client, actions)

    # Nom de l'index Elasticsearch
    index_name = "index_weather"

    print("début de l'indéxation")

    # Indexer les données
    try:
        pandas_df_to_elasticsearch(df, index_name)
        print("Indexation réussie")
    except helpers.BulkIndexError as e:
        print(f"Erreur lors de l'indexation : {e.errors}")

    # Vérifier si l'index a été créé avec succès et afficher le nombre de documents indexés
    if client.indices.exists(index=index_name.lower()):
        print(f"L'index {index_name.lower()} existe.")
        count = client.count(index=index_name.lower())['count']
        print(f"Nombre de documents dans l'index {index_name.lower()}: {count}")
    else:
        print(f"L'index {index_name.lower()} n'existe pas.")
def index_filter():
    # Désactiver les avertissements de sécurité
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Lire le fichier Parquet
    file_path = r'D:\filter.parquet'
    df = pd.read_parquet(file_path)
    # Configurer Pandas pour afficher toutes les lignes et toutes les colonnes
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Afficher le DataFrame complet
    print(df)

    # Initialiser le client Elasticsearch avec l'authentification de base
    client = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", "*TrARIRX=2kRhxsT8Ui7"),  # Remplacez par vos informations d'identification
        verify_certs=False
    )

    # Fonction pour transformer un DataFrame Pandas en un format compréhensible par Elasticsearch
    def pandas_df_to_elasticsearch(df, index_name):
        records = df.to_dict(orient='records')
        actions = [
            {
                "_index": index_name.lower(),
                "_source": record
            }
            for record in records
        ]
        helpers.bulk(client, actions)

    # Nom de l'index Elasticsearch
    index_name = "index_filter"

    print("début de l'indéxation")

    # Indexer les données
    try:
        pandas_df_to_elasticsearch(df, index_name)
        print("Indexation réussie")
    except helpers.BulkIndexError as e:
        print(f"Erreur lors de l'indexation : {e.errors}")

    # Vérifier si l'index a été créé avec succès et afficher le nombre de documents indexés
    if client.indices.exists(index=index_name.lower()):
        print(f"L'index {index_name.lower()} existe.")
        count = client.count(index=index_name.lower())['count']
        print(f"Nombre de documents dans l'index {index_name.lower()}: {count}")
    else:
        print(f"L'index {index_name.lower()} n'existe pas.")
def index_evopoints():
    # Désactiver les avertissements de sécurité
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Lire le fichier Parquet
    file_path = r'D:\evopoints.parquet'
    df = pd.read_parquet(file_path)
    # Configurer Pandas pour afficher toutes les lignes et toutes les colonnes
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Afficher le DataFrame complet
    print(df)

    # Initialiser le client Elasticsearch avec l'authentification de base
    client = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", "*TrARIRX=2kRhxsT8Ui7"),  # Remplacez par vos informations d'identification
        verify_certs=False
    )

    # Fonction pour transformer un DataFrame Pandas en un format compréhensible par Elasticsearch
    def pandas_df_to_elasticsearch(df, index_name):
        records = df.to_dict(orient='records')
        actions = [
            {
                "_index": index_name.lower(),
                "_source": record
            }
            for record in records
        ]
        helpers.bulk(client, actions)

    # Nom de l'index Elasticsearch
    index_name = "index_evopoints"

    print("début de l'indéxation")

    # Indexer les données
    try:
        pandas_df_to_elasticsearch(df, index_name)
        print("Indexation réussie")
    except helpers.BulkIndexError as e:
        print(f"Erreur lors de l'indexation : {e.errors}")

    # Vérifier si l'index a été créé avec succès et afficher le nombre de documents indexés
    if client.indices.exists(index=index_name.lower()):
        print(f"L'index {index_name.lower()} existe.")
        count = client.count(index=index_name.lower())['count']
        print(f"Nombre de documents dans l'index {index_name.lower()}: {count}")
    else:
        print(f"L'index {index_name.lower()} n'existe pas.")
def index_constructor():
    # Désactiver les avertissements de sécurité
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Lire le fichier Parquet
    file_path = r'D:\constructor.parquet'
    df = pd.read_parquet(file_path)
    # Configurer Pandas pour afficher toutes les lignes et toutes les colonnes
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Afficher le DataFrame complet
    print(df)

    # Initialiser le client Elasticsearch avec l'authentification de base
    client = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", "*TrARIRX=2kRhxsT8Ui7"),  # Remplacez par vos informations d'identification
        verify_certs=False
    )

    # Fonction pour transformer un DataFrame Pandas en un format compréhensible par Elasticsearch
    def pandas_df_to_elasticsearch(df, index_name):
        records = df.to_dict(orient='records')
        actions = [
            {
                "_index": index_name.lower(),
                "_source": record
            }
            for record in records
        ]
        helpers.bulk(client, actions)

    # Nom de l'index Elasticsearch
    index_name = "index_constructor"

    print("début de l'indéxation")

    # Indexer les données
    try:
        pandas_df_to_elasticsearch(df, index_name)
        print("Indexation réussie")
    except helpers.BulkIndexError as e:
        print(f"Erreur lors de l'indexation : {e.errors}")

    # Vérifier si l'index a été créé avec succès et afficher le nombre de documents indexés
    if client.indices.exists(index=index_name.lower()):
        print(f"L'index {index_name.lower()} existe.")
        count = client.count(index=index_name.lower())['count']
        print(f"Nombre de documents dans l'index {index_name.lower()}: {count}")
    else:
        print(f"L'index {index_name.lower()} n'existe pas.")
def index_pistop():
    # Désactiver les avertissements de sécurité
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # Lire le fichier Parquet
    file_path = r'D:\pitstop.parquet'
    df = pd.read_parquet(file_path)
    # Configurer Pandas pour afficher toutes les lignes et toutes les colonnes
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)

    # Afficher le DataFrame complet
    print(df)

    # Initialiser le client Elasticsearch avec l'authentification de base
    client = Elasticsearch(
        "https://localhost:9200",
        basic_auth=("elastic", "*TrARIRX=2kRhxsT8Ui7"),  # Remplacez par vos informations d'identification
        verify_certs=False
    )

    # Fonction pour transformer un DataFrame Pandas en un format compréhensible par Elasticsearch
    def pandas_df_to_elasticsearch(df, index_name):
        records = df.to_dict(orient='records')
        actions = [
            {
                "_index": index_name.lower(),
                "_source": record
            }
            for record in records
        ]
        helpers.bulk(client, actions)

    # Nom de l'index Elasticsearch
    index_name = "index_pitstop"

    print("début de l'indéxation")

    # Indexer les données
    try:
        pandas_df_to_elasticsearch(df, index_name)
        print("Indexation réussie")
    except helpers.BulkIndexError as e:
        print(f"Erreur lors de l'indexation : {e.errors}")

    # Vérifier si l'index a été créé avec succès et afficher le nombre de documents indexés
    if client.indices.exists(index=index_name.lower()):
        print(f"L'index {index_name.lower()} existe.")
        count = client.count(index=index_name.lower())['count']
        print(f"Nombre de documents dans l'index {index_name.lower()}: {count}")
    else:
        print(f"L'index {index_name.lower()} n'existe pas.")


with DAG('project_dag',
         default_args=default_args,
         description='Fetch, formats and combines F1 and weather data',
         schedule_interval='@monthly',
         catchup=False) as dag:

    fecth_f1_data = PythonOperator(
        task_id='fecth_f1_data',
        python_callable=extract_data,
        op_kwargs={'years': [2022, 2023, 2024], 'save_dir': '/home/lucassayag/datalake/raw/ergastF1'}
    )

    format_f1_data = PythonOperator(
        task_id='format_f1_data',
        python_callable=process_and_save_all_data,
        op_kwargs={'raw_dir': '/home/lucassayag/datalake/raw/ergastF1', 'formatted_dir': '/home/lucassayag/datalake/formatted/ergastF1', 'years': [2022, 2023, 2024]}
    )

    fetch_data_tasks = []
    for station_id, city_name, country_name in zip(station_ids, cities, countries):
        task = PythonOperator(
            task_id=f'fetch_data_for_{station_id}',
            python_callable=fetch_data_for_station,
            op_kwargs={'station_id': station_id, 'city_name': city_name, 'country_name': country_name},
            dag=dag,
        )
        fetch_data_tasks.append(task)

    format_weather_data = PythonOperator(
        task_id='format_weather_data',
        python_callable=aggregate_data,
        dag=dag,
    )

    combine_parquet = PythonOperator(
        task_id='combine_parquet',
        python_callable=combine_parquet_files,
        op_kwargs={
            'f1_data_path': f'/home/lucassayag/datalake/formatted/ergastF1/races/{current_date}/formatted_ergastF1_data.parquet',
            'weather_data_path': f'/home/lucassayag/datalake/formatted/meteostat/meteo/{current_date}/METEO2_data_aggregated.parquet',
            'combined_data_path': f'/home/lucassayag/datalake/formatted/meteostat_ergastF1/meteo_races/{current_date}/combined_data.parquet'
        },
        dag=dag,
    )

    usage_data_task = PythonOperator(
        task_id='usage_data_task',
        python_callable=usage_data_task,
        op_kwargs={'current_day': current_date},
    )



    index_wins_task = PythonOperator(
        task_id='index_wins_task',
        python_callable=index_wins,
    )
    index_fastestlap_task = PythonOperator(
        task_id='index_fastestlap_task',
        python_callable=index_fastestlap,
    )
    index_weather_task = PythonOperator(
        task_id='index_weather_task',
        python_callable=index_weather,
    )
    index_filter_task = PythonOperator(
        task_id='index_filter_task',
        python_callable=index_filter,
    )
    index_evopoints_task = PythonOperator(
        task_id='index_evopoints_task',
        python_callable=index_evopoints,
    )
    index_constructor_task = PythonOperator(
        task_id='index_constructor_task',
        python_callable=index_constructor,
    )
    index_pistop_task = PythonOperator(
        task_id='index_pistop_task',
        python_callable=index_pistop,
    )


    fecth_f1_data >> format_f1_data
    for task in fetch_data_tasks:
        task >> format_weather_data
    format_weather_data >> combine_parquet
    format_f1_data >> combine_parquet
    combine_parquet >> usage_data_task

    usage_data_task >> index_wins_task
    usage_data_task >> index_fastestlap_task
    usage_data_task >> index_weather_task
    usage_data_task >> index_filter_task
    usage_data_task >> index_evopoints_task
    usage_data_task >> index_constructor_task
    usage_data_task >> index_pistop_task

