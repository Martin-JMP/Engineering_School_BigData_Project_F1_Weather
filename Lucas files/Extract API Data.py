import json
import http.client
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

findspark.init()

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
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)

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

            with open(f'{save_dir}/races_{year}_{round_number}.json', 'w') as file:
                json.dump(race, file)

            results_url = f'http://ergast.com/api/f1/{year}/{round_number}/results.json'
            pitstops_url = f'http://ergast.com/api/f1/{year}/{round_number}/pitstops.json'

            results_response = requests.get(results_url)
            pitstops_response = requests.get(pitstops_url)

            results_data = results_response.json()
            pitstops_data = pitstops_response.json()

            with open(f'{save_dir}/results_{year}_{round_number}.json', 'w') as results_file:
                json.dump(results_data, results_file)

            with open(f'{save_dir}/pitstops_{year}_{round_number}.json', 'w') as pitstops_file:
                json.dump(pitstops_data, pitstops_file)

def process_and_save_all_data(raw_dir, formatted_dir, years):
    spark = SparkSession.builder \
        .appName("F1 Data Processing") \
        .getOrCreate()

    all_data = []

    for year in years:
        points_cumulative = {}
        races_files = [f for f in os.listdir(raw_dir) if f.startswith(f'races_{year}') and f.endswith('.json')]
        races_files.sort(key=lambda x: int(x.split('_')[2].split('.')[0]))  # Tri par numéro de course

        for filename in races_files:
            with open(f'{raw_dir}/{filename}', 'r') as file:
                race_data = json.load(file)

            round_number = race_data['round']
            with open(f'{raw_dir}/results_{year}_{round_number}.json', 'r') as results_file:
                results_data = json.load(results_file)

            with open(f'{raw_dir}/pitstops_{year}_{round_number}.json', 'r') as pitstops_file:
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
    df.coalesce(1).write.mode("overwrite").parquet(f'{formatted_dir}/formatted_ergastF1_data.parquet')




 


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



# Weather data retrieval and saving functions
api_key = "ac34867022msh32b09ce738739a0p18cb6ajsn451864220b04"
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

    if not os.path.exists("/home/lucassayag/datalake/raw/meteostat"):
        os.makedirs("/home/lucassayag/datalake/raw/meteostat")

    try:
        json_data = json.loads(data.decode("utf-8"))
        file_path = os.path.join("/home/lucassayag/datalake/raw/meteostat", f"METEO2_data_{city_name}.csv")

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
        file_path = os.path.join("/home/lucassayag/datalake/raw/meteostat", f"METEO2_data_{city_name}.csv")
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            df = df.withColumn("country", lit(country_name)).withColumn("city", lit(city_name))

            weather_data.append(df)

    if weather_data:
        combined_df = weather_data[0]
        for df in weather_data[1:]:
            combined_df = combined_df.union(df)
        combined_df.coalesce(1).write.mode("overwrite").parquet('/home/lucassayag/datalake/formatted/meteostat/METEO2_data_aggregated.parquet')

def convert_csv_to_parquet(source_filepath, dest_filepath):
    spark = SparkSession.builder \
        .appName("CSV to Parquet Conversion") \
        .getOrCreate()

    df = spark.read.csv(source_filepath, header=True, inferSchema=True)
    df.coalesce(1).write.parquet(dest_filepath)
    print(f"Converted {source_filepath} to Parquet format at {dest_filepath}")

def combine_parquet_files(f1_data_path, weather_data_path, combined_data_path):
    spark = SparkSession.builder \
        .appName("Combine Parquet Files") \
        .getOrCreate()

    f1_df = spark.read.parquet(f1_data_path)
    weather_df = spark.read.parquet(weather_data_path)

    combined_df = f1_df.join(weather_df, on=['city', 'country', 'date'], how='inner')
    combined_df.coalesce(1).write.mode("overwrite").parquet(combined_data_path)
    print(f"Combined data saved at: {combined_data_path}")

with DAG('combined_data_fetchh',
         default_args=default_args,
         description='Fetch and process F1 and weather data, including location details',
         schedule_interval='@monthly',
         catchup=False) as dag:

    fetch_raw_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
        op_kwargs={'years': [2022, 2023, 2024], 'save_dir': '/home/lucassayag/datalake/raw/ergastF1'}
    )

    process_data_task = PythonOperator(
        task_id='process_and_save_all_data',
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

    aggregate_task = PythonOperator(
        task_id='aggregate_data',
        python_callable=aggregate_data,
        dag=dag,
    )

    combine_parquet = PythonOperator(
        task_id='combine_parquet',
        python_callable=combine_parquet_files,
        op_kwargs={
            'f1_data_path': '/home/lucassayag/datalake/formatted/ergastF1/formatted_ergastF1_data.parquet',
            'weather_data_path': '/home/lucassayag/datalake/formatted/meteostat/METEO2_data_aggregated.parquet',
            'combined_data_path': '/home/lucassayag/datalake/combined/combined_data.parquet'
        },
        dag=dag,
    )

    fetch_raw_data_task >> process_data_task
    process_data_task >> fetch_data_tasks
    for task in fetch_data_tasks:
        task >> aggregate_task
    aggregate_task >> combine_parquet

