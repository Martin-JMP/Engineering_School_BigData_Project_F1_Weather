## Engineering School ISEP Project 

# Advanced Databases and Big-Data
This project, submitted on June 15, 2024, by Martin JONCOURT and Lucas SAYAG, focuses on integrating and analyzing Formula 1 Grand Prix race data with corresponding weather data. The project utilizes multiple APIs to gather data, processes it using Apache Spark, and visualizes it on a Kibana dashboard.

## Project Members
- Martin JONCOURT
- Lucas SAYAG

## API Information
### Formula 1 Grand Prix Race Result API
- **Provider:** Ergast API
- **Data:** Race results, driver standings, circuit details, constructor standings.
- **Update Frequency:** After every F1 Grand Prix
- **Data Format:** JSON
- **Example Request:** `http://ergast.com/api/f1/2024/1/results`

### Weather Data API
- **Provider:** Meteostat
- **Data:** Average, minimum, maximum temperatures, wind speed, precipitation, snow.
- **Update Frequency:** Every minute
- **Data Format:** CSV
- **Example Request:** `https://meteostat.p.rapidapi.com/point/daily?lat=-37.8497&lon=144.968&start=2024-03-16&end=2024-03-24`

## Data Processing
### Formula 1 Grand Prix Race Result API
- **Time Formatting:** Display race times in minutes, seconds, milliseconds.
- **New Variables:**
  - Points earned
  - Cumulative points
- **Data Format:** Parquet

### Weather Data API
- **New Variables:**
  - City
  - Country
- **Data Format:** Parquet

### Data Combination
- **Combination Method:** Inner join on city, country, and date using Spark DataFrames.
- **Purpose:** Enrich Formula 1 data with contextual meteorological information.
- **Output Format:** Parquet

## Analysis and Usage
### Analysis Tasks
- Wins per driver, per year, per city
- Fastest laps per race
- Weather data analysis by year, city, driver
- Evolution of driver points
- Pit stop analysis
- **Output Format:** Parquet files for each analysis

### Usage
- Data is visualized on a Kibana dashboard.

## DAG Structure
- The Airflow DAG `project_dag` orchestrates the data fetching, processing, and analysis tasks.

## Tasks
1. Fetch Raw Data
2. Process Data
3. Fetch Weather Data
4. Aggregate Data
5. Combine Data
6. Usage Data Analysis
7. Indexing Tasks

## DAG Graph
- Illustrates the organization of the DAG and the sequence of tasks.

## Data Lake Structure
- An organized collection of data files, structured for efficient querying and analysis.

## Indexes and Data Views
- Created individual data views in Kibana for each index to enhance data organization and usability, allowing efficient exploration and targeted analysis.

## Dashboard
- Provides comprehensive analysis of F1 drivers' performances and associated weather conditions at various Grand Prix events.

## Conclusion
The project successfully integrates and analyzes F1 race data with weather data, providing valuable insights into race performance and the impact of weather conditions. The results are visualized on a Kibana dashboard, facilitating informed decision-making and strategy planning for F1 teams and enthusiasts.
