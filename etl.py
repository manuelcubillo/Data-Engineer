# %% [markdown]
# 
# # Data Engineering Capstone Project
# 
# #### Project Summary
# 
# In this project, we are going to build a complete ETL pipeline to process information from several sources. The goal is to merge, the information from the sources, process it, and create a data model that helps later to analyze this data. For that purpose, we are going to work with Spark, and the data is:
# 
# - Immigration: specific information about every people that entry to EEUU
# - Cities: demographic information about cities and states.
# - Temperature: global temperature around the world, has been measured monthly.
# 
# With these datasets, we are going to build the data model and ETL. The final purpose is to help analytical people understand why those travelers come in that moment of the year, taking into account the temperature from the origin country, and also the temperature at that moment in the United States. Besides, demographic information can be added to this analysis where we can take into account the age of the traveler, the median age of state that is visited, and also the total population or percentage male/female.
# 
# The project follows the following steps:
# * Step 1: Scope the Project and Gather Data
# * Step 2: Explore and Assess the Data
# * Step 3: Define the Data Model
# * Step 4: Run ETL to Model the Data
# * Step 5: Check data quality
# * Step 6: Complete Project Write Up

# %%
# imported libraries
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import glob
import re
import math
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, asc, monotonically_increasing_id, to_timestamp, lit
from pyspark.sql.types import StringType, IntegerType, IntegerType

# %% [markdown]
# ### Step 1: Scope the Project and Gather Data
# 
# Every dataset is going to be explained in the following points:

# %% [markdown]
# #### 1.1 Read immigration data using Spark. It reads all data from "sas_data" folder
# 
# This data set is name: I94 Immigration. This data comes from the US National Tourism and Trade Office and is basically records from international visitors to US. These records store information such as arrival and departure date, visa, mode, and personal information of the passenger as it could be gender, country and age. [Link to source](https://travel.trade.gov/research/reports/i94/historical/2016.html)
# 
# 
# In this first step we are going to load the raw data from the US National Tourism and Trade Office to a dataframe, and later on we will processed in our data model.

# %%

spark = SparkSession.builder.\
config("spark.jars.repositories", "https://repos.spark-packages.org/").\
config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
enableHiveSupport().getOrCreate()

i94_fname = "../../data/18-83510-I94-Data-2016/i94_apr16_sub.sas7bdat"
i94_files = glob.glob("../../data/18-83510-I94-Data-2016/*.sas7bdat")
#Only april month. This small dataset contais more than 3Million records
immigrationDF = spark.read.format("com.github.saurfang.sas.spark").load(i94_fname)


# To load the full dataset uncomment the following line
#immigrationDF = spark.read.parquet("sas_data")



# %%
immigrationDF.show(3)

# %%
#immigrationDF = immigrationDF.withColumnRenamed('_c0','immigration_id')
immigrationDF.printSchema()


# %% [markdown]
# #### 1.2 Read global land temperature csv. 
# This file contains the average temperature of each month and by city. [Link to source](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)

# %%
# read temperature data
temperature_fname = "../../data2/GlobalLandTemperaturesByCity.csv"
fnameTemperature = 'GlobalLandTemperaturesByCity.csv'
temperatureDF = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(temperature_fname)

# %%
temperatureDF.show(3)

# %% [markdown]
# #### 1.3 Read cities information csv. 
# This file contains demographic information about US cities. This demographic information has some fields as the race, gender majority, median age, etc. [Link to source](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/)

# %%
# read us-cities data
usCitiesPath = 'us-cities-demographics.csv'
citiesDF = spark.read.format("csv").option("delimiter", ";").option("header", "true").load(usCitiesPath)

# %% [markdown]
# #### 1.4 Read airport information csv. 
# This file contais airport details as airport code IATA, locations, and other relevant information of each airport. [Link to source](http://ourairports.com/data/airports.csv)
# 

# %%
# read airport-codes data
airportPath = 'airport-codes_csv.csv'
airportDF = spark.read.format("csv").option("delimiter", ",").option("header", "true").load(airportPath)

# %% [markdown]
# ### Step 2: Explore and Assess the Data
# #### Explore the Data 
# 1. Explore data
# 2. Filter null values of important fields for the future analysis
# 2. Select the proper columns for the data model
# 3. Process and transforming data

# %% [markdown]
# #### 2.1 Exploring and parsing Immigration data set

# %%
pd.set_option("display.max.columns", None)
immigrationDF.printSchema()

# %% [markdown]
# For our data model is going to be important the departure date, that match with the column "depdate". First of all we are going to filter by no-null values. 

# %%
immigrationDF = immigrationDF.dropna(subset=['depdate'])
immigrationDF = immigrationDF.dropna(subset=['arrdate'])

# %% [markdown]
# Now we are going to procced to filter the data. We do not need several columns. Firstable we are going to remove some columns not needed and afterwards rename the rest of the columns for a better understanding:

# %%
immigrationDF.show(3)

# %%
immigrationDF = immigrationDF.select(col("cicid").alias("immigration_id"),
                                    col("i94port").alias("city_id"),
                                    col("i94cit").alias("country_from"),
                                    col("arrdate").alias("arrival_date"),
                                    col("i94mode").alias("mode"),
                                    col("i94addr").alias("state_code"),
                                    col("depdate").alias("departure_date"),
                                    col("i94visa").alias("visa"),
                                    col("i94bir").alias("age"),
                                    col("gender").alias("gender"),
                                    col("biryear").alias("biryear"),
                                    col("airline").alias("airline"),
                                    col("fltno").alias("flight_number"))

print((immigrationDF.count(), len(immigrationDF.columns)))



# %% [markdown]
# After that we procced to format date values in columns:
# 
# - Arrival date
# - Departure date

# %%
#parse date
@udf(StringType())
def sas_date_parser(sas_date):
    """
    parse sas date to datetime format

    Args:
        sas_date ([type]): [description]

    Returns:
        datetime format
    """
    if sas_date:
        return (datetime(1960, 1, 1).date() + timedelta(sas_date)).isoformat()
    else:
        return None


# %%
immigrationDF = immigrationDF.withColumn("arrival_date", sas_date_parser(immigrationDF.arrival_date))
immigrationDF = immigrationDF.withColumn("departure_date", sas_date_parser(immigrationDF.departure_date))

# %%
immigrationDF.limit(5).toPandas()


# %% [markdown]
# Following step would be use the SAS description file to change the numbers of country code, visa a mode, by their categories
# 
# 1. Read SAS description file content

# %%
i94_sas_label_descriptions_fname = "I94_SAS_Labels_Descriptions.SAS"
with open(i94_sas_label_descriptions_fname) as f:
    lines = f.readlines()


# %% [markdown]
# 2. Format the column to be able to parse

# %%
@udf(IntegerType())
def parse_country(x):
    return math.trunc(x)

# %%

immigrationDF = immigrationDF.withColumn('country_from', parse_country(immigrationDF.country_from))


# %%
@udf(StringType())
def parse_sas_country_codes(x):
    """
    parse the numeric codes of immigration data source with the SAS description file.
    The string return is the name of the country, if there is not information "no info" is returned

    Args:
        x ([type]): [description]

    Returns:
        [type]: [description]
    """
    x=str(x)
    result = [(i) for i in lines[9:298] if i.__contains__(x)]

    # there are index that have not description on SAS file
    if len(result) != 1: 
        return "No info"
    
    result = re.search(' \'(.*?)\'', result[0])
    return result.group(1)

# %% [markdown]
# 3. Use the file to parse the codes

# %%
# parse country code
immigrationDF = immigrationDF.withColumn('country_from', parse_sas_country_codes(immigrationDF.country_from))


# %% [markdown]
# 3. Aswell for mode and visa. Implement funtion to parser the value

# %%
@udf(StringType())
def parse_sas_mode(data):
    """
    1 = 'Air'
	2 = 'Sea'
	3 = 'Land'
	9 = 'Not reported'


    Args:
        data ([type]): [description]
    """
    mode = {1:'Air', 2: 'Sea', 3: 'Land', 9 : 'not reported'}
    
    if(data in mode):
        return mode[data]
        
    return 'not supported'

# %%
immigrationDF = immigrationDF.withColumn('mode', parse_sas_mode(immigrationDF.mode))

# %%
@udf(StringType())
def parse_sas_visa(data):
    """
    1: business
    2: pleasure
    3: student

    Args:
        data ([type]): [description]
    """

    visa = {1 : 'Business', 2:'Pleasure', 3:'Student'}

    if(data in visa):
        return visa[data]
        
    return 'not supported'

# %%
immigrationDF = immigrationDF.withColumn('visa', parse_sas_visa(immigrationDF.visa))

# %%
immigrationDF.limit(5).toPandas()

# %% [markdown]
# #### 2.2 Exploring and parsing Temperature data set

# %% [markdown]
# #### Cleaning Steps
# Document steps necessary to clean the data

# %%
number_rows_before_clean = (temperatureDF.count())

# %% [markdown]
# There are certain data that we have to ensure we have due to its importance. In that case would be columns: AverageTemperature, dt, City. With a null value among these values, the row is not going to be usefull. So we ensure removing those rows.

# %%
temperatureDF = temperatureDF.dropna(subset=['AverageTemperature', 'dt', 'City', 'Country'],how='any')
number_rows_after_clean = (temperatureDF.count())
print('Rows removed',number_rows_before_clean-number_rows_after_clean)

# %% [markdown]
# Removed columns not needed: Latitude, Longitude, Country, AverageTemperatureUncertainty

# %%
temperatureDF = temperatureDF.drop('Latitude', 'Longitude', 'AverageTemperatureUncertainty')

# %% [markdown]
# Upper case country to match this value with immigradition country data

# %%
@udf(StringType())
def uppercase(x):
    return str(x).upper()

# %%
temperatureDF = temperatureDF.withColumn("Country", uppercase(temperatureDF.Country))


# %% [markdown]
# We are going to group all data from each country by date. With this change we are going to remove city field, and make an average temperature of all cities of the country for each date. The reason to do that is that in immigration data we do not have city_from information, but we have country_from, so later in analytics we can comapre temperature from original country and United Stated.

# %%
temperatureDF = temperatureDF.drop("City")

# %%
temperatureDF = temperatureDF.withColumn("AverageTemperature",col("AverageTemperature").cast("int"))
# group by country and date
temperatureDF = temperatureDF.groupby(['Country','dt']).mean('AverageTemperature')
temperatureDF = temperatureDF.orderBy(asc('Country'))

# %%
temperatureDF.limit(5).toPandas()

# %% [markdown]
# #### 2.3 Exploring and parsing Cities dataset

# %% [markdown]
# 1. Firstable check null values to clean up.

# %%
citiesDF = spark.read.format("csv").option("delimiter", ";").option("header", "true").load(usCitiesPath)

# %%
citiesDF.printSchema()

# %% [markdown]
# Select columns to remove

# %%
citiesDF = citiesDF.drop('Number of Veterans', 'Foreign-born', 'Average Household Size', 'Race', 'Count')


# %%
citiesDF = citiesDF.select(col('City').alias('City'),
                            col('State').alias('State'),
                            col('Median Age').alias('Median_age'),
                            col('Male Population').alias('Male_Population'),
                            col('Female Population').alias('Female_Population'),
                            col('Total Population').alias('Total_Population'),
                            col('State Code').alias('State_code'))

# %%
citiesDF.columns

# %% [markdown]
# Add some statistics about the gender population

# %%
@udf(StringType())
def get_proportion(a,b):
    """
    calcule the percentage between argument a and b
    it is used to know the percentage of male / female on the total population

    Returns:
        [int]: percentage
    """
    a = int(a or 0)
    b = int(b or 0)
    if not math.isnan(a) and not math.isnan(b):
        return (a)/(b)*100
    else:
        return 0

# %%
# convert types tu numeric
citiesDF = citiesDF.withColumn("Median_age",col("Median_age").cast("int"))
citiesDF = citiesDF.withColumn("Male_Population",col("Male_Population").cast("int"))
citiesDF = citiesDF.withColumn("Female_Population",col("Female_Population").cast("int"))
citiesDF = citiesDF.withColumn("Total_Population",col("Total_Population").cast("int"))


# %%
citiesDF = citiesDF.withColumn("Male_percentage", get_proportion(citiesDF['Male_Population'].cast("int"),citiesDF['Total_Population'].cast("int")))
citiesDF = citiesDF.withColumn("Female_percentage", get_proportion(citiesDF['Female_Population'],citiesDF['Total_Population']))

# %% [markdown]
# Uppercase state values

# %%
citiesDF = citiesDF.withColumn('State',uppercase(citiesDF.State))

# %%
citiesDF.printSchema()

# %% [markdown]
# In immigration data we do not have information about the city of destiny, but we have the state code, so we are going to work we that value, remove city name, and do the average of all date per state code.

# %%
citiesDF = citiesDF.drop('City')
citiesDF = citiesDF.withColumn("Male_percentage",col("Male_percentage").cast("int"))
citiesDF = citiesDF.withColumn("Female_percentage",col("Female_percentage").cast("int"))
citiesDF = citiesDF.groupby(['State', 'State_Code']).mean('Median_age', 'Male_Population', 'Female_Population', 'Total_Population', 'Male_percentage', 'Female_percentage')

# %%
citiesDF.printSchema()

# %%
citiesDF.show(5)

# %% [markdown]
# ### Step 3: Define the Data Model
# #### 3.1 Conceptual Data Model
# 

# %% [markdown]
# The final data model is build by 3 differents data sources: immigration, temperature and cities data.
# With all this data we can build the following data model:
# 
# ![dataModel](img/dataModel-FACT-MODEL1.png)
# 
# 
# This data model is made by 5 tables. This star schema is build from the fact table:
# - Immigration
# 
# 4 dimension tables:
# - City
# - Airline
# - Personal
# - Temperature
# 
# 
# This data model breakdown most part of immigration source data, for a easy understanding. Also the rest of sources are filtered, clean and transformed based on the immigration data. All these make a star schema, where it would be very easily analyze personal information, temperature where the traveler comes, but also demographic information of the state that visit the tourist.
# 

# %% [markdown]
# 
# #### 3.2 Mapping Out Data Pipelines
# List the steps necessary to pipeline the data into the chosen data model

# %% [markdown]
# 1. Load data from sources.
# 2. Analyze, and clean data from nulls values.
# 3. Transform some data types.
# 4. Remove not necessary data.
# 5. Create fact and dimesion tables.
# 6. Save tables in parquet for downstream query 
# 7. Check quality data.

# %% [markdown]
# ### Step 4: Run Pipelines to Model the Data 
# #### 4.1 Create the data model
# Build the data pipelines to create the data model.

# %% [markdown]
# 4.2 Create Personal dataframe

# %%
immigrationDF.columns

# %%
personal_table = immigrationDF.select(col('immigration_id'),col('gender'), col('age'), col('country_from'), col('biryear'))

# %% [markdown]
# 4.3 Create City dataframe

# %%
citiesDF.columns

# %%
cities_table = citiesDF.select(col('State'),col('avg(Median_age)').alias('Median_Age'), col('avg(Male_Population)').alias('Male_Population'), col('avg(Female_Population)').alias('Female_Population'),
       col('avg(Total_Population)').alias('Total_Population'), col('State_Code'), col('avg(Male_percentage)').alias('Male_percentage'), col('avg(Female_percentage)').alias('Female_percentage'))


# %% [markdown]
# 4.4 Create Airline dataframe

# %%
#trip_immigration_id, airline_name, flight_number
airline_table = immigrationDF.select(col('immigration_id'), col('airline'), col('flight_number'))

# %% [markdown]
# 4.5 Create Temperature dataframe

# %%
temperature_table = temperatureDF.select(col('dt'), col('avg(AverageTemperature)').alias('AverageTemperature'),  col('Country'))

# %% [markdown]
# Add a id for each row

# %%
temperature_table.columns

# %%
#temperature_table['temp_id'] = np.arange(temperature_table.shape[0]) 
temperatureDF = temperatureDF.withColumn("temp_id", monotonically_increasing_id())
temperature_table = temperature_table.withColumn("temp_id", monotonically_increasing_id())

# %%
temperature_table.columns

# %% [markdown]
# 4.6 Create Immigration dataframe

# %%
immigration_table = immigrationDF.select(col('immigration_id'), col('state_code'), col('arrival_date'), col('departure_date'), col('mode'), col('visa'))

# %% [markdown]
# Associate temp_id of temperature table, based on the country of turist, and the date of arrival to the country

# %%
immigration_table = immigration_table.withColumn('arrival_date', to_timestamp(immigration_table.arrival_date)) 
temperature_table = temperature_table.withColumn('dt', to_timestamp(temperature_table.dt)) 

# %%
def get_temp_id(immi_id, date):
    """
    return the temp id of the temperature id that match with the country of origin of the tourist and also the arrival date to United States

    Args:
        immi_id ([type]): [description]
        date ([type]): [description]

    Returns:
        [type]: [description]
    """
    country = personal_table[personal_table['immigration_id']==immi_id]['country_from']
    year = date.year
    month = date.month
    
    id = temperature_table['temp_id'][temperature_table['dt'].dt.year==year][temperature_table['dt'].dt.month==month][temperature_table['Country']==country]
    
    return str(id)

# %%
immigration_table = immigration_table.withColumn('tem_id', lit(get_temp_id(immigration_table['immigration_id'], immigration_table['arrival_date'])))


# %% [markdown]
# ### Write to parquet files

# %%
immigration_table.write.mode("overwrite").parquet("immigration")


# %%
personal_table.write.mode("overwrite").parquet("personal")


# %%
temperature_table.write.mode("overwrite").parquet("temperature")

# %%
cities_table.write.mode("overwrite").parquet("city")

# %% [markdown]
# #### 4.2 Data Quality Checks
# 

# %% [markdown]
# There is several data quality check, all of them are on a specific file call: Data_quality.ipynb.
# 
# 1. Check every table has been stored: this is usefull to know firstable if there was any error during ETL, and later to check the schema match with the data model designed.
# 2. Check there is any table empty: If the table is empty, it is very likely that an error could happen running the pipeline.
# 3. Check there is not a immigration_id duplicated: Once that we are sure that the tables are well build and fill. We can check if there is any primary key duplicated, very important check.
# 
# Refer to "data_quality" file to run this test

# %% [markdown]
# #### 4.3 Data dictionary 
# In the following tables are described the dimension and fact table. Also every field of the tables has a description explaining the meaning of it.

# %% [markdown]
# FACT TABLE: IMMIGRATION
# 
# | Data field | Description |
# | --- | --- |
# | immigration_id | Immigration identifier, it's the PRIMARY KEY for every entrance |
# | state_code | State code where the person is getting in |
# | arrival_date | Date of arrival of the trip |
# | departure_date | Date of departure of the trip |
# | mode | Mean the mode of entry to the country (air, sea, ground)|
# | visa | Mean the visa type, it could by business, pleasure, student|
# | temp_id | id related to temperature dimension table. It is realted to a row that show the average temperature of the country of origin |

# %% [markdown]
# DIMENSION TABLE: PERSONAL
# 
# | Data field | Description |
# | --- | --- |
# | immigration_id | identifier related to primary key of immigration table |
# | gender | Gender of the person. Male/female. |
# | age | Age at the moment of the entry |
# | country_from | Country of origin of the traveler |
# | biryear | Birth year of the traveler |
# 

# %% [markdown]
# DIMENSION TABLE: TEMPERATURE
# 
# | Data field | Description |
# | --- | --- |
# | temp_id | Unique ID, for each date and country. |
# | dt | Date time value of the moment of the temperature |
# | AverageTemperature | Average temperature of all cities of the country for a certain month |
# | Country | Country where the temperature has been measure |
# 
# 

# %% [markdown]
# DIMENSION TABLE: CITY
# 
# | Data field | Description |
# | --- | --- |
# | State | Name of the state to visit |
# | Meadian Age | Median age of the state |
# | Male Population | Number of men in the state |
# | Male Percentage | Percentage of men |
# | Female Percentage | Percentage of female |
# | Female Population | Number of female in the state  |
# | Total Population | Total population of the sate |
# | State Code | State code |
# 
# 
# 

# %% [markdown]
# DIMENSION TABLE: AIRLINE
# 
# | Data field | Description |
# | --- | --- |
# | immigration_id | Immigrant ID who takes the flight |
# | airline | Airline of flight service |
# | flight_number | Flight number of the airline |
# 

# %% [markdown]
# #### Step 5: Complete Project Write Up
# * Clearly state the rationale for the choice of tools and technologies for the project.
# 
# We have chosen top-notch technologies that work in the cloud. Specifically, we have used AWS, and several services of it, as it could be S3, and Amazon Redshift as a datawarehouse.
# Also, Spark is heavily used, always thinking in massive amount of data, with Spark we could execute code in parallel due to how its works and it is designed for big data.
# 
# * Propose how often the data should be updated and why.
# 
# The data should be updated periodically, but not all the tables are necessary to do with the same frequency. The fact table, 'immigrantion', should be updated at least one per week if we want to use this data for analytics and dashboard. However, other data as demographic information in table 'city' is not needed to do every week, we can update this table every 5 years (by example). Temperature data should be updated once per month. 
# 
# * Write a description of how you would approach the problem differently under the following scenarios:
# 
#  * The data was increased by 100x.
# 
#  If the data became incredibly big enough to need another service, we can move to AWS EMR, which is a big data processing service designed for large dataset.
# 
#  * The data populates a dashboard that must be updated on a daily basis by 7am every day.
# 
#  The best way to approach this is by using Apache Airflow and creating a daily scheduled.
# 
#  * The database needed to be accessed by 100+ people.
#  We can consider scale up the datawarehouse cloud service in order to being able to manage every request.
# 


