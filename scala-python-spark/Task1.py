#Initilize a SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder \
    .master("local") \
    .appName("SP_6500_History") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#Load the SP 500 Historical Data
df = spark.read.csv('hdfs://localhost:8020/SP_500_Historical.csv', inferSchema=True, header= True)
#Display the Column names
df.columns
#Display the first row of the dataframe 
df.head()
#Check the schema of the dataframe
df.printSchema()
#Print the first 5 rows
for line in df.head(5):
    print(line,'\n')
#Print the general stats
df.describe().show()
#Format column to show just 2 decimal places
from pyspark.sql.functions import format_number
summary = df.describe()
summary.select(summary['summary'],format_number(summary['Open'].cast('float'),2).alias('Open'),summary['summary'],format_number(summary['High'].cast('float'),2).alias('High'),
               summary['summary'],format_number(summary['Low'].cast('float'),2).alias('Low'),summary['summary'],format_number(summary['Close'].cast('float'),2).alias('Close'),
               summary['summary'],format_number(summary['Volume'].cast('int'),0).alias('Volume')).show()

# Create a new dataframe with a column called HV Ratio that is the ratio of the High Price versus volume of stock traded for a day
df_hv = df.withColumn('HV Ratio',df['High']/df['Volume'])
df_hv.filter(col("Volume") > 0).show()

# Which day has the peak high in price?
df.orderBy(df [ 'High'].desc()).select(['Date']).head (1) [0] [ 'Date']

# What is the mean of the "Close" column
df.orderBy(df [ 'High'].desc()).select(['Date']).head (1) [0] [ 'Date']

# What is the mean of the "Close" column
from pyspark.sql.functions import mean
df.select (mean ('Close')).show()

# What is the maximum and minimum value of the "Volumn" column
from pyspark.sql.functions import min, max
df.select(max ('Volume'), min('Volume')).show()

# How many days did the stocks close lower than 60 dollars?
df.filter (df [ 'Close'] < 60).count()

## What percentage of the time was the "High" greater than 80 dollars
df.filter('High > 80').count() * 100/df.count()

# What is the Pearson correlation between "High" and "Volume"
df.corr('High', 'Volume')

# What is the max "High" per year?
from pyspark.sql.functions import (dayofmonth, hour,dayofyear, month, year, weekofyear, format_number, date_format)
year_df = df.withColumn('Year', year (df['Date']))
year_df.groupBy('Year') .max()['Year', 'max(High)'].show()

# What is the average "Close" for each calender month?
#Create a new column Month from existing Date column
month_df = df.withColumn('Month', month (df[ 'Date']))
#Group by month and take average of all other columns
month_df = month_df.groupBy('Month').mean()
#Sort by month
month_df = month_df.orderBy('Month')
#Display only month and avg(Close), the desired columns
month_df [ 'Month', 'avg(Close)'].show()
