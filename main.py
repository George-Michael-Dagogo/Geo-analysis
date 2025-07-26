from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ReadGeonamesUSTabSeparatedFile") \
    .master("local[*]") \
    .getOrCreate()

# Define the path to your text file
# Ensure US.txt is in the correct directory.
file_path = "extracted/allCountries.txt" 

# Official GeoNames schema (as of their documentation)
# The file is tab-delimited and has 19 columns.
# It's always best to define a schema for performance and correctness.
schema = StructType([
    StructField("geonameid", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("asciiname", StringType(), True),
    StructField("alternatenames", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("feature_class", StringType(), True),
    StructField("feature_code", StringType(), True),
    StructField("country_code", StringType(), True),
    StructField("cc2", StringType(), True),
    StructField("admin1_code", StringType(), True),
    StructField("admin2_code", StringType(), True),
    StructField("admin3_code", StringType(), True),
    StructField("admin4_code", StringType(), True),
    StructField("population", LongType(), True),
    StructField("elevation", StringType(), True), # Elevation can be -9999, so reading as String or Integer with a custom parser is safer.
    StructField("dem", IntegerType(), True),
    StructField("timezone", StringType(), True),
    StructField("modification_date", StringType(), True) # Using StringType is safer as DateType requires a specific format string.
])

# Read the tab-separated file using the CSV reader
# We specify the delimiter and use the defined schema.
df = spark.read.csv(
    file_path,
    sep='\t',            
    header=False,        
    schema=schema
)

df.show(n=5, truncate=False, vertical=True)




###pandas code
import pandas as pd

# Define the path to the file
file_path = "extracted/allCountries.txt"

# Define column names based on the GeoNames schema
column_names = [
    "geonameid",
    "name",
    "asciiname",
    "alternatenames",
    "latitude",
    "longitude",
    "feature_class",
    "feature_code",
    "country_code",
    "cc2",
    "admin1_code",
    "admin2_code",
    "admin3_code",
    "admin4_code",
    "population",
    "elevation",
    "dem",
    "timezone",
    "modification_date"
]

# Read the tab-separated file into a Pandas DataFrame
df = pd.read_csv(
    file_path,
    sep='\t',
    header=None,
    names=column_names,
    dtype={
        "geonameid": "Int64",
        "name": str,
        "asciiname": str,
        "alternatenames": str,
        "latitude": float,
        "longitude": float,
        "feature_class": str,
        "feature_code": str,
        "country_code": str,
        "cc2": str,
        "admin1_code": str,
        "admin2_code": str,
        "admin3_code": str,
        "admin4_code": str,
        "population": "Int64",
        "elevation": str,  # Optional: consider parsing invalid/missing values
        "dem": "Int64",
        "timezone": str,
        "modification_date": str
    },
    low_memory=False
)

# Show basic info and first few rows
print("--- First 5 rows of allCountries.txt (GeoNames data) ---")
print(df.head())
print(f"\nTotal records: {len(df)}")

