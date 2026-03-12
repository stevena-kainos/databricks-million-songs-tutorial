# Import modules
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from utilities.util import (
    get_songs_raw_schema,
    get_songs_prepared,
    get_top_artists_by_year
)

# Define the path to the source data
file_path = "/databricks-datasets/songs/data-001/"

# Create Spark session
spark = SparkSession.builder.getOrCreate()


# Ingest data into songs_raw
@dp.table(
    comment="Raw data from a subset of the Million Song Dataset; a \
        collection of features and metadata for contemporary music tracks."
)
def songs_raw():
    return (spark.readStream
            .format("cloudFiles")
            .schema(get_songs_raw_schema())
            .option("cloudFiles.format", "csv")
            .option("sep", "\t")
            .load(file_path))


# Define a materialized view that validates data and renames a column
@dp.materialized_view(
    comment="Million Song Dataset with data cleaned and prepared for analysis."
)
@dp.expect("valid_artist_name", "artist_name IS NOT NULL")
@dp.expect("valid_title", "song_title IS NOT NULL")
@dp.expect("valid_duration", "duration > 0")
def songs_prepared():
    return get_songs_prepared(spark.read.table("songs_raw"))


# Define a materialized view that has a filtered, aggregated, and
# sorted view of the data
@dp.materialized_view(
    comment="A table summarizing counts of songs released by the artists who \
        released the most songs each year."
)
def top_artists_by_year():
    return get_top_artists_by_year(spark.read.table("songs_prepared"))
