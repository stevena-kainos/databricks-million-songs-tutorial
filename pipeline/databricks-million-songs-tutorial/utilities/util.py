from pyspark.sql.functions import expr, desc
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructType,
    StructField
)


def get_songs_raw_schema():
    return (
        StructType(
            [
                StructField("artist_id", StringType(), True),
                StructField("artist_lat", DoubleType(), True),
                StructField("artist_long", DoubleType(), True),
                StructField("artist_location", StringType(), True),
                StructField("artist_name", StringType(), True),
                StructField("duration", DoubleType(), True),
                StructField("end_of_fade_in", DoubleType(), True),
                StructField("key", IntegerType(), True),
                StructField("key_confidence", DoubleType(), True),
                StructField("loudness", DoubleType(), True),
                StructField("release", StringType(), True),
                StructField("song_hotnes", DoubleType(), True),
                StructField("song_id", StringType(), True),
                StructField("start_of_fade_out", DoubleType(), True),
                StructField("tempo", DoubleType(), True),
                StructField("time_signature", DoubleType(), True),
                StructField("time_signature_confidence", DoubleType(), True),
                StructField("title", StringType(), True),
                StructField("year", IntegerType(), True),
                StructField("partial_sequence", IntegerType(), True)
            ]
        )
    )


def get_songs_prepared(songs_raw):
    return (
        songs_raw
        .withColumnRenamed("title", "song_title")
        .select("artist_id", "artist_name", "duration", "release", "tempo", "time_signature", "song_title", "year")
    )


def get_top_artists_by_year(songs_prepared_df):
    return (
        songs_prepared_df
        .filter(expr("year > 0"))
        .groupBy("artist_name", "year")
        .count().withColumnRenamed("count", "total_number_of_songs")
        .sort(desc("total_number_of_songs"), desc("year"))
    )