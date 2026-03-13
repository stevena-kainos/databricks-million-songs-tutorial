import pytest
from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
# # from pipeline.databricks-million-songs-tutorial.utilities.util import get_songs_raw_schema


# .utilities.util import get_songs_raw_schema

# from pipeline.utilities.util import (
#     get_songs_raw_schema,
#     # get_songs_prepared,
#     # get_top_artists_by_year
# )


@pytest.fixture
def spark():
    return SparkSession.builder.appName("test").getOrCreate()


# def test_get_songs_raw_schema():
#     schema = get_songs_raw_schema()
#     assert isinstance(schema, StructType)
#     assert len(schema.fields) == 20
#     assert schema["artist_id"].dataType == StringType()
#     assert schema["duration"].dataType == DoubleType()
#     assert schema["year"].dataType == IntegerType()


# def test_get_songs_prepared(spark):
#     data = [("id1", "Artist Name", 180.0, "Album", 120.0, 4.0,
#              "Song Title", 2020)]
#     df = spark.createDataFrame(data, ["artist_id", "artist_name",
#                                       "duration","release", "tempo",
#                                       "time_signature", "title", "year"])

#     result = get_songs_prepared(df)
#     assert "song_title" in result.columns
#     assert "title" not in result.columns
#     assert set(result.columns) == {"artist_id", "artist_name", "duration",
#                                    "release", "tempo", "time_signature",
#                                    "song_title", "year"}
#     assert result.count() == 1


# def test_get_top_artists_by_year(spark):
#     data = [
#         ("artist1", 2020),
#         ("artist1", 2020),
#         ("artist2", 2020),
#         ("artist1", 2019),
#     ]
#     df = spark.createDataFrame(data, ["artist_name", "year"])

#     result = get_top_artists_by_year(df)
#     rows = result.collect()
#     assert rows[0]["total_number_of_songs"] == 2
#     assert rows[0]["artist_name"] == "artist1"


# def test_get_top_artists_by_year_filters_invalid_years(spark):
#     data = [("artist1", 0), ("artist1", 2020), ("artist2", -1)]
#     df = spark.createDataFrame(data, ["artist_name", "year"])

#     result = get_top_artists_by_year(df)
#     assert result.count() == 1
