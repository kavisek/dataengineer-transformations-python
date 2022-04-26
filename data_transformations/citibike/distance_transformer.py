from math import radians, cos, sin, asin, sqrt
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType


METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


def haversine_distance(start_long: float, start_lat: float, end_long: float, 
    end_lat: float) -> float:
    """Haversine Distance Formula (Arcsine Implementation)."""
    start_lat, start_long, end_lat, end_long = map(radians,[start_lat,start_long,end_lat,end_long])
    delta_lat = end_lat - start_lat
    delta_long = end_long - start_long

    # Compute central angle.
    central_angle = (sin(delta_lat / 2) ** 2 + cos(start_lat) * cos(end_lat) * sin(delta_long / 2) ** 2)
    central_angle = 2 * asin(sqrt(central_angle))

    # Compute distance
    distance = (central_angle * EARTH_RADIUS_IN_METERS) / METERS_PER_MILE
    distance = abs(round(distance, 2))
    return distance

def compute_distance(_spark: SparkSession, dataframe: DataFrame) -> DataFrame:
    """Adding a distance colunn to DataFrame."""

    # FIX: Cast StringType Datatype to DoubleTypes.
    double_cols = [
        'start_station_longitude',
        'start_station_latitude',
        'end_station_longitude',
        'end_station_latitude',
    ]
    for col in double_cols:
        dataframe = dataframe.withColumn(col, dataframe[col].cast(DoubleType()))

    # Create UDF
    udf_get_distance = udf(haversine_distance)

    # Create Distance Column
    dataframe = dataframe.withColumn(
        "distance", udf_get_distance(
            'start_station_longitude',
            'start_station_latitude',
            'end_station_longitude',
            'end_station_latitude'
            ).cast("Double")
        )

    return dataframe


def run(spark: SparkSession, input_dataset_path: str, transformed_dataset_path: str) -> None:
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')
