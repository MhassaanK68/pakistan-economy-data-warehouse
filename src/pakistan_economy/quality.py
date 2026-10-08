"""Small validation primitives used by the full load."""

from pyspark.sql import DataFrame, functions as F


def split_valid_invalid(frame: DataFrame, required_columns: list[str]) -> tuple[DataFrame, DataFrame]:
    invalid_condition = F.lit(False)
    for column in required_columns:
        invalid_condition = invalid_condition | F.col(column).isNull()
    return frame.filter(~invalid_condition), frame.filter(invalid_condition)


def assert_unique(frame: DataFrame, key_columns: list[str]) -> None:
    duplicate = frame.groupBy(*key_columns).count().filter(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise ValueError(f"Incoming data is not unique on business key: {key_columns}")
