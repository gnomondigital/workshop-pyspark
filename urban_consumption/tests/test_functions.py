import pytest
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import urban_consumption.functions as f


def test_filter_dates(test_data: DataFrame):
    """Check that data was filtered according to the required range"""
    result = f.filter_dates(test_data)
    result = result.filter((F.col("Year") < 2008) | (F.col("Year") > 2012))
    assert result.count() == 0
