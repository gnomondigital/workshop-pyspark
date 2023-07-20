import pytest
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark_test import assert_pyspark_df_equal
import urban_consumption.functions as f


def test_filter_dates(test_data: DataFrame):
    """Check that data was filtered according to the required range"""
    result = f.filter_dates(test_data)
    result = result.filter((F.col("Year") < 2008) | (F.col("Year") > 2012))
    assert result.count() == 0


def test_convert_to_per_capita(init_spark, test_data: DataFrame):
    """Check that data was filtered according to the required range"""
    data = [
        (0.1,),
        (0.2,),
        (0.1,),
        (0.1,),
        (0.2,),
        (0.1,),
    ]
    expected = init_spark.createDataFrame(
        data,
        ["col_1_per_capita"],
    )
    result = f.convert_to_per_capita(test_data, ["col_1"])
    assert "col_1_per_capita" in expected.columns
    assert_pyspark_df_equal(expected, result.select("col_1_per_capita"))


def test_country_with_highest_avg_meat(test_data: DataFrame):
    """Test the country_with_highest_avg_meat function"""
    avg_expected = test_data.groupBy("Entity").agg(
        F.mean("meat_prod_tonnes_per_capita").alias("meat_prod_tonnes_per_capita")
    )
    avg_expected.show()
    expected_count = f.country_with_highest_avg_meat(avg_expected)
    assert expected_count == 1
