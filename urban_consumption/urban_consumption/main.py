#!/usr/bin/env python

"""Entry point for the project urban consumption"""
import logging
import os
import urban_consumption.functions as f

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    logging.info("Starting urban_consumption application")
    spark = f.init_spark()
    co2_and_oil = f.read_csv(spark, os.path.abspath("data/CO2_and_Oil.csv"))
    meat_and_egg = f.read_csv(
        spark, os.path.abspath("data/Meat_and_Egg_Production.csv")
    )
    urban_gdp = f.read_csv(
        spark, os.path.abspath("data/Urbanization_GDP_and_Population.csv")
    )
    # Keep only data between 2008 and 2012
    urban_gdp = f.filter_dates(urban_gdp)
    co2_and_oil = f.filter_dates(co2_and_oil)
    meat_and_egg = f.filter_dates(meat_and_egg)

    # Merge Datasets
    data_df = f.merge_dataframes(co2_and_oil, meat_and_egg, urban_gdp)
    data_df.show()

    # Convert columns from total to per capita
    cols_list = [
        "Oil production (Etemad & Luciana) (terawatt-hours)",
        "Food Balance Sheets: Eggs - Production (FAO (2017)) (tonnes)",
        "meat_prod_tonnes",
    ]
    data_df_converted = f.convert_to_per_capita(data_df, cols_list)
