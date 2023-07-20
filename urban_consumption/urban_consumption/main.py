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
