# README #

This repository contains a mini pyspark project for interview purpose.
 

# Summary #

The project is mainly about looking at country-level factors that are related to the production of carbon dioxide (CO2). 
# Data #
The data in question has 2 files: 
1. CO2_and_Oil.csv: Includes annual information on CO2 emissions per-capita and oil production in terawatt-hours for each country. 
   that describes the data (Temperature, Holiday Flag, Fuel price, etc...)
2. Urbanization_GDP_and_Population.csv: For each country-year combination, includes the percentage of people who live in urban
   areas (vs. rural areas), the Gross Domestic Product (GDP) per-capita, and the country’s population that year.
3. Meat_and_Egg_Production: For each country-year combination, includes the meat and egg production in tonnes, 
   as well as the meat supply per person in kg.

# What the candidate should do #
1. Load the data into dataframes and join them into a single one.
2. Include only data from the year 2008 up to and including 2012.
3. Merge the information from the three datasets into a single dataset.
4. Some columns ( Oil production (Etemad & Luciana) (terawatt-hours), 
   meat_prod_tonnes, and Food Balance Sheets: Eggs - Production (FAO (2017)) (tonnes) ) 
   report country total data. Transform these three columns into per-capita data (divide by country's population.)
5. Several rows in the Entity ​​column have the value World. Remove these rows, since they don’t correspond to any particular country.
6. From the prepared dataset, create a new dataset that aggregates the values of the seven "descriptive" columns 
   (over the years 2008 - 2012) for each country. Each row in the new dataset should contain the average value of the 
   descriptive columns per country during this five-year period.
7. which country has the highest average meat production per capita for the years 2008-2012?
8. What country has the 5th highest CO2 emissions per capita for the year 2010 ?
9. Add a classification column to compare oil production in 2012 to the median and indicate
   if the country ahs above or below median production.