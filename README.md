# Weather Enhanced MN Phenology Dataset
Per the Minnesota Phenology Network, "Phenology is the study of recurring
events in the life cycle of plants
and animals, many of which are closely tied to patterns of climate and
seasonality." By pairing this data with historical weather data to create
a set of analytics tables, we may
be able to see a fuller picture of the changing cycles of plants and
animals.

While this data set is merely a million rows or so of data, it has
the potential to expand greatly. As such, I used Spark for its ability to
distribute and handle big data. I used Jupyter notebook for the ease
of seeing the data transformations along the way and since this was
a single extraction, as opposed to an ongoing one. In terms of updating the
data, the NOAA climate source data updates monthly, so there would also be 
a reasonable update frequency for this dataset.

## Process
The process for preparing each table for analysis is below.
More detail is available in the actual `etl.ipynb` file.
- Read in data from csv or json source to a pyspark data frame.
- After reviewing data sets, perform appropriate cleanup.
- Once extraction and cleanup have been performed, analytic tables
  are created, ensuring proper data types
- Finally, data is written out to parquet files

## Data Dictionary
Chosen using a star scheme to simplify querying. See 'MNPheno_ERD.drawio.png'
for ERD.
### observations (fact table)
| Field Name | Data Type | Description             | Example      |
|------------|-----------|-------------------------|--------------|
| obsv_id    | int       | Observation ID (PK)     | 4            |
| date       | date      | Year and month of event | 1960-07-10   |
| species    | text      | Species observed        | ARGOPHYLLUM  |
| county_code| text      | County as FIPS code     | 27053        |
| clim_id    | int       | Climate ID              | 1254130450601|
| event      | text      | Observation event       | FLOWERING    |
### biological
| Field Name  | Data Type | Description                             | Example         |
|-------------|-----------|-----------------------------------------|-----------------|
| species     | text      | Scientific species name                 | FULVA           |
| common_name | text      | Common name of species                  | ORANGE DAY-LILY |
| genus       | text      | Genus of species                        | HEMEROCALLIS    |
| lifeform    | text      | Type of lifeform                        | PLANTS          |
| group       | text      | Group of lifeform                       | FORB            |
| mn_invasive | text      | Indicates invasive species in Minnesota | None            |
### county
| Field Name | Data Type | Description    | Example |
|------------|-----------|----------------|---------|
| county     | text      | Name of county | Aitkin  |
| county_code| text      | FIPS code      | 27001   |
| state      | text      | Sate of county | MN      |
### climate
| Field Name | Data Type | Description                                | Example      |
|------------|-----------|--------------------------------------------|--------------|
| clim_id    | int       | Climate ID (PK                             | 309237645512 |
| county_code| text      | FIPS code for county                       | 27053        |
| year       | integer   | Year                                       | 1982         |
| month      | integer   | Month                                      | 1            |
| pcpn       | double    | Total precipitation in inches              | 1.7          |
| tmpMin     | double    | Minimum temperature in degrees Fahrenheit  | -9.6         |
| tmpMax     | double    | Maximum temperature in degrees Fahrenheit  | 9.7          |
| tmpAvg     | double    | Average temperature in degrees Fahrenheit  | 0.1          |
### time
| Field Name | Data Type | Description              | Example    |
|------------|-----------|--------------------------|------------|
| date       | date      | Date as year, month, day | 2005-06-06 |
| day        | integer   | Day of the month         | 6          |
| week       | integer   | Week of the year         | 23         |
| month      | integer   | Month of the year        | 6          |
| year       | integer   | Year                     | 2005       |
| weekday    | text      | Day of the week          | Mon        |
## Datasets

- Minnesota Phenology Network. Data accessed from the MnPN. Available:
  http://mnpn.usanpn.org. Accessed: 11/15/2021.
- NOAA Climate Data: ftp://ftp.ncdc.noaa.gov/pub/data/cirs/climdiv/
- County FIPS Codes (converted to json with https://csvjson.com/csv2json): https://www.nrcs.usda.gov/wps/portal/nrcs/detail/national/home/?cid=nrcs143_013697

## Potential Future Concerns

- The data was increased by 100x.
    - A significant increase would mean utilizing Spark to perform parallel processing, and data
  wouldn't be stored locally. Using a combination of AWS EMR and S3 could do the job.
    - This would also likely mean we've expanded the data beyond Minnesota.
      As such, the conversion of the FIPS codes would need to be done with a
      crosswalk instead of a hard coded conversion. Additionally, the "invasive"
      field requires more subtlety.
- The pipelines would be run on a daily basis by 7 am every day.
    - Airflow or another scheduler could be used.
- The database needed to be accessed by 100+ people.
    - The tables could be loaded onto a cloud system such as AWS, specifically using
  Redshift, as opposed to being accessed in Jupyter notebook.

## Acknowledgements

We thank the Minnesota Phenology Network for supplying data. We also thank
all of the volunteer participants who gathered data for the project.