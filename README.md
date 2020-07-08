## Cummulative SARS-CoV-2 case counts aggregated at world bank region, country, state, US country levels and cruises. 

[Biothings](https://github.com/biothings/outbreak.api) parser for COVID-19 case counts and testing rates. Daily reports have been corrected in a fork [here](https://github.com/gkarthik/COVID-19) that is maintained.

### Running

[preprocess.py](preprocess.py) aggregates case count and testing data from multiple sources using shapefiles to standardize the data with iso3 and fips codes. A `config.ini` is required to run the script with paths to each of the data sources. An example of the config.ini file is given at [example.config.ini](./example.config.ini). This script will take daily report files from JHU CSSE COVID-19 data and aggregate daily cummulative casecounts at world bank region, country, state, US country levels and cruises (Grand Princess and Diamond Princess). It aggregates NYT COVID-19 data to get counts for the US at state and county levels. It adds testing results for US states from [covid trackin](http://covidtracking.com/).

Create a new virtual environment and install the requirements for running the preprocess script with `pip install -r requirements.txt`. [GDAL](https://gdal.org/download.html) is also required. Building from source is suggested in preference of using Homebrew, although Linux and Windows users may find their package manager capable of installing without issue.

#### Data Sources

##### Shapefiles:

1. [Natural Earth](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/): ne_10m_admin_0_countries
2. [Natural Earth](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/): ne_10m_admin_1_states_provinces
3. [2019 US County Shapefiles](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html): tl_2019_us_county
4. [US Census 2018 CBSA Shapefile](https://www2.census.gov/geo/tiger/GENZ2018/shp/cb_2018_us_cbsa_500k.zip)

##### Data:
1. [Country GDP per capita](https://data.worldbank.org/indicator/NY.GDP.PCAP.CD?most_recent_value_desc=true)
2. [US Census Sep 2018 metropolitan CBSA](https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2018/delineation-files/list1_Sep_2018.xls) Data from here is exported as csv to data/census_metropolitan_areas.csv with lines corresponding to the title and notes removed from the top and bottom of the file.

##### Repos
1. [JHU CSSE COVID-19 data](https://github.com/CSSEGISandData/COVID-19). A fork of this data with small corrections is available [here](https://github.com/gkarthik/COVID-19).
2. [NYT US State and County data](https://github.com/nytimes/covid-19-data)
