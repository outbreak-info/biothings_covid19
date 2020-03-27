## Cummulative SARS-CoV-2 case counts aggregated at world bank region, country, state, US country levels and cruises. 

[Biothings](https://github.com/biothings/outbreak.api) parser for [JHU CSSE COVID-19 data](https://github.com/CSSEGISandData/COVID-19). Daily reports have been corrected in a fork [here](https://github.com/gkarthik/COVID-19) that is maintained.

### Running

[preprocess.py](preprocess.py) uses 3 shapefiles that have to be put in the folder ./data/. This script will take daily report files present at `../outbreak_db/COVID-19/` and aggregate daily cummulative casecounts at world bank region, country, state, US country levels and cruises (Grand Princess and Diamond Princess).

Shapefiles:

1. [Natural Earth](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/): ne_10m_admin_0_countries
2. [Natural Earth](https://www.naturalearthdata.com/downloads/10m-cultural-vectors/): ne_10m_admin_1_states_provinces
3. [2019 US County Shapefiles](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html): tl_2019_us_county
