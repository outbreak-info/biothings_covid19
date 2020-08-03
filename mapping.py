def get_customized_mapping(cls):
    mapping = {
        "date": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "cbsa": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "lat": {
            "type": "float"
        },
        "long": {
            "type": "float"
        },
        "location_id": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "admin_level": {
            "type": "float"
        },
        "sub_parts": {
            "properties": {
                "fips": {
                    "normalizer": "keyword_lowercase_normalizer",
                    "type": "keyword"
                },
                "state_name": {
                    "type": "keyword"
                },
                "county_name": {
                    "type": "keyword"
                }
            }
        },
        "confirmed_per_100k_breaks": {
            "type":"float"
        },
        "confirmed_rolling_14days_ago_diff_breaks": {
            "type":"float"
        },
        "confirmed_rolling_14days_ago_diff_per_100k_breaks": {
            "type":"float"
        },
        "confirmed_rolling_breaks": {
            "type":"float"
        },
        "confirmed_rolling_per_100k_breaks": {
            "type":"float"
        },
        "dead_per_100k_breaks": {
            "type":"float"
        },
        "dead_rolling_14days_ago_diff_breaks": {
            "type":"float"
        },
        "dead_rolling_14days_ago_diff_per_100k_breaks": {
            "type":"float"
        },
        "dead_rolling_breaks": {
            "type":"float"
        },
        "dead_rolling_per_100k_breaks": {
            "type":"float"
        },
        "mostRecent": {
            "type": "boolean"
        },
        "confirmed": {
            "type": "float"
        },
        "confirmed_rolling": {
            "type": "float"
        },
        "confirmed_rolling_14days_ago": {
            "type": "float"
        },
        "confirmed_rolling_14days_ago_diff": {
            "type": "float"
        },
        "confirmed_doublingRate": {
            "type": "float"
        },
        "confirmed_firstDate": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "confirmed_newToday": {
            "type": "boolean"
        },
        "confirmed_numIncrease": {
            "type": "float"
        },
        "confirmed_pctIncrease": {
            "type": "float"
        },
        "recovered": {
            "type": "float"
        },
        "recovered_rolling": {
            "type": "float"
        },
        "recovered_rolling_14days_ago": {
            "type": "float"
        },
        "recovered_rolling_14days_ago_diff": {
            "type": "float"
        },
        "recovered_firstDate": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "recovered_newToday": {
            "type": "boolean"
        },
        "recovered_numIncrease": {
            "type": "float"
        },
        "dead": {
            "type": "float"
        },
        "dead_rolling": {
            "type": "float"
        },
        "dead_rolling_14days_ago": {
            "type": "float"
        },
        "dead_rolling_14days_ago_diff": {
            "type": "float"
        },
        "dead_firstDate": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "dead_newToday": {
            "type": "boolean"
        },
        "dead_numIncrease": {
            "type": "float"
        },
        "first_dead-first_confirmed": {
            "type": "integer"
        },
        "dead_pctIncrease": {
            "type": "float"
        },
        "daysSince100Cases": {
            "type": "float"
        },
        "dead_doublingRate": {
            "type": "float"
        },
        "daysSince10Deaths": {
            "type": "float"
        },
        "daysSince50Deaths": {
            "type": "float"
        },
        "population": {
            "type": "integer"
        },
        "num_subnational": {
            "type": "integer"
        },
        "gdp_last_updated": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "gdp_per_capita": {
            "type": "float"
        },
        "recovered_pctIncrease": {
            "type": "float"
        },
        "country_iso3": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "country_population": {
            "type": "integer"
        },
        "country_gdp_per_capita": {
            "type": "float"
        },
        "recovered_doublingRate": {
            "type": "float"
        },
        "testing_positive": {
            "type": "float"
        },
        "testing_negative": {
            "type": "float"
        },
        "testing_dateChecked": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "testing_total": {
            "type": "float"
        },
        "testing_totalTestResults": {
            "type": "float"
        },
        "testing_posNeg": {
            "type": "float"
        },
        "testing_fips": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "testing_deathIncrease": {
            "type": "float"
        },
        "testing_hospitalizedIncrease": {
            "type": "float"
        },
        "testing_negativeIncrease": {
            "type": "float"
        },
        "testing_positiveIncrease": {
            "type": "float"
        },
        "testing_totalTestResultsIncrease": {
            "type": "float"
        },
        "testing_hospitalizedCumulative": {
            "type": "float"
        },
        "testing_hospitalized": {
            "type": "float"
        },
        "testing_death": {
            "type": "float"
        },
        "testing_hospitalizedCurrently": {
            "type": "float"
        },
        "testing_recovered": {
            "type": "float"
        },
        "state_iso3": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "testing_inIcuCurrently": {
            "type": "float"
        },
        "testing_inIcuCumulative": {
            "type": "float"
        },
        "iso3": {
            "type": "keyword"
        },
        "state_name": {
            "type": "keyword"
        },
        "name": {
            "type": "keyword"
        },
        "country_name": {
            "type": "keyword"
        },
        "wb_region": {
            "type": "keyword"
        },
        "confirmed_rolling_per_100k": {
            "type": "float"
        },
        "dead_rolling_per_100k": {
            "type": "float"
        },
        "recovered_rolling_per_100k": {
            "type": "float"
        },
        "confirmed_per_100k": {
            "type": "float"
        },
        "dead_per_100k": {
            "type": "float"
        },
        "recovered_per_100k": {
            "type": "float"
        }
    }
    return mapping
