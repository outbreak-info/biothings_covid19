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
        "confirmed": {
            "type": "float"
        },
        "confirmed_currentCases": {
            "type": "float"
        },
        "confirmed_currentIncrease": {
            "type": "float"
        },
        "confirmed_currentPctIncrease": {
            "type": "float"
        },
        "confirmed_currentToday": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
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
        "recovered": {
            "type": "float"
        },
        "recovered_currentCases": {
            "type": "float"
        },
        "recovered_currentIncrease": {
            "type": "float"
        },
        "recovered_currentToday": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
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
        "dead_currentCases": {
            "type": "float"
        },
        "dead_currentIncrease": {
            "type": "float"
        },
        "dead_currentToday": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
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
        "dead_currentPctIncrease": {
            "type": "float"
        },
        "first_dead-first_confirmed": {
            "type": "integer"
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
        "recovered_currentPctIncrease": {
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
        "testing_recovered": {
            "type": "float"
        },
        "testing_hospitalizedCurrently": {
            "type": "float"
        },
        "testing_inIcuCumulative": {
            "type": "float"
        },
        "testing_inIcuCurrently": {
            "type": "float"
        },
        "state_iso3": {
            "normalizer": "keyword_lowercase_normalizer",
            "type": "keyword"
        },
        "name": {
            "type": "keyword"
        },
        "country_name": {
            "type": "keyword"
        },
        "iso3": {
            "type": "keyword"
        },
        "wb_region": {
            "type": "keyword"
        },
        "state_name": {
            "type": "keyword"
        }
    }
    return mapping
