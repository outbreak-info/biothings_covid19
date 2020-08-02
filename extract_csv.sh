#!/bin/bash

gifdata=$(awk -F "=" '/gif_data/ {print $2}' config.ini)
jsonout=$(awk -F "=" '/out_json_path/ {print $2}' config.ini)

echo "Reading json from ${jsonout}"

jq -r '["date","location_id","iso3","name","admin_level", "confirmed_per_100k", "confirmed_rolling", "confirmed_rolling_per_100k", "confirmed_rolling_14days_ago_diff", "confirmed_rolling_14days_ago_diff_per_100k", "dead_per_100k", "dead_rolling", "dead_rolling_per_100k", "dead_rolling_14days_ago_diff", "dead_rolling_14days_ago_diff_per_100k"], (.[] | select(.admin_level==2 and .country_iso3=="USA" ) | [.date,.location_id,.name,.iso3,.state_iso3,.admin_level,.confirmed_per_100k, .confirmed_rolling, .confirmed_rolling_per_100k, .confirmed_rolling_14days_ago_diff, .confirmed_rolling_14days_ago_diff_per_100k, .dead_per_100k, .dead_rolling, .dead_rolling_per_100k, .dead_rolling_14days_ago_diff, .dead_rolling_14days_ago_diff_per_100k]) | @csv' $jsonout > ${gifdata}test_counties.csv

echo "Wrote counties csv to ${gifdata}test_counties.csv"

jq -r '["date","location_id","iso3","name","admin_level", "confirmed_per_100k", "confirmed_rolling", "confirmed_rolling_per_100k", "confirmed_rolling_14days_ago_diff", "confirmed_rolling_14days_ago_diff_per_100k", "dead_per_100k", "dead_rolling", "dead_rolling_per_100k", "dead_rolling_14days_ago_diff", "dead_rolling_14days_ago_diff_per_100k"], (.[] | select(.admin_level==1 and .country_iso3=="USA" ) | [.date,.location_id,.name,.iso3,.state_iso3,.admin_level,.confirmed_per_100k, .confirmed_rolling, .confirmed_rolling_per_100k, .confirmed_rolling_14days_ago_diff, .confirmed_rolling_14days_ago_diff_per_100k, .dead_per_100k, .dead_rolling, .dead_rolling_per_100k, .dead_rolling_14days_ago_diff, .dead_rolling_14days_ago_diff_per_100k]) | @csv' $jsonout > ${gifdata}test_states.csv

echo "Wrote states csv to ${gifdata}test_states.csv"

jq -r '["date","location_id","iso3","name","admin_level", "confirmed_per_100k", "confirmed_rolling", "confirmed_rolling_per_100k", "confirmed_rolling_14days_ago_diff", "confirmed_rolling_14days_ago_diff_per_100k", "dead_per_100k", "dead_rolling", "dead_rolling_per_100k", "dead_rolling_14days_ago_diff", "dead_rolling_14days_ago_diff_per_100k"], (.[] | select(.admin_level==1.5 and .country_name=="United States of America") | [.date,.location_id,.name,.iso3,.state_iso3,.admin_level,.confirmed_per_100k, .confirmed_rolling, .confirmed_rolling_per_100k, .confirmed_rolling_14days_ago_diff, .confirmed_rolling_14days_ago_diff_per_100k, .dead_per_100k, .dead_rolling, .dead_rolling_per_100k, .dead_rolling_14days_ago_diff, .dead_rolling_14days_ago_diff_per_100k]) | @csv' $jsonout > ${gifdata}test_metros.csv

echo "Wrote metro csv to ${gifdata}test_metros.csv"

jq -r '["date","location_id","iso3","name","admin_level", "confirmed_per_100k", "confirmed_rolling", "confirmed_rolling_per_100k", "confirmed_rolling_14days_ago_diff", "confirmed_rolling_14days_ago_diff_per_100k", "dead_per_100k", "dead_rolling", "dead_rolling_per_100k", "dead_rolling_14days_ago_diff", "dead_rolling_14days_ago_diff_per_100k"], (.[] | select(.admin_level==0) | [.date,.location_id,.name,.iso3,.state_iso3,.admin_level,.confirmed_per_100k, .confirmed_rolling, .confirmed_rolling_per_100k, .confirmed_rolling_14days_ago_diff, .confirmed_rolling_14days_ago_diff_per_100k, .dead_per_100k, .dead_rolling, .dead_rolling_per_100k, .dead_rolling_14days_ago_diff, .dead_rolling_14days_ago_diff_per_100k]) | @csv' $jsonout  > ${gifdata}test_admin0.csv

echo "Wrote country csv to ${gifdata}test_admin0.csv"
