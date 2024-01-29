python3 ingest-data.py \
    --user=root \
    --password=root \
    --port=5432 \
    --host=localhost \
    --db=ny_taxi \
    --table_name=green_taxi_data \
    --url="https://s3.amazonaws.com/nyc-tlc/trip+data/green_tripdata_2019-09.csv"
    