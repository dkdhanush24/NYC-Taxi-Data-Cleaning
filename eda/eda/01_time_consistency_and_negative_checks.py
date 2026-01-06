import pandas as pd
import numpy as np

# df = pd.read_parquet("yellow_tripdata_2019-06.parquet")

# # print(df.dtypes)

# #checking for datetime logic
# # print(df[["tpep_pickup_datetime", "tpep_dropoff_datetime"]].isna().sum)

# #check if the time provided is logically correct
# # invalid_time = df["tpep_dropoff_datetime"] < df["tpep_pickup_datetime"]

# # print(invalid_time.sum())

# #came to know that 16 rows are logically impossible

# df = df[df["tpep_dropoff_datetime"] >= df["tpep_pickup_datetime"]]

# #removed the 16 rows

# print((df["tpep_dropoff_datetime"] < df["tpep_pickup_datetime"]).sum())

# #value reduced to 0

# #saving the file

# df.to_parquet("yellow-taxi-data-partial-cleaned.parquet")

df = pd.read_parquet("yellow-taxi-data-partial-cleaned.parquet")

# removing neg values


df =df[(
    (df["fare_amount"] >= 0) &
    (df["extra"] >= 0) &
    (df["mta_tax"] >= 0) &
    (df["tip_amount"] >= 0) &
    (df["tolls_amount"] >= 0) &
    (df["improvement_surcharge"] >= 0) &
    (df["total_amount"] >= 0) &
    (df["congestion_surcharge"] >= 0) 
)]

# print(df["VendorID"].value_counts())
# print(df["tpep_pickup_datetime"].value_counts())
# print(df["tpep_dropoff_datetime"].value_counts())
# print(df["passenger_count"].value_counts())
# print(df["trip_distance"].value_counts())
# print(df["RatecodeID"].value_counts())
# print(df["store_and_fwd_flag"].value_counts())
# print(df["PULocationID"].value_counts())
# print(df["DOLocationID"].value_counts())
# print(df["payment_type"].value_counts())
# print(df["fare_amount"].value_counts())
# print(df["extra"].value_counts())
# print(df["mta_tax"].value_counts())
# print(df["tip_amount"].value_counts())
# print(df["tolls_amount"].value_counts())
# print(df["improvement_surcharge"].value_counts())
# print(df["total_amount"].value_counts())
# print(df["congestion_surcharge"].value_counts())
# print(df["airport_fee"].value_counts())

#controlling impossible zero values

df["zero_passenger"] = df["passenger_count"] == 0

df["zero_distance"] = df["trip_distance"] == 0

df["zero_fare"] = df["fare_amount"] == 0

df.to_parquet("yellow-taxi-data-partial-cleaned2.parquet")
