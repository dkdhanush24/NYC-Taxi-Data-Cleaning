import pandas as pd
import numpy as np

df = pd.read_parquet("yellow-taxi-data-partial-cleaned2.parquet")

df["trip_duration"] = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
print(df["trip_duration"].describe(percentiles=[0.01 , 0.05 ,0.95, 0.99]))

df["neg_duration"] = (df["trip_duration"] < 0).astype(int)
df["zero_duration"] = (df["trip_duration"] == 0).astype(int)
df["long_duration"] = (df["trip_duration"] > 4 * 3600).astype(int)
df["short_duration"] = (df["trip_duration"] < 60).astype(int)

print(df[["neg_duration", "zero_duration", "long_duration","short_duration"]].sum())

df["short_time_long_trips"] = ((df["trip_duration"] < 60) & (df["trip_distance"] > 0.05)).astype(int)
print(df["short_time_long_trips"].sum())

df = df[df["zero_duration"] == 0]
df = df[df["short_time_long_trips"] == 0]

print(df[["neg_duration", "zero_duration", "long_duration","short_duration"]].sum())

df["short_time_long_trips"] = ((df["trip_duration"] < 60) & (df["trip_distance"] > 0.5)).astype(int)
print(df["short_time_long_trips"].sum())

df["avg_speed"] = (df["trip_distance"] / (df["trip_duration"] /3600))
print("average <= 0:",(df["avg_speed"] <= 0).sum())
print("average > 80:",(df["avg_speed"] > 80).sum())
print("low speed and long trips:",((df["avg_speed"] < 1) & (df["trip_distance"] > 1)).sum())

df = df[df["avg_speed"] > 0]
df = df[df["avg_speed"] <= 80]
df = df[~((df["avg_speed"] < 1) & (df["trip_distance"] > 1))]


df["avg_speed"] = (df["trip_distance"] / (df["trip_duration"] /3600))
print("average <= 0:",(df["avg_speed"] <= 0).sum())
print("average > 80:",(df["avg_speed"] > 80).sum())
print("low speed and long trips:",((df["avg_speed"] < 1) & (df["trip_distance"] > 1)).sum())

df.loc[df["trip_distance"] > 0, "fare_per_mile"] = (df["fare_amount"] / df["trip_distance"])

print((df["fare_per_mile"] < 1).sum())
print((df["fare_per_mile"] > 20).sum())

df = df[(df["fare_per_mile"] >= 1) & (df["fare_per_mile"] <= 20)]
# df["fare_per_mile"] = df[df["fare_per_mile"] > 1]
# df["fare_per_mile"] = df[df["fare_per_mile"] <= 20]
print((df["fare_per_mile"] < 1).sum())
print((df["fare_per_mile"] > 20).sum())

# print(df["airport_fee"].dtype)
# print(df["airport_fee"].isna().sum())

df["airport_fee"] = pd.to_numeric(df["airport_fee"],errors="coerce").fillna(0)
print(df["airport_fee"].isna().sum())

df["recomputed_total"] = (
    df["fare_amount"]
    + df["extra"]
    + df["mta_tax"]
    + df["tip_amount"]
    + df["tolls_amount"]
    + df["improvement_surcharge"]
    + df["congestion_surcharge"]
    + df["airport_fee"]
)

df["total_diff"] = (df["total_amount"] - df["recomputed_total"]).abs()
print((df["total_diff"] > 0.01).sum())
print(df.groupby("payment_type")["total_diff"].describe())

df =df[df["RatecodeID"] <= 6]

df = df[df["total_diff"] <= 2.51]
print((df["total_diff"] > 2.51).sum())
print(df.columns)

helper_cols = ['zero_passenger','zero_distance', 'zero_fare', 'trip_duration', 'neg_duration',
    'zero_duration', 'long_duration', 'short_duration','short_time_long_trips',
    'avg_speed', 'fare_per_mile','recomputed_total', 'total_diff']

df = df.drop(columns = helper_cols)

print(df.columns)
print(df.dtypes)
print(df.shape)

df.to_parquet("NY_yellow_taxi_data_fully_cleaned.parquet")
