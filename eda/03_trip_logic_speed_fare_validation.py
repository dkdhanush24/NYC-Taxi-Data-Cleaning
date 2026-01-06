import pandas as pd
import numpy as np

df = pd.read_parquet("yellow_tripdata_2019-06.parquet")

print(df.shape)

df = pd.read_parquet("NY_yellow_taxi_data_fully_cleaned.parquet")

print(df.shape)
