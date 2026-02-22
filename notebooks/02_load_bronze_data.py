storage_account = "YOUR_STORAGE_ACCOUNT_NAME""
container = "data"
base_url = f"abfss://{container}@{storage_account}.dfs.core.windows.net"
MY_KEY = "YOUR_16_CHAR_LENGTH_KEY"  # 16 lengths

processor = PIIProcessor(MY_KEY)

# Read Expedia
df_expedia = spark.read.format("avro").load(f"{base_url}/expedia")

# Encryption id and user_location_city
expedia_pii = ["user_id", "user_location_city"]
df_expedia_secure = processor.encrypt_columns(df_expedia, expedia_pii)

# Write bronze data expedia
df_expedia_secure.write.format("delta").mode("overwrite").saveAsTable("bronze.expedia_raw")

# Read hotel-weather
df_hw = spark.read.format("parquet").load(f"{base_url}/hotel-weather")
# Adress encryption
hw_pii = ["address"]
df_hw_secure = processor.encrypt_columns(df_hw, hw_pii)

# Write bronze data hotel-weather
df_hw_secure.write.format("delta").mode("overwrite").saveAsTable("bronze.hotel_weather_raw")

