%run ./pii_utils

from pyspark.sql.functions import col, trim, to_timestamp, nullif, when

# 2. Key initialization
MY_KEY = "YOUR_KEY"
processor = PIIProcessor(MY_KEY)

# 3. Read bronze table
df_expedia = spark.table("bronze.expedia_raw")
df_hotel_weather = spark.table("bronze.hotel_weather_raw")

# Decryption expedia
expedia_pii = ["user_id", "user_location_city"]
df_ex_decrypted = processor.decrypt_columns(df_expedia, expedia_pii)

# Cleaning
df_ex_cleaned = df_ex_decrypted \
    .withColumn("date_time", to_timestamp(col("date_time"))) \
    .withColumn("srch_ci", to_timestamp(col("srch_ci"))) \
    .withColumn("srch_co", to_timestamp(col("srch_co"))) \
    .filter(col("id").isNotNull()) \
    .dropDuplicates()

# Again encryption
df_ex_final = processor.encrypt_columns(df_ex_cleaned, expedia_pii)

# Write to silver
df_ex_final.write.format("delta").mode("overwrite").saveAsTable("silver.expedia_processed")

# Decryption hotel_weather
hw_pii = ["address"]
df_hw_decrypted = processor.decrypt_columns(df_hotel_weather, hw_pii)

# Cleaning
df_hw_cleaned = df_hw_decrypted \
    .withColumn("address", trim(col("address"))) \
    .withColumn("name", trim(col("name"))) \
    .withColumn("wthr_date", to_timestamp(col("wthr_date"))) \
    .dropna(subset=["id", "wthr_date"]) \
    .distinct()

# Encryption
df_hw_final = processor.encrypt_columns(df_hw_cleaned, hw_pii)

# Write to silver
df_hw_final.write.format("delta").mode("overwrite").saveAsTable("silver.hotel_weather_processed")