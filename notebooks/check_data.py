print("BRONZE: Table Expedia")
bronze_expedia = spark.read.table("bronze.expedia_raw")
display(bronze_expedia.limit(10))

print("BRONZE: Table Hotel Weather")
bronze_weather = spark.read.table("bronze.hotel_weather_raw")
display(bronze_weather.limit(10))

print("Silver")
silver_data = spark.read.table("silver.expedia_processed")
display(silver_data.limit(10))

silver_data = spark.read.table("silver.hotel_weather_processed")
display(silver_data.limit(10))

print("--- GOLD: Top 10 Busiest Hotels Monthly ---")
display(spark.read.table("gold.top_10_busiest_hotels_monthly").limit(10))

print("--- GOLD: Top 10 Temp Diff Monthly ---")
display(spark.read.table("gold.top_10_temp_diff_monthly").limit(10))

print("--- GOLD: Weather Trend for Extended Stays ---")
display(spark.read.table("gold.weather_trend_extended_stay").limit(10))