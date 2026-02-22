%run ./pii_utils

from pyspark.sql.functions import col, abs, count, max, min, last, first, datediff, month, avg

# Initialization
MY_KEY = "YOUR_AES_KEY"
processor = PIIProcessor(MY_KEY)

# Read silver table
df_expedia = spark.table("silver.expedia_processed")
df_hw = spark.table("silver.hotel_weather_processed")

# Decryption
df_expedia_dec = processor.decrypt_columns(df_expedia, ["user_id", "user_location_city"])
df_hw_dec = processor.decrypt_columns(df_hw, ["address"])

# Create TempView
df_expedia_dec.createOrReplaceTempView("expedia")
df_hw_dec.createOrReplaceTempView("weather")

# Calculate max and min temp for hotel and month
top_10_temp_diff = spark.sql("""
    SELECT 
        id as hotel_id,
        name,
        month,
        (MAX(avg_tmpr_c) - MIN(avg_tmpr_c)) as temp_diff
    FROM weather
    GROUP BY id, name, month
    ORDER BY ABS(temp_diff) DESC
    LIMIT 10
""")

top_10_temp_diff.write.format("delta").mode("overwrite").saveAsTable("gold.top_10_temp_diff_monthly")

# Top 10 busiest hotels monthly
top_10_busiest = spark.sql("""
    SELECT 
        hotel_id,
        month(to_date(srch_ci)) as month,
        COUNT(*) as visit_count
    FROM expedia
    GROUP BY hotel_id, month
    ORDER BY visit_count DESC
    LIMIT 10
""")

top_10_busiest.write.format("delta").mode("overwrite").saveAsTable("gold.top_10_busiest_hotels_monthly")

# We use a CTE (WITH) to first calculate the statistics across windows
# and then extract the unique rows for each reservation
weather_trend = spark.sql("""
    WITH stay_stats AS (
        SELECT 
            e.hotel_id,
            e.srch_ci as check_in,
            e.srch_co as check_out,
            DATEDIFF(e.srch_co, e.srch_ci) as stay_duration,
            w.avg_tmpr_c,
            w.wthr_date,
            -- Średnia temperatura w oknie całej rezerwacji
            AVG(w.avg_tmpr_c) OVER (PARTITION BY e.hotel_id, e.srch_ci, e.srch_co) as avg_temp_during_stay,
            -- Pierwsza i ostatnia temperatura w pobycie
            FIRST_VALUE(w.avg_tmpr_c) OVER (PARTITION BY e.hotel_id, e.srch_ci, e.srch_co ORDER BY w.wthr_date) as first_day_temp,
            LAST_VALUE(w.avg_tmpr_c) OVER (PARTITION BY e.hotel_id, e.srch_ci, e.srch_co ORDER BY w.wthr_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as last_day_temp
        FROM expedia e
        JOIN weather w ON e.hotel_id = w.id AND w.wthr_date BETWEEN e.srch_ci AND e.srch_co
        WHERE DATEDIFF(e.srch_co, e.srch_ci) > 7
    )
    SELECT DISTINCT
        hotel_id,
        check_in,
        check_out,
        stay_duration,
        avg_temp_during_stay,
        (last_day_temp - first_day_temp) as temp_trend
    FROM stay_stats
""")

weather_trend.write.format("delta").mode("overwrite").saveAsTable("gold.weather_trend_extended_stay")