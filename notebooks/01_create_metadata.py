storage_account = "YOUR_STORAGE_ACCOUNT_NAME"
container = "YOUR_CONTAINER_NAME"
base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net"

# 1. Create the physical directories on ADLS
for layer in ["bronze", "silver", "gold"]:
    dbutils.fs.mkdirs(f"{base_path}/{layer}")

# 2. Create the Metastore Schemas
spark.sql(f"CREATE SCHEMA IF NOT EXISTS bronze LOCATION '{base_path}/bronze'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS silver LOCATION '{base_path}/silver'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS gold LOCATION '{base_path}/gold'")