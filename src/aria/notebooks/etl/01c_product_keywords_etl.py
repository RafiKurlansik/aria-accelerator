# Databricks notebook source
# MAGIC %md
# MAGIC ### Product Keywords ETL Pipeline
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Tested on Databricks Runtime 15.4
# MAGIC - For demo purposes, use `sample_data_mode=true` to use local sample files
# MAGIC 
# MAGIC #### Configuration Parameters

# COMMAND ----------

# Set up widgets for parameterization
dbutils.widgets.text("uc_catalog", "users", "Unity Catalog")
dbutils.widgets.text("uc_schema", "rafi.kurlansik@databricks.com", "Unity Catalog Schema (User Email)")
dbutils.widgets.text("data_path", "/Workspace/Users/rafi.kurlansik@databricks.com/aria-bundle/files", "Data Path")
dbutils.widgets.text("sample_data_mode", "true", "Use Sample Data (true/false)")

# Get parameter values
UC_CATALOG = dbutils.widgets.get("uc_catalog")
WORKSPACE_USER = dbutils.widgets.get("uc_schema")  # For workspace file access
# Sanitize schema name - use only the part before @ and replace . with _
UC_SCHEMA = WORKSPACE_USER.split("@")[0].replace(".", "_")  # For Unity Catalog
DATA_PATH = dbutils.widgets.get("data_path")
SAMPLE_DATA_MODE = dbutils.widgets.get("sample_data_mode").lower() == "true"

print(f"Using Catalog: {UC_CATALOG}")
print(f"Using Schema: {UC_SCHEMA} (sanitized from: {WORKSPACE_USER})")
print(f"Data Path: {DATA_PATH}")
print(f"Sample Data Mode: {SAMPLE_DATA_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load product keyword mapping

# COMMAND ----------

if SAMPLE_DATA_MODE:
    # Load from sample data using pandas (workspace files not accessible to Spark)
    import pandas as pd
    
    sample_path = f"/Workspace/Users/{WORKSPACE_USER}/aria-bundle/files/sample_data/knowledge_base/product_keyword_mappings.csv"
    print(f"Loading sample keywords from: {sample_path}")
    
    # Read with pandas first, then convert to Spark DataFrame
    pandas_df = pd.read_csv(sample_path)
    product_mapping = spark.createDataFrame(pandas_df)
else:
    # Load from volume
    volume_path = f"{DATA_VOLUME_PATH}/product_keyword_map.csv"
    print(f"Loading keywords from: {volume_path}")
    product_mapping = spark.read.option("header", True) \
                   .option("inferSchema", True) \
                   .option("delimiter", ",") \
                   .option("multiLine", True) \
                   .option("escape", "\"") \
                   .csv(volume_path)

display(product_mapping)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Add a distinct id column
product_mapping = product_mapping.withColumn("id", monotonically_increasing_id())

# COMMAND ----------

# UC locations to store the chunked documents
KEYWORD_DELTA_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.product_keyword_mappings"
print(KEYWORD_DELTA_TABLE)

# Ensure unique column names after transformations
#all_docs = all_docs.withColumnRenamed("chunk_index", "unique_chunk_index")

product_mapping.write.format("delta").mode("overwrite").option("mergeSchema", "true").saveAsTable(KEYWORD_DELTA_TABLE)
spark.sql(
    f"ALTER TABLE {KEYWORD_DELTA_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)