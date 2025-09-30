# Databricks notebook source
# MAGIC %md
# MAGIC ### Blogs ETL Pipeline
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Tested on Databricks Runtime 15.4
# MAGIC - Web scraping functionality may not work as expected on Serverless compute  
# MAGIC - For demo purposes, use `sample_data_mode=true` to use local sample files

# COMMAND ----------

# MAGIC %pip install playwright bs4 lxml
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Set up widgets for parameterization
dbutils.widgets.text("uc_catalog", "users", "Unity Catalog")
dbutils.widgets.text("uc_schema", "rafi.kurlansik@databricks.com", "Unity Catalog Schema (User Email)")
dbutils.widgets.text("sample_data_mode", "true", "Use Sample Data (true/false)")
dbutils.widgets.text("db_blogs_url", "https://www.databricks.com/en-blog-assets/sitemap/sitemap-0.xml", "Databricks Blogs URL")

# Get parameter values
CATALOG = dbutils.widgets.get("uc_catalog")
WORKSPACE_USER = dbutils.widgets.get("uc_schema")  # For workspace file access
# Sanitize schema name - use only the part before @ and replace . with _
SCHEMA = WORKSPACE_USER.split("@")[0].replace(".", "_")  # For Unity Catalog
SAMPLE_DATA_MODE = dbutils.widgets.get("sample_data_mode").lower() == "true"
DB_BLOGS_TABLE = "dbblogs_rag_chunks"
DB_BLOGS_URL = dbutils.widgets.get("db_blogs_url")

print(f"Using Catalog: {CATALOG}")
print(f"Using Schema: {SCHEMA} (sanitized from: {WORKSPACE_USER})")
print(f"Sample Data Mode: {SAMPLE_DATA_MODE}")

if SAMPLE_DATA_MODE:
    # Load sample blog data from CSV
    import pandas as pd
    
    sample_path = f"/Workspace/Users/{WORKSPACE_USER}/aria-bundle/files/sample_data/knowledge_base/databricks_blogs.csv"
    print(f"Loading sample blog data from: {sample_path}")
    
    db_blogs_docs = pd.read_csv(sample_path)
    print(f"Loaded {len(db_blogs_docs)} sample blog records")
        
else:
    # Use web scraping for blogs
    from web_scraper.scraper import *
    
    print("Running web scraping for Databricks blogs...")
    db_blogs_urls = crawl_site(DB_BLOGS_URL, max_pages=10000, max_workers=1)
    db_blogs_docs = fetch_and_chunk_pages(db_blogs_urls, max_workers=1)
    print(f"Generated {len(db_blogs_docs)} blog records from web scraping")

# COMMAND ----------

display(db_blogs_docs)

# COMMAND ----------

print(f"{CATALOG}.{SCHEMA}.{DB_BLOGS_TABLE}")

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

dbBlogsDF = spark.createDataFrame(db_blogs_docs)
dbBlogsDF = dbBlogsDF.withColumn("id", monotonically_increasing_id())

dbBlogsDF.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{DB_BLOGS_TABLE}")
spark.sql(
    f"ALTER TABLE {CATALOG}.{SCHEMA}.{DB_BLOGS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)