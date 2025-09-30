# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Documentation ETL Pipeline
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Tested on Databricks Runtime 15.4
# MAGIC - Web scraping functionality may not work as expected on Serverless compute
# MAGIC - For demo purposes, use `sample_data_mode=true` to use local sample files

# COMMAND ----------

# MAGIC %pip install playwright
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Parameters

# COMMAND ----------

# Set up widgets for parameterization
dbutils.widgets.text("uc_catalog", "users", "Unity Catalog")
dbutils.widgets.text("uc_schema", "rafi.kurlansik@databricks.com", "Unity Catalog Schema (User Email)")
dbutils.widgets.text("sample_data_mode", "true", "Use Sample Data (true/false)")
dbutils.widgets.text("db_aws_url", "https://docs.databricks.com/en/doc-sitemap.xml", "Databricks AWS Docs URL")
dbutils.widgets.text("db_azure_url", "https://learn.microsoft.com/en-us/azure/databricks/introduction", "Databricks Azure Docs URL")
dbutils.widgets.text("db_gcp_url", "https://docs.databricks.com/gcp/en/sitemap.xml", "Databricks GCP Docs URL")
dbutils.widgets.text("db_sap_url", "https://docs.databricks.com/sap/en/sitemap.xml", "Databricks SAP Docs URL")
dbutils.widgets.text("mlflow_url", "https://mlflow.org/docs/latest/sitemap.xml", "MLflow Docs URL")
dbutils.widgets.text("spark_url", "https://spark.apache.org/sitemap.xml", "Spark Docs URL")
dbutils.widgets.text("delta_url", "https://docs.delta.io/latest/index.html", "Delta Docs URL")

# Get parameter values
CATALOG = dbutils.widgets.get("uc_catalog")
WORKSPACE_USER = dbutils.widgets.get("uc_schema")  # For workspace file access
# Sanitize schema name - use only the part before @ and replace . with _
SCHEMA = WORKSPACE_USER.split("@")[0].replace(".", "_")  # For Unity Catalog
SAMPLE_DATA_MODE = dbutils.widgets.get("sample_data_mode").lower() == "true"
DB_DOCS_TABLE = "dbdocs_rag_chunks"

DB_AWS_URL = dbutils.widgets.get("db_aws_url")
DB_AZURE_URL = dbutils.widgets.get("db_azure_url")
DB_GCP_URL = dbutils.widgets.get("db_gcp_url")
DB_SAP_URL = dbutils.widgets.get("db_sap_url")

print(f"Using Catalog: {CATALOG}")
print(f"Using Schema: {SCHEMA} (sanitized from: {WORKSPACE_USER})")
print(f"Sample Data Mode: {SAMPLE_DATA_MODE}")

# COMMAND ----------

# MAGIC %md ## Scrape DB Docs

# COMMAND ----------

if SAMPLE_DATA_MODE:
    # Load pre-generated Databricks documentation CSV (result of web scraping)
    import pandas as pd
    
    sample_path = f"/Workspace/Users/{WORKSPACE_USER}/aria-bundle/files/sample_data/knowledge_base/dbdocs.csv"
    print(f"Loading pre-generated Databricks documentation from: {sample_path}")
    
    all_db_docs = pd.read_csv(sample_path)
    print(f"Loaded {len(all_db_docs)} pre-generated Databricks documentation records")
    
else:
    # Use web scraping to generate fresh documentation data
    from web_scraper.scraper import *

    print("Running web scraping to generate fresh Databricks documentation...")
    
    db_aws_urls = crawl_site(DB_AWS_URL, max_pages=10, max_workers=1)
    db_aws_docs = fetch_and_chunk_pages(db_aws_urls, max_workers=1)

    db_gcp_urls = crawl_site(DB_GCP_URL, max_pages=10, max_workers=1)
    db_gcp_docs = fetch_and_chunk_pages(db_gcp_urls, max_workers=1)

    db_sap_urls = crawl_site(DB_SAP_URL, max_pages=10, max_workers=1)
    db_sap_docs = fetch_and_chunk_pages(db_sap_urls, max_workers=1)
    
    # Combine all scraped documentation
    all_db_docs = pd.concat(
        [db_aws_docs, db_gcp_docs, db_sap_docs],
        ignore_index=True
    )
    
    print(f"Generated {len(all_db_docs)} documentation records from web scraping")

# COMMAND ----------

display(all_db_docs)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

dbdocsDF = spark.createDataFrame(all_db_docs)

# Add ID column only if it doesn't already exist (for web scraping case)
if 'id' not in all_db_docs.columns:
    dbdocsDF = dbdocsDF.withColumn("id", monotonically_increasing_id())

dbdocsDF.write.option("mergeSchema", "true").mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.{DB_DOCS_TABLE}")
spark.sql(
    f"ALTER TABLE {CATALOG}.{SCHEMA}.{DB_DOCS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)