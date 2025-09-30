# Databricks notebook source
# MAGIC %md
# MAGIC ### Data preparation
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Tested on Databricks Runtime 15.4
# MAGIC - Web scraping functionality may not work as expected on Serverless compute
# MAGIC - For demo purposes, use `sample_data_mode=true` to use local sample files
# MAGIC 
# MAGIC To start off, we ingest previous RFI questions/answers into a table named `rfi_qa_bronze`. To prep it for vector search we: 
# MAGIC * Join Q/A into one column, formatted nicely
# MAGIC * Run an embedding model to see how many tokens are in the chunk
# MAGIC * Create this new token length as a column
# MAGIC * Create a unique column for a chunk_id (like an md5 hash or something)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Install dependencies

# COMMAND ----------

# MAGIC %pip install --quiet tokenizers torch transformers openpyxl
# MAGIC %pip install -U --quiet databricks-sdk langchain==0.1.13
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Parameters

# COMMAND ----------

# Set up widgets for parameterization
dbutils.widgets.text("uc_catalog", "users", "Unity Catalog")
dbutils.widgets.text("uc_schema", "rafi.kurlansik@databricks.com", "Unity Catalog Schema (User Email)") 
dbutils.widgets.text("data_path", "/Workspace/Users/rafi.kurlansik@databricks.com/aria-bundle/files", "Data Path")
dbutils.widgets.text("responses_file", "/Workspace/Users/rafi.kurlansik@databricks.com/aria-bundle/files/sample_data/knowledge_base/databricks_qa_dataset.csv", "Responses Data File")
dbutils.widgets.text("sample_data_mode", "true", "Use Sample Data (true/false)")

# Get parameter values
UC_CATALOG = dbutils.widgets.get("uc_catalog")
WORKSPACE_USER = dbutils.widgets.get("uc_schema")  # For workspace file access
# Sanitize schema name - use only the part before @ and replace . with _
UC_SCHEMA = WORKSPACE_USER.split("@")[0].replace(".", "_")  # For Unity Catalog
DATA_PATH = dbutils.widgets.get("data_path")
RESPONSES_FILE = dbutils.widgets.get("responses_file")
SAMPLE_DATA_MODE = dbutils.widgets.get("sample_data_mode").lower() == "true"

print(f"Using Catalog: {UC_CATALOG}")
print(f"Using Schema: {UC_SCHEMA} (sanitized from: {WORKSPACE_USER})")
print(f"Data Path: {DATA_PATH}")
print(f"Sample Data Mode: {SAMPLE_DATA_MODE}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Ingest raw data and save to bronze table

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Define the schema
schema = StructType([
    StructField("question", StringType(), True),
    StructField("response", StringType(), True),
    StructField("year", IntegerType(), True),
])

# Read the data file based on mode
if SAMPLE_DATA_MODE:
    # Use sample CSV data (databricks_qa_dataset.csv)
    file_path = f"/Workspace/Users/{WORKSPACE_USER}/aria-bundle/files/sample_data/knowledge_base/databricks_qa_dataset.csv"
    pandas_df = pd.read_csv(file_path)
    print(f"Loaded sample data from: {file_path}")
else:
    # Use file from widget path
    file_path = RESPONSES_FILE
    if not file_path or file_path.strip() == "":
        raise ValueError("responses_file widget cannot be empty when sample_data_mode=false")
    
    try:
        if file_path.endswith('.csv'):
            pandas_df = pd.read_csv(file_path)
        elif file_path.endswith(('.xlsx', '.xls')):
            pandas_df = pd.read_excel(file_path)
        else:
            raise ValueError(f"Unsupported file format. Expected .csv, .xlsx, or .xls, got: {file_path}")
        print(f"Loaded production data from: {file_path}")
    except FileNotFoundError:
        raise FileNotFoundError(f"File not found: {file_path}")

print(f"Data shape: {pandas_df.shape}")

# Ensure the data types match the schema
pandas_df['response'] = pandas_df['response'].astype(str)

# Convert the pandas DataFrame to a Spark DataFrame
qa_df = spark.createDataFrame(pandas_df, schema=schema)

display(qa_df)

# COMMAND ----------

bronze_table_name = f"{UC_CATALOG}.{UC_SCHEMA}.rfi_qa_bronze"

qa_df.write.format("delta").mode("overwrite").saveAsTable(bronze_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Concatenate columns for silver table

# COMMAND ----------

from pyspark.sql.functions import concat_ws, col, lit

# Concatenate 'question' and 'response' columns
concatenated_df = qa_df.withColumn("text", concat_ws(" ", col("question"), lit("Response:"), col("response")))

# Save the new DataFrame as a silver table
silver_table_name = f"{UC_CATALOG}.{UC_SCHEMA}.rfi_qa_silver"
concatenated_df.write.format("delta").mode("overwrite").saveAsTable(silver_table_name)

display(concatenated_df)

# COMMAND ----------

# Filter out rows where the response value is 'n/a'
filtered_df = concatenated_df.filter(concatenated_df.response != "n/a")
display(filtered_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Tokenize text to embeddings

# COMMAND ----------

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline
from transformers.utils import logging
import pyspark.sql.functions as func
from pyspark.sql.types import MapType, StringType
from langchain.text_splitter import RecursiveCharacterTextSplitter, CharacterTextSplitter
from pyspark.sql import Column
from pyspark.sql.types import *
from datetime import timedelta
from typing import List
import warnings

# Create a tokenizer object using the `AutoTokenizer` class and the pre-trained model 'BAAI/bge-large-en-v1.5'
tokenizer = AutoTokenizer.from_pretrained('BAAI/bge-large-en-v1.5')

# Defaults
bge_context_window_length_tokens = 512
chunk_size_tokens = 425
chunk_overlap_tokens = 75
databricks_fmapi_bge_endpoint = "databricks-gte-large-en"
fmapi_embeddings_task = "llm/v1/embeddings"

chunk_column_name = "chunked_text"
chunk_id_column_name = "chunk_id"

@func.udf(returnType=ArrayType(StringType()))
def split_char_recursive(content: str) -> List[str]:
    text_splitter = CharacterTextSplitter.from_huggingface_tokenizer(
        tokenizer, chunk_size=chunk_size_tokens, chunk_overlap=chunk_overlap_tokens
    )
    chunks = text_splitter.split_text(content)
    return [doc for doc in chunks]

df_chunked = concatenated_df.select(
    "*", func.explode(split_char_recursive("text")).alias(chunk_column_name)
).drop(func.col("text"))
df_chunked = df_chunked.select(
    "*", func.md5(func.col(chunk_column_name)).alias(chunk_id_column_name)
)

display(df_chunked)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gold table
# MAGIC
# MAGIC The gold table is created from a filtered silver table to reflect the most recent question and answer pairs.

# COMMAND ----------

filteredDF = df_chunked.filter(df_chunked.year >= 2025)
display(filteredDF)

# COMMAND ----------

gold_table_name = f"{UC_CATALOG}.{UC_SCHEMA}.rfi_qa_gold"

filteredDF.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(gold_table_name)

# Enable CDC for Vector Search Delta Sync
spark.sql(f"ALTER TABLE {gold_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %md
# MAGIC