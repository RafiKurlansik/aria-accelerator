# Databricks notebook source
# MAGIC %md
# MAGIC ### Vector Search Index Creation
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Tested on Databricks Runtime 15.4
# MAGIC - Requires Vector Search endpoint to be available
# MAGIC 
# MAGIC #### Install dependencies, specify resource identities

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Set up widgets for parameterization
dbutils.widgets.text("uc_catalog", "users", "Unity Catalog")
dbutils.widgets.text("uc_schema", "rafi.kurlansik@databricks.com", "Unity Catalog Schema (User Email)")
dbutils.widgets.text("vector_search_endpoint", "one-env-shared-endpoint-2", "Vector Search Endpoint Name")
dbutils.widgets.text("embedding_model_endpoint", "databricks-gte-large-en", "Embedding Model Endpoint")
dbutils.widgets.text("sample_data_mode", "true", "Use Sample Data (true/false)")

# Get parameter values
UC_CATALOG = dbutils.widgets.get("uc_catalog")
WORKSPACE_USER = dbutils.widgets.get("uc_schema")  # For workspace file access
# Sanitize schema name - use only the part before @ and replace . with _
UC_SCHEMA = WORKSPACE_USER.split("@")[0].replace(".", "_")  # For Unity Catalog
VECTOR_SEARCH_ENDPOINT = dbutils.widgets.get("vector_search_endpoint")
EMBEDDING_MODEL_ENDPOINT = dbutils.widgets.get("embedding_model_endpoint")

print(f"Using Catalog: {UC_CATALOG}")
print(f"Using Schema: {UC_SCHEMA} (sanitized from: {WORKSPACE_USER})")
print(f"Vector Search Endpoint: {VECTOR_SEARCH_ENDPOINT}")
print(f"Embedding Model: {EMBEDDING_MODEL_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create RFI responses index

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.utils import AnalysisException

client = VectorSearchClient()

# COMMAND ----------

# If the index has been corrupted, here is a way to delete it
#client.delete_index(endpoint_name=VECTOR_SEARCH_ENDPOINT, index_name="users.rafi_kurlansik.db_docs_index")

# COMMAND ----------

# Check if RFI QA index exists
rfi_qa_index_name = f"{UC_CATALOG}.{UC_SCHEMA}.rfi_qa_index"
try:
    existing_index = client.get_index(rfi_qa_index_name)
    print(f"✅ Index '{rfi_qa_index_name}' already exists, skipping creation")
except Exception:
    # Index doesn't exist, create it
    try:
        print(f"🚀 Creating index '{rfi_qa_index_name}'...")
        index = client.create_delta_sync_index(
          endpoint_name=VECTOR_SEARCH_ENDPOINT,
          source_table_name=f"{UC_CATALOG}.{UC_SCHEMA}.rfi_qa_gold",
          index_name=rfi_qa_index_name,
          pipeline_type="TRIGGERED",
          primary_key="chunk_id",
          embedding_source_column="chunked_text",
          embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT
        )
        print(f"✅ Index '{rfi_qa_index_name}' created successfully")
    except Exception as e:
        error_message = str(e)
        if "RESOURCE_ALREADY_EXISTS" in error_message:
            print(f"✅ Index '{rfi_qa_index_name}' already exists, continuing...")
        else:
            print(f"❌ Error creating index '{rfi_qa_index_name}': {e}")
            raise  # Re-raise if it's not a "already exists" error

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Databricks documentation index

# COMMAND ----------

# UC locations to store the chunked documents & index
DBDOCS_CHUNKS_DELTA_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.dbdocs_rag_chunks"
DBDOCS_CHUNKS_VECTOR_INDEX = f"{UC_CATALOG}.{UC_SCHEMA}.db_docs_index"

print(DBDOCS_CHUNKS_DELTA_TABLE)
print(DBDOCS_CHUNKS_VECTOR_INDEX)

# COMMAND ----------

# Check if DB docs index exists
db_docs_index_name = f"{UC_CATALOG}.{UC_SCHEMA}.db_docs_index"
try:
    existing_index = client.get_index(db_docs_index_name)
    print(f"✅ Index '{db_docs_index_name}' already exists, skipping creation")
except Exception:
    # Index doesn't exist, create it
    try:
        print(f"🚀 Creating index '{db_docs_index_name}'...")
        index = client.create_delta_sync_index_and_wait(
            endpoint_name=VECTOR_SEARCH_ENDPOINT,
            index_name=db_docs_index_name,
            primary_key="id",
            source_table_name=DBDOCS_CHUNKS_DELTA_TABLE,
            pipeline_type="TRIGGERED",
            embedding_source_column="text",
            embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
        )
        print(f"✅ Index '{db_docs_index_name}' created successfully")
    except Exception as e:
        error_message = str(e)
        if "RESOURCE_ALREADY_EXISTS" in error_message:
            print(f"✅ Index '{db_docs_index_name}' already exists, continuing...")
        else:
            print(f"❌ Error creating index '{db_docs_index_name}': {e}")
            raise  # Re-raise if it's not a "already exists" error

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create product keywords index

# COMMAND ----------

# UC locations to store the chunked documents & index
KEYWORDS_CHUNKS_DELTA_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.product_keyword_mappings"
KEYWORDS_CHUNKS_VECTOR_INDEX = f"{UC_CATALOG}.{UC_SCHEMA}.product_keyword_index"

print(KEYWORDS_CHUNKS_DELTA_TABLE)
print(KEYWORDS_CHUNKS_VECTOR_INDEX)

# COMMAND ----------

# Check if product keywords index exists
product_keyword_index_name = f"{UC_CATALOG}.{UC_SCHEMA}.product_keyword_index"
try:
    existing_index = client.get_index(product_keyword_index_name)
    print(f"✅ Index '{product_keyword_index_name}' already exists, skipping creation")
except Exception:
    # Index doesn't exist, create it
    try:
        print(f"🚀 Creating index '{product_keyword_index_name}'...")
        index = client.create_delta_sync_index_and_wait(
            endpoint_name=VECTOR_SEARCH_ENDPOINT,
            index_name=product_keyword_index_name,
            primary_key="id",
            source_table_name=KEYWORDS_CHUNKS_DELTA_TABLE,
            pipeline_type="TRIGGERED",
            embedding_source_column="description",
            embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
        )
        print(f"✅ Index '{product_keyword_index_name}' created successfully")
    except Exception as e:
        error_message = str(e)
        if "RESOURCE_ALREADY_EXISTS" in error_message:
            print(f"✅ Index '{product_keyword_index_name}' already exists, continuing...")
        else:
            print(f"❌ Error creating index '{product_keyword_index_name}': {e}")
            raise  # Re-raise if it's not a "already exists" error

# COMMAND ----------

# MAGIC %md ### Create blog index

# COMMAND ----------

# UC locations to store the chunked documents & index
BLOGS_CHUNKS_DELTA_TABLE = f"{UC_CATALOG}.{UC_SCHEMA}.dbblogs_rag_chunks"
BLOGS_CHUNKS_VECTOR_INDEX = f"{UC_CATALOG}.{UC_SCHEMA}.db_blogs_index"

print(BLOGS_CHUNKS_DELTA_TABLE)
print(BLOGS_CHUNKS_VECTOR_INDEX)

# COMMAND ----------

# Check if blogs index exists
db_blogs_index_name = f"{UC_CATALOG}.{UC_SCHEMA}.db_blogs_index"
try:
    existing_index = client.get_index(db_blogs_index_name)
    print(f"✅ Index '{db_blogs_index_name}' already exists, skipping creation")
except Exception:
    # Index doesn't exist, create it
    try:
        print(f"🚀 Creating index '{db_blogs_index_name}'...")
        index = client.create_delta_sync_index_and_wait(
            endpoint_name=VECTOR_SEARCH_ENDPOINT,
            index_name=db_blogs_index_name,
            primary_key="id",
            source_table_name=BLOGS_CHUNKS_DELTA_TABLE,
            pipeline_type="TRIGGERED",
            embedding_source_column="text",
            embedding_model_endpoint_name=EMBEDDING_MODEL_ENDPOINT,
        )
        print(f"✅ Index '{db_blogs_index_name}' created successfully")
    except Exception as e:
        error_message = str(e)
        if "RESOURCE_ALREADY_EXISTS" in error_message:
            print(f"✅ Index '{db_blogs_index_name}' already exists, continuing...")
        else:
            print(f"❌ Error creating index '{db_blogs_index_name}': {e}")
            raise  # Re-raise if it's not a "already exists" error