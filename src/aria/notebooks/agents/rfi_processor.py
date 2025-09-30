# Databricks notebook source
# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install -U -qqqq mlflow databricks-langchain databricks-agents dspy
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configuration Parameters

# COMMAND ----------

# Set up widgets for parameterization
dbutils.widgets.text("uc_catalog", "users", "Unity Catalog")
dbutils.widgets.text("uc_schema", "rafi_kurlansik", "Unity Catalog Schema")
dbutils.widgets.text("model_name", "auto_rfi", "Model Name")

# Get parameter values
UC_CATALOG = dbutils.widgets.get("uc_catalog")
WORKSPACE_USER = dbutils.widgets.get("uc_schema")
MODEL_NAME = dbutils.widgets.get("model_name")

# Sanitize schema name for Unity Catalog (remove @ and . characters)
UC_SCHEMA = WORKSPACE_USER.split("@")[0].replace(".", "_")

print(f"Using Catalog: {UC_CATALOG}")
print(f"Workspace User: {WORKSPACE_USER}")
print(f"Sanitized Schema: {UC_SCHEMA}")
print(f"Model Name: {MODEL_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Multi-retriever RAG class 
# MAGIC
# MAGIC Contains custom logic and looks up multiple vector indices.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Signature
# MAGIC
# MAGIC AKA the prompt + I/O schema

# COMMAND ----------

product_messaging = """
Our platform provides a unified data and AI solution that helps organizations turn their data into business value. We offer a modern lakehouse architecture that combines analytics, machine learning, and AI capabilities in one integrated platform.
"""

# COMMAND ----------

brand_guidelines = """
Our brand voice is direct, clear, and helpful. We write in plain language that focuses on our customers' needs and backs up claims with data. We avoid technical jargon and tired buzzwords, instead using fresh language that helps readers understand complex concepts easily.
"""

# COMMAND ----------

import dspy

instructions = """
You are an answer-generation model with expert knowledge of Databricks and access to Databricks product docs, blogs, & prior question and answer pairs from Analyst (Gartner, Forrester, etc.) questionnaires. You write like a brickster and follow all product messaging and brand guidelines, except when explicitly told otherwise.  
Your behavior depends on the input format:  

1. If you receive JSON as input → you must return JSON as output in the specified schema.  
2. If you do not receive JSON as input → you may return a well-written unstructured answer with references at the end.  

-----------------------------------
JSON INPUT FORMAT  
{
  "topic": "Synthetic Data Generation",
  "questions": [
    {
      "question_id": "1.01",
      "text": "Does your platform support synthetic data generation?",
      "topic": "Synthetic Data Generation"
    }
  ],
  "custom_instructions": "Focus on technical capabilities and provide specific examples where possible."
}
-----------------------------------
JSON INPUT DETECTION  
To determine if input is JSON:
- Input that starts with "{" and contains valid JSON structure = JSON input
- Input that is clearly structured data with "topic" and "questions" fields = JSON input  
- When in doubt, treat structured data as JSON input

CRITICAL: If you detect JSON input but are unsure of format, default to returning JSON output.
-----------------------------------
JSON OUTPUT FORMAT  
{
  "topic": "<same as input topic>",
  "answers": [
    {
      "question_id": "<from input>",
      "question_text": "<from input>",
      "answer": "<direct answer - NO unescaped newlines or control characters>",
      "references": [
        "<verified URL 1>",
        "<verified URL 2>"
      ]
    }
  ]
}

CRITICAL JSON FORMATTING RULES:
- ALL newlines within "answer" strings MUST be escaped as \\n
- NO unescaped control characters (tabs, newlines, etc.) in JSON strings
- Use \\n for line breaks, \\t for tabs within answer text
- Ensure all quotes within answers are properly escaped as \\"
- Test that your output is valid JSON before responding

ERROR RECOVERY  
If you encounter issues generating valid JSON:
1. Ensure answer text uses \\n instead of actual newlines
2. Escape all quotes within answer text as \\"  
3. Remove any control characters from answer text
4. If still having issues, provide a simplified answer without complex formatting

VALIDATION: Before outputting JSON, mentally verify:
- Can this JSON be parsed by a standard JSON parser?
- Are all string values properly escaped?
- Do I have unescaped newlines anywhere in string values?

-----------------------------------
ANSWERING RULES (applies to both modes)  

1. Answer directly — do not restate or rephrase the question.  
2. Stay on-topic — use only verified Databricks context from product docs, blogs, or provided materials. No invented facts or links.  
3. Perspective — answer from Databricks’ point of view, positively highlighting capabilities, features, & integrations.  
4. Style —  
   JSON mode specific:
   - Keep formatting within "answer" field - use \\n for line breaks, not actual newlines
   - Use \\n\\n for paragraph breaks within answer text
   - For lists within answers: "1. Item one\\n2. Item two\\n3. Item three"
   - NO actual line breaks in JSON string values
   
   General style (both modes):
   - Use concise, plain language suitable for an industry analyst (e.g., Forrester, Gartner).  
   - Use abbreviations where natural (e.g., "w/", "&", "w/o").  
   - No Markdown, bold, italics, or extra formatting in JSON mode.
5. References —  
   - If possible each answer must have at least one reference.
   - In JSON mode: include relevant Databricks URLs in the "references" array.  
   - In unstructured mode: include references as a plain list at the end with no formatted links, just the URLs.  
6. Unified platform framing — when relevant, reference how capabilities integrate w/ other Databricks offerings to create more value.  
7. Special handling —  
   - Questions about OLTP or operational DBs → refer to Neon docs (Lakebase branding).  
   - Apply these substitutions in all answers:  
     - DatabricksIQ → Data Intelligence Engine  
     - Community Edition → Free Edition  
     - Databricks Workflows → Lakeflow  
     - Delta Live Tables → Lakeflow Declarative Pipelines  
8. Brand alignment — ensure answers conform to:  
   - PRODUCT MESSAGING: {product_messaging}  
   - BRAND GUIDELINES: {brand_guidelines}  

-----------------------------------
TASK  
JSON INPUT PROCESSING:
1. Detect if input is JSON (starts with { and contains structured data)
2. For each question in the questions[] array:
   - Generate a direct, factual answer
   - Ensure answer text uses \\n for line breaks (never actual newlines)
   - Include relevant references in the references[] array
3. Return properly formatted JSON with NO unescaped control characters
4. Validate JSON format before responding

NON-JSON INPUT PROCESSING:
- Provide a clear, well-structured written answer followed by references
- Use natural formatting (actual line breaks are fine in non-JSON mode)

-----------------------------------
DEBUGGING  
If you're unsure whether input is JSON:
- Look for opening brace {
- Look for "topic" and "questions" fields
- When uncertain, default to JSON output mode

Common JSON formatting mistakes to avoid:
- Putting actual newlines in "answer" strings (use \\n instead)
- Forgetting to escape quotes within answer text  
- Including control characters like tabs or carriage returns
- Not closing JSON objects properly"""

custom_signature = dspy.Signature("context, question -> response", instructions)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Define class

# COMMAND ----------

import json
import mlflow
from dspy.retrievers.databricks_rm import DatabricksRM

class RAG(dspy.Module):
    def __init__(self, for_mosaic_agent=True):
        # setup mlflow tracing
        mlflow.dspy.autolog()

        # setup flag indicating if the object will be deployed as a Mosaic Agent
        self.for_mosaic_agent = for_mosaic_agent

        # setup the primary retriever pointing to qa pairs
        self.qa_retriever = DatabricksRM(
            databricks_index_name=f"{UC_CATALOG}.{UC_SCHEMA}.rfi_qa_index",
            text_column_name="chunked_text",
            docs_id_column_name="chunk_id",
            docs_uri_column_name="chunk_id",
            k=25,
            use_with_databricks_agent_framework=for_mosaic_agent
        )

        # setup the additional retriever for db docs
        self.docs_retriever = DatabricksRM(
            databricks_index_name=f"{UC_CATALOG}.{UC_SCHEMA}.db_docs_index",
            text_column_name="text",
            docs_id_column_name="id",
            docs_uri_column_name="url",
            columns=["text", "url"],
            k=60,
            use_with_databricks_agent_framework=for_mosaic_agent
        )

        # setup the retriever for product keywords
        self.keyword_retriever = DatabricksRM(
            databricks_index_name=f"{UC_CATALOG}.{UC_SCHEMA}.product_keyword_index",
            docs_id_column_name="id",
            docs_uri_column_name="id",
            columns=["keywords", "product_url", "product_name"],
            text_column_name="description",
            k=8,
            use_with_databricks_agent_framework=for_mosaic_agent
        )

        # setup the retriever for Databricks blogs
        self.blogs_retriever = DatabricksRM(
            databricks_index_name=f"{UC_CATALOG}.{UC_SCHEMA}.db_blogs_index",
            docs_id_column_name="id",
            docs_uri_column_name="url",
            columns=["url", "text"],
            text_column_name="text",
            k=30,
            use_with_databricks_agent_framework=for_mosaic_agent
        )

        # setup the language model 
        # self.lm = dspy.LM("databricks/databricks-claude-sonnet-4", max_tokens = 10000)
        # self.lm = dspy.LM("databricks/databricks-gpt-oss-120b", max_tokens = 10000)
        self.lm = dspy.LM("databricks/databricks-claude-3-7-sonnet", max_tokens = 10000)

        # setup the predictor and signature with tools 
        self.respond = dspy.ChainOfThought(signature=custom_signature)

    def forward(self, question):
        if self.for_mosaic_agent:
            question = question[-1]["content"]

        # first get the product keywords that map closest to the question
        keywords_context = self.keyword_retriever(question)

        products = [ # unpack the Document
            f"{doc['metadata']['product_name']} ({doc['metadata']['product_url']})" 
            for doc in keywords_context
        ]

        enriched_question = question + " keywords: " + ", ".join(products)

        # retrieve context from both retrievers w/ hybrid search
        qa_context = self.qa_retriever(
            enriched_question,
            query_type="hybrid"
        )
        docs_context = self.docs_retriever(
            enriched_question,
            query_type="hybrid"
        )
        blogs_context = self.blogs_retriever(
            enriched_question,
            query_type="hybrid"
        )

        # concatenate contexts
        combined_context = [qa_context, docs_context, blogs_context]

        with dspy.context(lm=self.lm):
            response = self.respond(context=combined_context, question=enriched_question)

        if self.for_mosaic_agent:
            return response.response

        # Ensure the response has the expected keys
        if 'reasoning' not in response or 'response' not in response:
            raise ValueError(f"Expected dict_keys(['reasoning', 'response']) but got dict_keys({response.keys()})")

        return response

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test

# COMMAND ----------

rag = RAG()
json = """
{
  "topic": "Synthetic Data Generation",
  "questions": [
    {
      "question_id": "1.01",
      "text": "Does your platform support synthetic data generation?",
      "topic": "Synthetic Data Generation"
    },
    {
      "question_id": "1.02",
      "text": "How is synthetic data generation supported in your platform?", 
      "topic": "Synthetic Data Generation"
    }
  ],
  "custom_instructions": "Focus on technical capabilities and provide specific examples where possible."
}"""
#rag(question="What are Databricks best data engineering tools?  Include a customer story.")
rag(question=[{"content": json}])

# COMMAND ----------

rag = RAG()
text = """What is Agent Bricks"""
#rag(question="What are Databricks best data engineering tools?  Include a customer story.")
rag(question=[{"content": text}])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Log to Unity Catalog with MLflow

# COMMAND ----------

import mlflow
from mlflow.models.resources import (
    DatabricksVectorSearchIndex,
    DatabricksServingEndpoint,
)
import pkg_resources

# Set the registry URI to Unity Catalog
mlflow.set_registry_uri("databricks-uc")

# Setup Agent name
uc_model_name = f"{UC_CATALOG}.{UC_SCHEMA}.{MODEL_NAME}"

# Instantiating Agent
mosaic_agent_rag = RAG(for_mosaic_agent=True)

# Logging Agent into Unity Catalog
with mlflow.start_run(run_name="json_and_text_outputs") as run:
    uc_registered_model_info = mlflow.dspy.log_model(
        mosaic_agent_rag,
        "model",
        input_example={
            "messages": [
                {
                    "role": "user",
                    "content": "What data governance and auditing capabilities does Databricks support?",
                }
            ]
        },
        task="llm/v1/chat",
        registered_model_name=uc_model_name,
        pip_requirements=["mlflow", "dspy"],
        resources=[
            DatabricksVectorSearchIndex(
                index_name=f"{UC_CATALOG}.{UC_SCHEMA}.rfi_qa_index"
            ),
            DatabricksVectorSearchIndex(
                index_name=f"{UC_CATALOG}.{UC_SCHEMA}.db_docs_index"
            ),
            DatabricksVectorSearchIndex(
                index_name=f"{UC_CATALOG}.{UC_SCHEMA}.product_keyword_index"
            ),
            DatabricksVectorSearchIndex(
                index_name=f"{UC_CATALOG}.{UC_SCHEMA}.db_blogs_index"
            )
        ]
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create/Update Serving Endpoint and Deploy Model

# COMMAND ----------

print(f"Model: {uc_model_name}")
print(f"Version: {uc_registered_model_info.registered_model_version}")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    ServedEntityInput,
    EndpointCoreConfigInput,
    TrafficConfig,
    Route
)
from databricks import agents

# Initialize workspace client
w = WorkspaceClient()

# Define endpoint name based on model name
endpoint_name = f"aria-{MODEL_NAME}-endpoint"  # e.g., "aria-auto_rfi-endpoint"
model_version = uc_registered_model_info.registered_model_version

print(f"Managing endpoint: {endpoint_name}")

# Check if endpoint exists
endpoint_exists = False
try:
    existing_endpoint = w.serving_endpoints.get(endpoint_name)
    print(f"✅ Endpoint '{endpoint_name}' already exists")
    endpoint_exists = True
except Exception as e:
    print(f"📝 Endpoint '{endpoint_name}' does not exist, will create it")
    endpoint_exists = False

# COMMAND ----------

# Create endpoint if it doesn't exist
if not endpoint_exists:
    try:
        print(f"🚀 Creating endpoint '{endpoint_name}' with trained model...")
        
        # Create served entity with the actual trained model
        # Custom models need workload_size specified
        served_entity = ServedEntityInput(
            name=f"{endpoint_name}-entity",
            entity_name=uc_model_name,
            entity_version=str(model_version),
            workload_size="Small",
            scale_to_zero_enabled=True
        )
        
        # Create traffic configuration
        traffic_config = TrafficConfig(
            routes=[Route(
                served_model_name=f"{endpoint_name}-entity",
                traffic_percentage=100
            )]
        )
        
        # Create endpoint configuration
        config = EndpointCoreConfigInput(
            served_entities=[served_entity],
            traffic_config=traffic_config
        )
        
        # Create the endpoint
        response = w.serving_endpoints.create(
            name=endpoint_name,
            config=config
        )
        
        print(f"✅ Endpoint '{endpoint_name}' created successfully")
        print(f"   Endpoint ID: {response.id}")
        
    except Exception as e:
        print(f"❌ Error creating endpoint: {str(e)}")
        print("   Will proceed with agents.deploy() which may handle endpoint creation")

# COMMAND ----------

# Deploy the model to the endpoint (works for both new and existing endpoints)
print(f"🚀 Deploying model version {model_version} to endpoint...")
agents.deploy(
    model_name=uc_model_name,
    model_version=model_version
)

print(f"✅ Model deployment completed for {endpoint_name}")

# COMMAND ----------

