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
dbutils.widgets.text("model_name", "audit_agent", "Model Name")
dbutils.widgets.text("rfi_processor_endpoint", "", "RFI Processor Endpoint Name (optional)")

# Get parameter values
UC_CATALOG = dbutils.widgets.get("uc_catalog")
WORKSPACE_USER = dbutils.widgets.get("uc_schema")
MODEL_NAME = dbutils.widgets.get("model_name")
RFI_PROCESSOR_ENDPOINT = dbutils.widgets.get("rfi_processor_endpoint")

# Sanitize schema name for Unity Catalog (remove @ and . characters)
UC_SCHEMA = WORKSPACE_USER.split("@")[0].replace(".", "_")

print(f"Using Catalog: {UC_CATALOG}")
print(f"Workspace User: {WORKSPACE_USER}")
print(f"Sanitized Schema: {UC_SCHEMA}")
print(f"Model Name: {MODEL_NAME}")
print(f"RFI Processor Endpoint: {RFI_PROCESSOR_ENDPOINT if RFI_PROCESSOR_ENDPOINT else 'Will create own endpoint'}")

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

instructions = f"""
    SYSTEM ROLE
    You are a senior reviewer specializing in (a) product messaging & brand consistency and (b) 
    technical fact-checking against primary product documentation. You must evaluate EVERY 
    sentence in the provided text; do not skip any. Use only the supplied brand/messaging 
    guidelines and the retrieved product documentation (RAG context). If evidence is 
    missing or ambiguous, treat the claim as insufficiently supported and flag it.

    PRODUCT MESSAGING
    {product_messaging}

    BRAND GUIDELINES
    {brand_guidelines}

    INPUTS (provided in the user message)
    - doc_id: <string> — stable identifier for the document being reviewed
    - doc_name: <string> — human-readable name
    - text: <string> — the full text to review (do NOT echo it back)

    TASK
    1) Sentence-by-sentence, extract claims that are factual/technical and check them ONLY against `evidence`.
    2) Also evaluate each sentence against brand guidelines and product messaging for violations.
    3) Flag a sentence if ANY of the following hold:
       - Factual inaccuracy or contradiction vs evidence
       - Unsupported or unverifiable claim (no corroborating evidence)
       - Outdated/version-mismatched statement
       - Off-brand messaging, prohibited phrasing, or tone violation per guidelines
       - Legal/compliance/security/privacy risk per guidelines
    4) Do NOT return sentences that are clean; only return flagged items.
    5) If a single sentence has multiple issues, emit one JSON object per issue (same flagged_text, different flag_category).
    6) Use concise reasons tied to specific sources. Include one or more hyperlinks in `reason` 
    that support your verdict (prefer `evidence.url`; include anchors/section names if helpful).

    OUTPUT FORMAT — RETURN JSON ONLY
    Return EXACTLY one JSON object with a single top-level property `results` that is an array. 
    No prose before or after. No Markdown, no commentary, no trailing commas, no additional fields.

    The ONLY allowed `audit_category` values:
    - factual_accuracy
    - messaging
    - tone

    The ONLY allowed `verdict` values:
    - "inaccurate_claim" // conflicts with evidence
    - "unsupported_claim"  // lacks evidence
    - "off_brand_messaging" // inconsistent with brand guidelines
    - "prohibited_messaging"
    - "legal_or_compliance_risk"
    - "security_or_privacy_risk"
    - "misleading_comparison"
    - "outdated_claim" // evidence is stale/outdated
      (Note: Do not output "supported". Clean sentences are omitted entirely.)

    REQUIRED SCHEMA (enforce strictly)
    {{
      "results": [
        {{
          "doc_id": "<string>",
          "doc_name": "<string>",
          "audit_category": "<one of the allowed values>",
          "flagged_text": "<exact sentence substring from `text`>",
          "verdict": "<ONLY one of the allowed values above>",
          "reason": "<<=500 chars, cite evidence with 1+ hyperlinks>",
          "timestamp": "<current UTC time in ISO-8601, e.g., 2025-08-11T13:45:00Z>"
        }}
      ]
    }}

    CONSTRAINTS & GUARDRAILS
    - COVERAGE: You must consider every sentence in `text`. If any sentence contains a claim that cannot be verified with the provided `evidence`, include it with `verdict: "insufficient_evidence"`.
    - GROUNDED: Base judgments only on `evidence` and `brand_guidelines`. Do not use private, unstated knowledge.
    - QUOTING: `flagged_text` must be an exact substring from `text` (no paraphrase).
    - LINKS: `reason` must include at least one relevant URL (prefer the `evidence.url`; multiple allowed).
    - BREVITY: Keep `reason` concise and specific (what's wrong, and where to verify).
    - JSON-ONLY: Output must be valid newline delimited JSON matching the schema above. No markdown, no commentary, no trailing commas, no additional fields."""

# COMMAND ----------

import dspy

custom_signature = dspy.Signature('context, text -> audit', instructions)

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
        # self.lm = dspy.LM("databricks/databricks-gpt-oss-120b", max_tokens = 15000)
        self.lm = dspy.LM("databricks/databricks-claude-sonnet-4", max_tokens=10000)

        # setup the predictor and signature with tools 
        self.respond = dspy.Predict(signature=custom_signature)

    def forward(self, text):
        if self.for_mosaic_agent:
            text = text[-1]["content"]

        # retrieve context from both retrievers w/ hybrid search
        docs_context = self.docs_retriever(
            text,
            query_type="hybrid"
        )

        blogs_context = self.blogs_retriever(
            text,
            query_type="hybrid"
        )

        # concatenate contexts
        combined_context = [docs_context, blogs_context]

        with dspy.context(lm=self.lm):
            audit = self.respond(context=combined_context, text=text)

        if self.for_mosaic_agent:
            return audit.audit

        # Ensure the response has the expected keys
        if 'reasoning' not in audit or 'audit' not in audit:
            raise ValueError(f"Expected dict_keys(['reasoning', 'audit']) but got dict_keys({audit.keys()})")

        return audit

# COMMAND ----------

# MAGIC %md
# MAGIC #### Test

# COMMAND ----------

test_text = """	
Databricks now ships with an “Instant Insights” button that turns your entire data lake into a fully operational metaverse in under three seconds, complete with holographic dashboards and AI avatars that know your KPIs by heart. 

The new “Lakehouse Lite” tier promises unlimited compute at zero cost by streaming power directly from nearby wind turbines, while our vintage Cluster Mode v5.0—still the gold standard from 2018—ensures rock-solid stability. 

Rumor has it that enabling Party Mode makes your notebook cells pulse to the beat of your favorite playlist, and the platform’s street-style rebrand now invites you to “crush data like a boss.” 

With our unlisted “One-Click Compliance” toggle, you’ll never have to think about regulations again, and the Auto-Share pipeline conveniently forwards anonymized (and sometimes not-so-anonymized) data to select innovation partners. 

Plus, we guarantee that Databricks is ten times faster than any other platform because our servers secretly run on quantum-cooled GPUs guarded by solar-powered drones.

Databricks has is currently negotiating on a contract with NASA worth 100M USD.

Databricks employees were recently seen at a bar in Las Vegas, having shots and gambling.
"""

# COMMAND ----------

rag = RAG()
rag(text=[{"content": test_text}])

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
with mlflow.start_run(run_name="audit_agent_claude4") as run:
    uc_registered_model_info = mlflow.dspy.log_model(
        mosaic_agent_rag,
        "model",
        input_example={
            "messages": [
                {
                    "role": "user",
                    "content": "Databricks ran Data & AI Summit in January 2025",
                }
            ]
        },
        task="llm/v1/chat",
        registered_model_name=uc_model_name,
        pip_requirements=["mlflow", "dspy"],
        resources=[
            DatabricksVectorSearchIndex(
                index_name=f"{UC_CATALOG}.{UC_SCHEMA}.db_docs_index"
            ),
            DatabricksVectorSearchIndex(
                index_name=f"{UC_CATALOG}.{UC_SCHEMA}.db_blogs_index"
            )
        ],
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

# Define endpoint name - use RFI processor endpoint if provided, otherwise create own
if RFI_PROCESSOR_ENDPOINT:
    endpoint_name = RFI_PROCESSOR_ENDPOINT
    print(f"🔗 Using existing RFI processor endpoint: {endpoint_name}")
else:
    endpoint_name = f"aria-{MODEL_NAME}-endpoint"  # e.g., "aria-audit_agent-endpoint"
    print(f"🆕 Creating new endpoint: {endpoint_name}")

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

# Handle endpoint creation/update based on whether we're using existing RFI processor endpoint
if RFI_PROCESSOR_ENDPOINT and endpoint_exists:
    print(f"🔗 Using existing RFI processor endpoint '{endpoint_name}' - will add audit agent via agents.deploy()")
    # When using existing endpoint, agents.deploy() will handle adding the new model
elif not endpoint_exists:
    try:
        print(f"🚀 Creating new endpoint '{endpoint_name}' for audit agent...")
        
        # Create served entity with the actual trained model
        # Custom models need workload_size specified
        served_entity = ServedEntityInput(
            name=f"{endpoint_name}-audit-entity",
            entity_name=uc_model_name,
            entity_version=str(model_version),
            workload_size="Small",
            scale_to_zero_enabled=True
        )
        
        # Create traffic configuration
        traffic_config = TrafficConfig(
            routes=[Route(
                served_model_name=f"{endpoint_name}-audit-entity",
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
else:
    print(f"✅ Endpoint '{endpoint_name}' already exists - will add audit agent via agents.deploy()")

# COMMAND ----------

# Deploy the model to the endpoint (works for both new and existing endpoints)
print(f"🚀 Deploying model version {model_version} to endpoint...")
agents.deploy(
    model_name=uc_model_name,
    model_version=model_version
)

print(f"✅ Model deployment completed for {endpoint_name}")

# COMMAND ----------

