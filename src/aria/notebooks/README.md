# ARIA Notebooks

This directory contains Jupyter notebooks (in Databricks `.py` format) organized by functionality. All notebooks maintain their original Databricks notebook source format with `# Databricks notebook source` headers and `# MAGIC` commands.

## Directory Structure

### `/agents/`
Agent development and training notebooks:
- `audit_agent.py` - Document auditing agent implementation
- `rfi_processor.py` - RFI processing agent implementation  
- `rag/` - Retrieval Augmented Generation implementations
  - `01_create_vs_idx.py` - Vector store index creation
- `eval/` - Agent evaluation notebooks
  - `Evaluate ARIA vs Submission.py` - Comparative evaluation
  - `quality.py` - Quality assessment tools

### `/deployment/`
Model serving infrastructure deployment:
- `create_serving_endpoints.py` - Creates model serving endpoints with foundation models

### `/etl/`
Data processing and ETL workflows:
- `01a_responses_etl.py` - Response data ETL pipeline
- `01b_docs_etl.py` - Document data ETL pipeline  
- `01c_product_keywords_etl.py` - Product keywords ETL pipeline
- `01d_blogs_etl.py` - Blog data ETL pipeline
- `web_scraper/` - Web scraping utilities and tools
  - `scraper.py` - Main scraping functionality
  - `web_scraper.py` - Web scraper implementation
  - `testing web scraping.py` - Scraping tests

### `/evaluation/`
Testing, evaluation, and quality assurance notebooks:
- `Evaluate_ARIA_vs_Submission.py` - Comparative evaluation between ARIA and submissions

## Usage

These notebooks are designed to run in Databricks environments and contain:
- Databricks-specific magic commands (`%md`, `%pip`, etc.)
- Spark SQL and DataFrame operations
- MLflow integration for model tracking
- Unity Catalog integration for data access

## Relationship to Main Application

While the main ARIA application (`/aria-accelerator/src/aria/services/`) provides production services, these notebooks contain:
- Research and development experiments
- Model training and evaluation workflows
- Data preparation and analysis
- Alternative implementations and prototypes
- Batch processing workflows

## Dependencies

Additional dependencies required by these notebooks are listed in:
- `requirements-notebooks.txt` - Notebook-specific dependencies  
- Root `requirements.txt` - Updated to include all notebook dependencies

Many packages may be pre-installed in Databricks environments, but these files ensure compatibility for local development and testing.

To install notebook dependencies:
```bash
pip install -r requirements-notebooks.txt
```

## Note on File Format

All `.py` files in this directory are Databricks notebooks in Python format, not regular Python scripts. They can be imported directly into Databricks workspaces while maintaining their notebook structure with cells and markdown content.
