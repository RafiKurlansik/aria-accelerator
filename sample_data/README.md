# ARIA Sample Data

This directory contains sample data files for demonstrating ARIA functionality without requiring web scraping or external data sources.

## Directory Structure

### `/rfps/` - Sample RFP/RFI Documents
Place your sample RFP/RFI files here for testing the web application:

**Required files:**
- `agricultural_ai_rfp_example.html` - Main demo RFP for web app testing (HTML format)

**Format expectations:**
- CSV files should have columns like: `Question`, `Category`, `Required`, `Description`
- HTML files should contain structured question lists
- Files will be processed by ARIA's question extraction service

### `/knowledge_base/` - All Supporting Content
Consolidated directory for all content used by ARIA's knowledge base and response generation:

**Required files:**
- `databricks_qa_dataset.csv` - Historical Q&A responses (question, response, year)
- `dbdocs.csv` - Pre-generated Databricks documentation (url, title, chunk_index, text, headers_char_count, id, published_date)
- `databricks_blogs.csv` - Sample blog posts/articles from Databricks blog
- `product_keyword_mappings.csv` - Product keywords and terminology for search

**Optional files:**
- `template_responses.json` - Standard response templates
- `quality_examples.csv` - Good/bad response examples for training

## Technical Requirements

- **Databricks Runtime**: Tested on version 15.4
- **Compute**: Web scraping functionality may not work as expected on Serverless compute
- **Demo Mode**: Set `sample_data_mode=true` in notebook widgets to use local sample files (default)

## Demo vs Production Mode

### **Demo Mode** (`sample_data_mode=true` - Default)
- ✅ **Uses pre-generated CSV files**: Fast, reliable, no web scraping
- ✅ **Active data sources**: Databricks docs, Q&A dataset, blogs, keywords  
- ⏸️ **Disabled**: OSS docs (MLflow, Spark, Delta) and Neon docs
- 🚀 **Perfect for**: External demos, development, testing

### **Production Mode** (`sample_data_mode=false`)
- 🌐 **Runs live web scraping**: Fresh data from actual websites
- ✅ **Full data sources**: All documentation including OSS and Neon
- ⚠️ **Requirements**: Databricks Runtime 15.4, not Serverless compute
- 🏭 **Perfect for**: Production deployments, fresh data needs

## Data Requirements

### For Web App Demo:
- The demo RFP file `rfps/agricultural_ai_rfp_example.html`
- Must be processable by ARIA's question extraction
- Should contain realistic questions for agricultural AI use case

### For ETL Pipeline:
- Both directories should have core files
- Files should be realistic but anonymized
- Data should be consistent (same company names, products, etc.)

### For Model Training:
- Response data should cover various question categories
- Include both good and poor quality examples
- Ensure sufficient volume for meaningful training (50+ responses recommended)

## Usage in ARIA

1. **ETL Phase**: Notebooks will read from these directories instead of web scraping
2. **Web App**: Users can upload the sample RFPs to test the full workflow
3. **Model Training**: Sample responses will be used for training custom models
4. **Vector Search**: Knowledge base content will populate the RAG system

## File Format Notes

- **CSV files**: Use UTF-8 encoding, include headers
- **JSON files**: Well-formatted, consistent schema
- **HTML files**: Clean markup, structured content
- **Size limits**: Keep individual files under 10MB for demo purposes

## Anonymization

All sample data should be:
- ✅ Fictional company names and details
- ✅ Realistic but not real customer information  
- ✅ Generic product descriptions
- ✅ Safe for public GitHub repository sharing
