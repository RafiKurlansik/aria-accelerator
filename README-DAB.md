# Databricks Asset Bundle (DAB) for ARIA

This directory contains the Databricks Asset Bundle configuration for the ARIA (Analyst Relations Intelligent Assistant) project.

## Overview

The ARIA DAB includes:
- **Web Application**: Node.js frontend + Python FastAPI backend
- **ETL Pipelines**: Lakeflow jobs for data processing
- **ML Jobs**: Model deployment and batch inference
- **Model Serving**: Question extraction, answer generation, and document auditing endpoints
- **Evaluation**: Automated model quality assessment

## Quick Start

### Prerequisites
- Databricks CLI installed and configured
- Access to Databricks workspace with permissions for:
  - Model Serving
  - Jobs
  - Apps
  - Unity Catalog
  - Vector Search

### Initial Setup

1. **Install Databricks CLI**
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   ```

2. **Configure CLI**
   ```bash
   databricks configure
   ```

3. **Validate Configuration**
   ```bash
   databricks bundle validate
   ```

### Deployment

#### 🚀 One-Click Setup (Recommended for New Users)
```bash
# Deploy the bundle
databricks bundle deploy --target development

# Run complete setup (everything in correct order)
databricks bundle run aria-complete-setup --target development
```

**That's it!** This single job will:
1. ✅ Process all sample data (ETL pipeline)
2. ✅ Create vector search indexes
3. ✅ Deploy and configure AI agents
4. ✅ Create model serving endpoints
5. ✅ Make ARIA ready to use

#### Advanced: Individual Jobs (For Debugging/Development)
```bash
# Deploy to development
databricks bundle deploy --target development

# Run individual components (if needed)
databricks bundle run demo-etl-pipeline --target development
databricks bundle run create-vector-search-indexes --target development
databricks bundle run demo-model-processing --target development
databricks bundle run deploy-audit-agent --target development

# Deploy model serving endpoints
databricks bundle run rfi-processor-deployment --target development
```

#### Production Environment
```bash
# Deploy to production
databricks bundle deploy --target production

# Monitor deployment
databricks bundle status --target production
```

## Resource Components

### 1. Databricks Apps
- **aria-web-app**: Main web application with hybrid Node.js/Python architecture

### 2. Jobs

#### ETL Pipeline (`demo-etl-pipeline`)
Sequential data processing workflow:
1. `01a_responses_etl.py` - Process RFI responses  
2. `01b_docs_etl.py` - Process documentation
3. `01c_product_keywords_etl.py` - Process product keywords
4. `01d_blogs_etl.py` - Process blog content

**Schedule**: Manual trigger for demo purposes

#### Vector Search (`vector-search-index-job`)
- **Purpose**: Create and maintain vector search indices
- **Notebook**: `01_create_vs_idx.py`
- **Trigger**: Manual (run as needed)

#### Model Processing (`demo-model-processing`)
- **Purpose**: Demonstrate RFI processing capabilities
- **Notebook**: `rfi_processor.py`
- **Trigger**: Manual (for demo purposes)

#### Batch Processing (`batch-inference-job`)
- **Purpose**: Large-scale batch inference
- **Notebook**: `06_batch_inference.py`
- **Schedule**: Weekly on Sundays at 6 AM UTC

#### Model Evaluation (`demo-evaluation`)
- **Purpose**: Demonstrate model evaluation workflow
- **Notebook**: `Evaluate_ARIA_vs_Submission.py`
- **Trigger**: Manual (for demo purposes)

### 3. Model Serving Endpoints
- **aria-question-extraction**: Question extraction from documents
- **aria-answer-generation**: Answer generation for RFI responses
- **aria-document-audit**: Document auditing and compliance checking

### 4. Clusters
- **aria-development-cluster**: Shared development cluster with auto-termination

## Target Configuration

### Development
- Workspace path: `/Workspace/Users/${workspace.current_user.userName}/aria-dev`
- Purpose: Development and testing
- Resources: Smaller clusters, auto-termination enabled

### Staging  
- Workspace path: `/Workspace/Users/${workspace.current_user.userName}/aria-staging`
- Purpose: Pre-production testing
- Resources: Production-like sizing

### Production
- Workspace path: `/Workspace/Users/${workspace.current_user.userName}/aria-prod`
- Purpose: Live production deployment
- Resources: Optimized for performance and reliability

## Variables

Key configurable variables:
- `catalog_name`: Unity Catalog for ARIA data (default: "users")
- `schema_name`: Schema for ARIA tables (default: "rafi_kurlansik")
- `vector_search_endpoint`: Vector search endpoint name
- Model names for different services

## File Structure

```
aria-accelerator/
├── databricks.yml           # Main DAB configuration
├── README-DAB.md            # This documentation
├── src/aria-accelerator/notebooks/      # Databricks notebooks
│   ├── etl/                 # ETL workflows (Lakeflow)
│   ├── agents/              # Model and agent notebooks
│   ├── batch/               # Batch processing
│   └── evaluation/          # Model evaluation
├── src/aria-accelerator/               # Python application code
├── static/                 # Frontend assets
└── app.yaml               # Databricks Apps config
```

## Common Commands

### Bundle Management
```bash
# Validate configuration
databricks bundle validate

# Deploy to environment
databricks bundle deploy --target <environment>

# View bundle status
databricks bundle status --target <environment>

# Destroy resources (careful!)
databricks bundle destroy --target <environment>
```

### Job Management  
```bash
# Run specific job
databricks bundle run <job-name> --target <environment>

# List all jobs
databricks jobs list

# View job run history
databricks jobs runs list --job-id <job-id>
```

### Monitoring
```bash
# View app logs
databricks apps logs aria-web-application

# Check model serving endpoint status
databricks serving-endpoints get aria-answer-generation-endpoint

# Monitor job execution
databricks jobs runs get <run-id>
```

## Customization

### Adding New Jobs
1. Add job definition to `databricks.yml` under `resources.jobs`
2. Include required notebook paths
3. Configure cluster specifications
4. Set scheduling if needed

### Target-Specific Overrides
Create target-specific variable files:
- `targets/development.yml`
- `targets/staging.yml`
- `targets/production.yml`

### Security
- Service principals recommended for production
- Use Databricks Secrets for sensitive configuration
- Configure appropriate workspace permissions

## Troubleshooting

### Common Issues
1. **Permission Errors**: Ensure service principal has required permissions
2. **Path Issues**: Verify notebook paths exist in workspace
3. **Resource Limits**: Check workspace compute quotas
4. **Dependencies**: Ensure all required libraries are available

### Debug Commands
```bash
# Validate bundle with verbose output
databricks bundle validate --debug

# Check workspace permissions
databricks workspace whoami

# Test model serving endpoints
databricks serving-endpoints get <endpoint-name>
```

## Support

For issues or questions:
1. Check Databricks documentation
2. Review bundle validation output
3. Contact workspace administrators for permission issues
