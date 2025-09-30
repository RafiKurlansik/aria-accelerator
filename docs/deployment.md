# ARIA Deployment Guide

This guide covers deployment options for the ARIA application.

## Architecture Overview

ARIA uses a hybrid architecture:
- **Frontend**: Node.js with Express.js (Port 8000)
- **Backend**: Python FastAPI (Port 8080)
- **AI Services**: Databricks Model Serving

## Deployment Options

### 1. Local Development

#### Prerequisites
- Python 3.9+
- Node.js 16+
- Databricks workspace access
- Personal Access Token

#### Setup
1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   npm install
   ```

2. **Configure environment**
   Create `.env` file:
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your_personal_access_token
   NODE_ENV=development
   PORT=8000
   DEBUG=true
   ```

3. **Start both services**
   ```bash
   # Terminal 1: Python FastAPI backend
   python app.py
   
   # Terminal 2: Node.js frontend
   node app.js
   ```

4. **Access application**
   Open `http://localhost:8000`

### 2. Databricks Apps Deployment

#### Prerequisites
- Databricks workspace with Apps feature enabled
- Databricks CLI configured
- Service Principal with appropriate permissions

#### Deployment Steps

1. **Prepare deployment package**
```bash
   # Ensure all dependencies are specified
   cat requirements.txt
   cat package.json

   # Verify app.yaml configuration
cat app.yaml
```

2. **Deploy to Databricks Apps**
   ```bash
   # Create new app
   databricks apps create aria
   
   # Deploy from current directory
   databricks apps deploy aria --source-path .
   
   # Check deployment status
   databricks apps status aria
   ```

3. **Monitor deployment**
   ```bash
   # View logs
   databricks apps logs aria
   
   # Check app URL
   databricks apps get aria
   ```

#### Databricks Apps Configuration

The `app.yaml` file configures the deployment:

```yaml
name: aria
version: 1.0.0

# Start both Python and Node.js services
command: 
  - python app.py &
  - node app.js

env:
  NODE_ENV: production
  PORT: 8000
  PYTHONPATH: /app/src

resources:
  cpu: 2
  memory: 2GB

health_check:
  path: "/"
  timeout: 30
```

### 3. Docker Deployment (Optional)

For containerized deployment:

#### Dockerfile
```dockerfile
FROM node:18-slim

# Install Python
RUN apt-get update && apt-get install -y python3 python3-pip

# Set working directory
WORKDIR /app

# Copy package files
COPY package.json requirements.txt ./

# Install dependencies
RUN npm install
RUN pip3 install -r requirements.txt

# Copy application code
COPY . .

# Set Python path
ENV PYTHONPATH=/app/src

# Expose ports
EXPOSE 8000 8080

# Start both services
CMD ["sh", "-c", "python3 app.py & node app.js"]
```

#### Build and run
```bash
# Build image
docker build -t aria .

# Run container
docker run -p 8000:8000 -p 8080:8080 \
  -e DATABRICKS_HOST=your-host \
  -e DATABRICKS_TOKEN=your-token \
  aria
```

## Environment Configuration

### Required Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `DATABRICKS_HOST` | Databricks workspace URL | `https://workspace.cloud.databricks.com` |
| `DATABRICKS_TOKEN` | Personal access token | `dapi12345...` |

### Optional Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `NODE_ENV` | Node.js environment | `development` |
| `PORT` | Frontend port | `8000` |
| `DEBUG` | Enable debug mode | `false` |
| `PYTHONPATH` | Python module path | `./src` (local), `/app/src` (container) |

### Databricks Apps Environment

In Databricks Apps, authentication is handled automatically:
- Service Principal credentials are provided
- No need to specify `DATABRICKS_TOKEN`
- Set `NODE_ENV=production`

## Health Checks

### Frontend Health Check
```bash
curl http://localhost:8000/
```

### Backend Health Check
```bash
curl http://localhost:8080/api/health
```

### API Status Check
```bash
curl -X POST http://localhost:8080/api/chat \
  -H "Content-Type: application/json" \
  -d '{"message": "test", "session_id": "health-check"}'
```

## Monitoring and Troubleshooting

### Log Locations

**Local Development:**
- Frontend logs: Terminal running `node app.js`
- Backend logs: Terminal running `python app.py`

**Databricks Apps:**
```bash
# View app logs
databricks apps logs aria

# Stream live logs
databricks apps logs aria --follow
```

### Common Issues

1. **Port conflicts**
   - Ensure ports 8000 and 8080 are available
   - Check with `lsof -i :8000` and `lsof -i :8080`

2. **Authentication errors**
   - Verify `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
   - Test with: `databricks clusters list`

3. **File upload issues**
   - Check `uploads/` directory permissions
   - Verify disk space availability

4. **Model serving errors**
   - Verify model endpoints are active in Databricks
   - Check `src/aria/config/backend_services.yaml` model names

### Performance Tuning

1. **Backend optimization**
   - Adjust FastAPI worker processes
   - Implement connection pooling
   - Monitor memory usage

2. **Frontend optimization**
   - Enable gzip compression
   - Optimize static asset serving
   - Implement caching headers

## Security Considerations

### Local Development
- Use `.env` file for secrets (not committed to git)
- Restrict network access to development ports

### Production Deployment
- Use Service Principal authentication
- Enable HTTPS only
- Implement proper CORS policies
- Regular security updates

### Databricks Apps Security
- Service Principal permissions are automatically managed
- Network isolation provided by Databricks
- Built-in security scanning

## Backup and Recovery

### Critical Data
- Configuration: `src/aria/config/backend_services.yaml`
- Static assets: `static/` directory
- Application code: Full repository

### Recovery Procedures
1. **Code recovery**: Git repository restore
2. **Configuration restore**: Backup YAML files
3. **Redeploy**: Use Databricks CLI commands

## Updates and Maintenance

### Rolling Updates
```bash
# Deploy new version
databricks apps deploy aria --source-path .

# Verify deployment
databricks apps status aria

# Rollback if needed
databricks apps rollback aria --version previous
```

### Dependency Updates
```bash
# Update Python dependencies
pip install -r requirements.txt --upgrade

# Update Node.js dependencies  
npm update

# Test locally before deploying
python -m pytest src/tests/
```

### Model Updates
1. Update model configurations in `backend_services.yaml`
2. Test with development environment
3. Deploy to Databricks Apps
4. Monitor for any issues