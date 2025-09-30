# Development Guide

This guide covers the development setup and workflow for the ARIA application.

## Architecture Overview

ARIA uses a hybrid architecture:
- **Frontend**: Node.js with Express.js serving static HTML/CSS/JavaScript
- **Backend**: Python FastAPI exposing AI services as REST endpoints
- **AI Services**: Databricks Model Serving for LLM interactions

## Project Structure

```
aria-accelerator/
├── app.py                    # Python FastAPI backend launcher
├── app.js                    # Node.js Express frontend server
├── package.json              # Node.js dependencies
├── requirements.txt          # Python dependencies
├── pyproject.toml           # Python project configuration
├── static/                  # Frontend static assets
│   ├── css/main.css         # Main stylesheet
│   ├── js/app.js           # Frontend JavaScript utilities
│   ├── images/             # Logo, icons, assets
│   ├── index.html          # RFI upload page
│   ├── extract.html        # Question extraction page
│   ├── generate.html       # Answer generation page
│   ├── download.html       # Results download page
│   ├── audit.html          # Document audit page
│   └── chat.html           # Chat interface
├── src/aria/               # Python backend services
│   ├── api/                # FastAPI application
│   │   └── app.py         # API routes and handlers
│   ├── config/             # Configuration management
│   │   ├── constants.py    # Application constants
│   │   ├── settings.py     # Settings and environment handling
│   │   └── backend_services.yaml  # AI model configurations
│   ├── core/               # Core utilities
│   │   ├── exceptions.py   # Custom exceptions
│   │   ├── logging_config.py # Logging configuration
│   │   └── types.py        # Type definitions
│   ├── services/           # Business logic services
│   │   ├── answer_generation.py    # AI answer generation
│   │   ├── chat_service.py         # Chat functionality
│   │   ├── document_checker.py     # Document auditing
│   │   ├── document_processor.py   # Document processing
│   │   ├── lakebase_service.py     # Databricks integration
│   │   └── question_extraction.py  # AI question extraction
│   └── utils/              # Utility functions
├── uploads/                # Temporary file storage (created at runtime)
└── tmp/                   # Processing artifacts (created at runtime)
```

## Development Setup

### Prerequisites
- Python 3.9+
- Node.js 16+
- Access to Databricks workspace with Model Serving

### Environment Setup

1. **Clone and setup the repository**
   ```bash
   git clone <repository-url>
   cd aria
   ```

2. **Python environment**
   ```bash
   # Create virtual environment (recommended)
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   
   # Install Python dependencies
   pip install -r requirements.txt
   
   # Install development dependencies
   pip install -e ".[dev]"
   ```

3. **Node.js environment**
   ```bash
   # Install Node.js dependencies
   npm install
   ```

4. **Environment configuration**
   
   Create a `.env` file:
   ```bash
   # Databricks Configuration
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
   DATABRICKS_TOKEN=your_personal_access_token
   
   # Development Settings
   NODE_ENV=development
   PORT=8000
   DEBUG=true
   ```

### Running the Application

For development, you need to run both the Python backend and Node.js frontend:

```bash
# Terminal 1: Python FastAPI backend (default port 8080)
python app.py

# Terminal 2: Node.js frontend (default port 8000)
node app.js
```

The application will be available at `http://localhost:8000`

### API Development

The FastAPI backend is located in `aria-accelerator/src/aria/api/app.py`. Key endpoints:

- `POST /api/upload` - Upload and process documents
- `POST /api/extract-questions` - Extract questions from documents
- `POST /api/generate-answers` - Generate answers for extracted questions
- `POST /api/audit` - Audit document content
- `POST /api/chat` - Chat with AI assistant

API documentation is available at `http://localhost:8080/docs` when the backend is running.

### Frontend Development

The frontend uses vanilla JavaScript with static HTML pages. Key files:

- `static/js/app.js` - Core utilities and API communication
- `static/css/main.css` - Main stylesheet with responsive design
- `static/*.html` - Individual page templates

### Configuration Management

AI model configurations are managed in `aria-accelerator/src/aria/config/backend_services.yaml`:

```yaml
question_extraction:
  model: "databricks-claude-sonnet-4"
  prompt: |
    Your prompt here...
    
answer_generation:
  model: "databricks-claude-sonnet-4"
  prompt: |
    Your prompt here...
```

## Testing

### Python Backend Tests

```bash
# Run all tests
python -m pytest src/tests/

# Run with coverage
python -m pytest src/tests/ --cov=src.aria --cov-report=html

# Run specific test module
python -m pytest src/tests/unit/test_services/
```

### Frontend Testing

Currently, frontend testing is manual. Future enhancements may include:
- Jest for JavaScript unit tests
- Playwright for end-to-end tests

## Code Quality

### Python

```bash
# Format code
ruff format src/

# Lint code
ruff check src/

# Type checking
mypy src/aria

# Run all quality checks
ruff check src/ && ruff format --check src/ && mypy src/aria
```

### JavaScript

The project uses vanilla JavaScript. Consider adding:
- ESLint for linting
- Prettier for formatting

## Debugging

### Backend Debugging

1. **Enable debug mode**
   ```bash
   export DEBUG=true
   python app.py
   ```

2. **View logs**
   - FastAPI logs appear in terminal
   - Application logs are configured in `aria-accelerator/src/aria/core/logging_config.py`

3. **API documentation**
   - Visit `http://localhost:8080/docs` for interactive API docs
   - Use `/api/health` endpoint to test connectivity

### Frontend Debugging

1. **Browser DevTools**
   - Open browser DevTools (F12)
   - Check Console for JavaScript errors
   - Monitor Network tab for API requests

2. **Session debugging**
   - Session data is stored in the Node.js server memory
   - Check terminal logs for session management

## Performance Considerations

### Backend
- FastAPI is async-capable for concurrent requests
- Consider connection pooling for Databricks SDK
- Monitor memory usage during large document processing

### Frontend
- Static assets are served by Express.js
- Large documents are processed server-side
- Consider implementing file upload progress indicators

## Deployment Preparation

### Environment Variables
Ensure all required environment variables are set:
- `DATABRICKS_HOST`
- `DATABRICKS_TOKEN`
- `NODE_ENV=production`

### Build Process
```bash
# Ensure dependencies are up to date
pip install -r requirements.txt
npm install

# Run tests
python -m pytest src/tests/

# Check for security vulnerabilities
npm audit
```

### Docker (Optional)
Consider containerizing for consistent deployments:
```dockerfile
# Example Dockerfile structure
FROM node:18-slim
# Install Python
# Copy and install dependencies
# Expose ports
# Start both services
```

## Contributing

1. **Code Style**
   - Python: Follow PEP 8, use type hints
   - JavaScript: Use consistent naming, add comments
   - HTML/CSS: Semantic markup, mobile-first design

2. **Testing**
   - Add tests for new Python services
   - Manual testing for frontend changes
   - Test API endpoints with different inputs

3. **Documentation**
   - Update README.md for user-facing changes
   - Update this guide for development changes
   - Add docstrings for new Python functions

4. **Pull Request Process**
   - Create feature branch from main
   - Ensure all tests pass
   - Update documentation as needed
   - Request review before merging