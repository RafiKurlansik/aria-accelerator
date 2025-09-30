# ARIA Application Layer

This directory contains the complete ARIA web application - a dual-architecture system combining a **Node.js/Express frontend** with a **Python/FastAPI backend** to deliver the Analyst Relations Intelligent Assistant.

## 🏗️ **Architecture Overview**

ARIA uses a **hybrid architecture** designed for both local development and Databricks Apps deployment:

```
┌─────────────────────────────────────────────────────────────┐
│                    ARIA Web Application                     │
├─────────────────────────────────────────────────────────────┤
│  Frontend (Node.js/Express)     │  Backend (Python/FastAPI) │
│  Port: 8000                     │  Port: 8080               │
│                                 │                           │
│  • Static HTML/CSS/JS           │  • AI Services            │
│  • File Upload Handling        │  • Document Processing    │
│  • API Proxy                   │  • LLM Integration        │
│  • User Interface              │  • Analytics Tracking     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Databricks Platform                      │
│  • Unity Catalog  • Vector Search  • Model Serving        │
└─────────────────────────────────────────────────────────────┘
```

### Why This Architecture?

1. **Separation of Concerns**: Frontend handles UI/UX, backend handles AI/ML logic
2. **Scalability**: Each layer can be scaled independently
3. **Development Flexibility**: Frontend and backend teams can work in parallel
4. **Databricks Integration**: Python backend seamlessly integrates with Databricks services
5. **Modern Web Standards**: Node.js frontend provides responsive, interactive UI

## 📁 **Directory Structure**

```
app/
├── 🚀 Entry Points
│   ├── app.js              # Node.js Express server (Frontend)
│   ├── app.py              # Python FastAPI launcher (Backend)
│   ├── start.sh            # Development startup script
│   └── app.yaml            # Databricks Apps configuration
│
├── 📦 Dependencies
│   ├── package.json        # Node.js dependencies
│   ├── package-lock.json   # Node.js lockfile
│   └── requirements.txt    # Python dependencies
│
├── 🐍 Python Backend (src/aria/)
│   ├── api/
│   │   └── app.py          # FastAPI application with all endpoints
│   ├── config/
│   │   ├── backend_services.yaml    # AI model configurations
│   │   ├── constants.py             # Application constants
│   │   └── unified_config.py        # Configuration management
│   ├── core/
│   │   ├── base_service.py          # Base service class
│   │   ├── error_handler.py         # Error handling utilities
│   │   ├── exceptions.py            # Custom exceptions
│   │   ├── logging_config.py        # Logging configuration
│   │   ├── logging_utils.py         # Logging utilities
│   │   └── types.py                 # Type definitions
│   ├── services/
│   │   ├── analytics_service.py     # Usage analytics tracking
│   │   ├── answer_generation.py     # AI answer generation
│   │   ├── chat_service.py          # Chat functionality
│   │   ├── document_checker.py      # Document auditing
│   │   ├── document_processor.py    # File processing
│   │   └── question_extraction.py   # Question parsing
│   └── utils/               # Utility functions
│
├── 🌐 Frontend Assets (static/)
│   ├── *.html              # Page templates (index, chat, extract, etc.)
│   ├── css/
│   │   └── main.css        # Application styles
│   ├── js/
│   │   ├── app.js          # Main frontend JavaScript
│   │   ├── analytics.js    # Analytics tracking
│   │   └── error-handler.js # Error handling
│   └── images/
│       ├── icons/          # ARIA icons
│       └── logos/          # Databricks logos
│
└── 🧪 Testing
    ├── test_imports.py     # Import validation
    └── tests/              # Comprehensive test suite
        ├── unit/           # Unit tests for all components
        └── integration/    # Integration tests
```

## 🚀 **How to Run the Application**

### Option 1: Quick Start (Recommended)
```bash
# From the app/ directory
./start.sh
```

### Option 2: Manual Startup
```bash
# Terminal 1: Start Python backend
python app.py

# Terminal 2: Start Node.js frontend  
node app.js
```

### Option 3: Development Mode
```bash
# Concurrent development with auto-reload
npm run dev
```

### Option 4: Individual Services
```bash
# Backend only
npm run start:backend

# Frontend only  
npm run start:frontend
```

## 🔌 **API Endpoints**

The application exposes these key endpoints:

### Core Functionality
- `POST /api/upload` - File upload and processing
- `POST /api/extract-questions` - Extract questions from documents
- `POST /api/generate-answers` - Generate AI responses
- `POST /api/chat` - Interactive chat with AI assistant
- `POST /api/audit` - Document auditing and analysis

### System Endpoints
- `GET /api/health` - Application health check
- `GET /api/healthz` - Kubernetes-style health check
- `POST /api/analytics/track` - Usage analytics tracking

### Debug Endpoints (Development)
- `GET /api/debug/config` - Configuration debugging
- `POST /api/debug/test-extraction` - Test question extraction
- `POST /api/debug/streaming-test` - Test streaming responses

## 🎯 **Key Features**

### 📄 **Document Processing**
- **Multi-format Support**: HTML, CSV, and text documents
- **Intelligent Parsing**: AI-powered question extraction
- **Progress Tracking**: Real-time processing status
- **Error Handling**: Graceful failure recovery

### 🤖 **AI Integration**
- **RAG Pipeline**: Retrieval-Augmented Generation for accurate responses
- **Multiple Models**: Support for various LLM providers
- **Streaming Responses**: Real-time answer generation
- **Context Awareness**: Maintains conversation history

### 🔍 **Document Auditing**
- **Content Analysis**: Factual accuracy verification
- **Brand Compliance**: Messaging consistency checks
- **Visual Feedback**: Highlighted issues with explanations
- **Export Options**: Multiple output formats

### 📊 **Analytics & Monitoring**
- **Usage Tracking**: Detailed analytics on user interactions
- **Performance Metrics**: Response times and success rates
- **Error Monitoring**: Comprehensive error logging
- **Health Checks**: System status monitoring

## ⚙️ **Configuration**

### Environment Variables
Key configuration options (see `env.example` in project root):

```bash
# Server Configuration
NODE_ENV=development
HOST=0.0.0.0
BACKEND_PORT=8080
PORT=8000

# Databricks Integration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-token

# Application Settings
APP_DEBUG=true
APP_DEVELOPMENT_MODE=true
LOG_LEVEL=DEBUG
```

### Model Configuration
AI models are configured in `src/aria/config/backend_services.yaml`:
- Question extraction models
- Answer generation models  
- Chat completion models
- Embedding models

## 🧪 **Testing**

### Run All Tests
```bash
# From app/ directory
python -m pytest tests/ -v
```

### Test Categories
- **Unit Tests**: Individual component testing
- **Integration Tests**: End-to-end workflow testing
- **Import Tests**: Dependency validation

### Test Import Validation
```bash
python test_imports.py
```

## 🔧 **Development Notes**

### Frontend (Node.js/Express)
- **Modern JavaScript**: ES6+ modules with import/export
- **File Handling**: Multer for multipart uploads
- **API Integration**: Axios for backend communication
- **Markdown Support**: Marked.js for content rendering

### Backend (Python/FastAPI)
- **Async Support**: Full async/await pattern
- **Type Safety**: Pydantic models for request/response validation
- **Service Architecture**: Modular service-based design
- **Error Handling**: Comprehensive exception management

### Deployment Considerations
- **Databricks Apps**: Configured via `app.yaml`
- **Process Management**: Automatic subprocess handling
- **Health Monitoring**: Built-in health check endpoints
- **Logging**: Structured logging with configurable levels

## 🚨 **Troubleshooting**

### Common Issues

**Backend not starting:**
```bash
# Check Python path
echo $PYTHONPATH
# Should include: /path/to/app/src

# Verify imports
python test_imports.py
```

**Frontend can't reach backend:**
```bash
# Check if backend is running
curl http://localhost:8080/healthz

# Verify ARIA_API_BASE environment variable
echo $ARIA_API_BASE
```

**File upload issues:**
- Check `uploads/` directory permissions
- Verify `APP_MAX_FILE_SIZE_MB` setting
- Ensure sufficient disk space

### Debug Mode
Enable debug logging:
```bash
export LOG_LEVEL=DEBUG
export APP_DEBUG=true
```

## 📚 **Additional Resources**

- **Main Documentation**: See project root `README.md`
- **Deployment Guide**: `/docs/deployment.md`
- **Development Guide**: `/docs/development.md`
- **Analytics Overview**: `/docs/analytics.md`

---

**Built with ❤️ by the PM Technical team at Databricks**
