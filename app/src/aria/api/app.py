"""FastAPI app exposing ARIA services as HTTP endpoints.

This thin API wraps the existing Python services so a Node.js/React frontend
can interact with the system using clean JSON endpoints.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import os
import json
from datetime import datetime

from fastapi import FastAPI, File, Form, UploadFile, Request
from fastapi.responses import PlainTextResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from aria.services.document_checker import DocumentCheckerService
from aria.services.document_processor import DocumentProcessor
from aria.services.question_extraction import QuestionExtractionService
from aria.services.answer_generation import AnswerGenerationService
from aria.services.chat_service import ChatService
from aria.services.analytics_service import AnalyticsService
from aria.core.logging_config import get_logger, setup_logging

# Setup logging configuration - environment-based level
# Local: DEBUG for everything, Deployed: WARNING for errors only
log_level = os.getenv("LOG_LEVEL", "DEBUG" if os.getenv("NODE_ENV", "development") == "development" else "WARNING")
setup_logging(level=log_level)
logger = get_logger(__name__)


def _convert_audit_results_to_markdown(audit_results: List[Dict[str, Any]], original_text: str = "") -> str:
    """Convert audit results list to markdown format."""
    if not audit_results:
        return "# Audit Results\n\nNo issues found in the document."
    
    markdown_parts = ["# Audit Results"]
    
    # Summary
    total_items = len(audit_results)
    fact_items = [item for item in audit_results if item.get('verdict')]
    rule_items = [item for item in audit_results if item.get('status')]
    
    markdown_parts.append(f"\n## Summary\n")
    markdown_parts.append(f"- **Total items reviewed**: {total_items}")
    markdown_parts.append(f"- **Factual claims**: {len(fact_items)}")
    markdown_parts.append(f"- **Rule compliance**: {len(rule_items)}")
    
    # Fact Checks Table
    if fact_items:
        markdown_parts.append(f"\n## Fact Checks\n")
        markdown_parts.append("| Claim | Verdict | Explanation |")
        markdown_parts.append("|-------|---------|-------------|")
        
        for item in fact_items:
            claim = item.get('claim', '').replace('|', '\\|')[:100] + ('...' if len(item.get('claim', '')) > 100 else '')
            verdict = item.get('verdict', 'Unknown')
            explanation = item.get('explanation', '').replace('|', '\\|')[:150] + ('...' if len(item.get('explanation', '')) > 150 else '')
            markdown_parts.append(f"| {claim} | {verdict} | {explanation} |")
    
    # Rule Compliance Table
    if rule_items:
        markdown_parts.append(f"\n## Rule Compliance\n")
        markdown_parts.append("| Rule | Status | Explanation |")
        markdown_parts.append("|------|--------|-------------|")
        
        for item in rule_items:
            rule = item.get('rule', '').replace('|', '\\|')[:100] + ('...' if len(item.get('rule', '')) > 100 else '')
            status = item.get('status', 'Unknown')
            explanation = item.get('explanation', '').replace('|', '\\|')[:150] + ('...' if len(item.get('explanation', '')) > 150 else '')
            markdown_parts.append(f"| {rule} | {status} | {explanation} |")
    
    # Detailed Issues
    markdown_parts.append(f"\n## Detailed Issues\n")
    for i, item in enumerate(audit_results, 1):
        markdown_parts.append(f"### Issue {i}")
        
        if item.get('claim'):
            markdown_parts.append(f"**Claim**: {item['claim']}")
            markdown_parts.append(f"**Verdict**: {item.get('verdict', 'Unknown')}")
        
        if item.get('rule'):
            markdown_parts.append(f"**Rule**: {item['rule']}")
            markdown_parts.append(f"**Status**: {item.get('status', 'Unknown')}")
        
        markdown_parts.append(f"**Explanation**: {item.get('explanation', 'No explanation provided')}")
        markdown_parts.append("")  # Empty line
    
    return "\n".join(markdown_parts)


app = FastAPI(title="ARIA API", version="1.0.0")


# Allow common local dev origins by default; override with ARIA_CORS_ORIGINS
_default_origins = [
    "http://localhost:8000",  # Node.js frontend 
    "http://127.0.0.1:8000",
    # Note: Databricks Apps will automatically handle CORS for production
]
_env_origins = os.getenv("ARIA_CORS_ORIGINS", "")
origins = [o.strip() for o in _env_origins.split(",") if o.strip()] or _default_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class AuditRequest(BaseModel):
    doc_id: Optional[str] = None
    doc_name: Optional[str] = None
    text: str
    audit_categories: List[str]
    brand_guidelines: Optional[str] = None
    evidence: Optional[List[Dict[str, Any]]] = None
    model_name: Optional[str] = None


class AuditResponse(BaseModel):
    success: bool
    results_markdown: str
    info: Dict[str, Any]


@app.post("/api/audit", response_model=AuditResponse)
def audit(req: AuditRequest) -> AuditResponse:
    """Audit raw text using selected categories and optional guidelines."""
    service = DocumentCheckerService()
    success, audit_results, info = service.check_document(
        content=req.text,
        include_rules_markdown=req.brand_guidelines or "",
        only_fact_check=False,
        audit_categories=req.audit_categories,
        model_name=req.model_name or None,
    )
    
    # Convert audit results to markdown format
    results_markdown = _convert_audit_results_to_markdown(audit_results, req.text)
    
    # Add audit_items to info for frontend use
    info_with_results = info or {}
    info_with_results["audit_items"] = audit_results
    info_with_results["original_text"] = req.text
    
    return AuditResponse(
        success=success, 
        results_markdown=results_markdown, 
        info=info_with_results
    )


@app.post("/api/audit-file", response_model=AuditResponse)
async def audit_file(
    file: UploadFile = File(...),
    audit_categories: str = Form(...),  # JSON list as string
    brand_guidelines: Optional[str] = Form(None),
    model_name: Optional[str] = Form(None),
    doc_id: Optional[str] = Form(None),
    doc_name: Optional[str] = Form(None),
) -> AuditResponse:
    """Audit a CSV/HTML file by first extracting text then running the checker."""
    tmp_dir = os.path.join("/tmp", "aria_api")
    os.makedirs(tmp_dir, exist_ok=True)
    tmp_path = os.path.join(tmp_dir, file.filename)
    with open(tmp_path, "wb") as f:
        f.write(await file.read())

    processor = DocumentProcessor()
    prep = processor.read_text_from_any(tmp_path)
    if not prep.get("ready"):
        return AuditResponse(success=False, results_markdown="", info={"error": "File not supported or unreadable"})

    import json as _json

    try:
        cats: List[str] = _json.loads(audit_categories) if audit_categories else []
    except Exception:
        cats = []

    service = DocumentCheckerService()
    success, audit_results, info = service.check_document(
        content=prep.get("content", ""),
        include_rules_markdown=brand_guidelines or "",
        only_fact_check=False,
        audit_categories=cats,
        model_name=model_name or None,
    )
    
    # Convert audit results to markdown format
    document_content = prep.get("content", "")
    results_markdown = _convert_audit_results_to_markdown(audit_results, document_content)
    
    # Add audit_items to info for frontend use
    info_with_results = info or {}
    info_with_results["audit_items"] = audit_results
    info_with_results["original_text"] = document_content
    
    return AuditResponse(
        success=success, 
        results_markdown=results_markdown, 
        info=info_with_results
    )


class ProcessDocumentRequest(BaseModel):
    file_path: str
    document_name: str


class ProcessDocumentResponse(BaseModel):
    success: bool
    content: str
    ready: bool
    info: Dict[str, Any]


class ExtractQuestionsRequest(BaseModel):
    content: str
    document_name: str
    extraction_method: Optional[str] = "ai_extraction"
    custom_prompt: Optional[str] = ""
    model_name: Optional[str] = None


class ExtractQuestionsResponse(BaseModel):
    success: bool
    questions: List[Dict[str, Any]]
    info: Dict[str, Any]


class GenerateAnswersRequest(BaseModel):
    questions: List[Dict[str, Any]]
    settings: Dict[str, Any]
    is_regeneration: bool = False  # New field for explicit regeneration control
    document_name: str = "Unknown Document"  # Document name for consistent analytics


class GenerateAnswersResponse(BaseModel):
    success: bool
    answers: List[Dict[str, Any]]
    info: Dict[str, Any]


class ChatRequest(BaseModel):
    question: str
    context: Optional[List[Dict[str, Any]]] = None
    max_tokens: Optional[int] = 15000


class ChatResponse(BaseModel):
    success: bool
    answer: str
    sources: Optional[List[str]] = None
    info: Dict[str, Any]


class AnalyticsTrackingRequest(BaseModel):
    event_type: str  # "rfi_export", "chat_copy", etc.
    session_id: str
    # RFI Export fields
    document_name: Optional[str] = None
    export_format: Optional[str] = None  # "csv", "html", "pdf"
    total_questions: Optional[int] = None
    total_answers: Optional[int] = None
    # Chat fields
    chat_session_id: Optional[str] = None
    question_text: Optional[str] = None
    response_text: Optional[str] = None
    model_used: Optional[str] = None
    response_time_seconds: Optional[float] = None
    copied_to_clipboard: Optional[bool] = None


class AnalyticsTrackingResponse(BaseModel):
    success: bool
    info: Dict[str, Any] = {}


@app.post("/api/process-document", response_model=ProcessDocumentResponse)
def process_document(req: ProcessDocumentRequest, request: Request) -> ProcessDocumentResponse:
    """Process uploaded document and extract text content."""
    start_time = datetime.now()
    
    try:
        processor = DocumentProcessor()
        result = processor.read_text_from_any(req.file_path)
        
        # Track the upload event (non-blocking)
        try:
            import os
            file_size = os.path.getsize(req.file_path) if os.path.exists(req.file_path) else 0
            file_type = os.path.splitext(req.file_path)[1].lower()
            
            # Use user-entered document name for consistent analytics tracking
            analytics_service = AnalyticsService()
            analytics_service.track_rfi_upload(
                session_id=request.headers.get('X-Session-Id', 'unknown'),
                document_name=req.document_name,  # Use user-entered document name
                file_size_bytes=file_size,
                file_type=file_type,
                processing_method="document_processor",
                headers=dict(request.headers)
            )
        except Exception as analytics_error:
            logger.warning(f"Failed to track upload analytics: {analytics_error}")
        
        return ProcessDocumentResponse(
            success=result.get("ready", False),
            content=result.get("content", ""),
            ready=result.get("ready", False),
            info=result
        )
    except Exception as e:
        return ProcessDocumentResponse(
            success=False,
            content="",
            ready=False,
            info={"error": str(e)}
        )


@app.post("/api/extract-questions", response_model=ExtractQuestionsResponse)
def extract_questions(req: ExtractQuestionsRequest, request: Request) -> ExtractQuestionsResponse:
    """Extract questions from processed document content."""
    start_time = datetime.now()
    
    try:
        # Use provided extraction method or auto-detect
        extraction_method = req.extraction_method
        if extraction_method == "auto":
            extraction_method = "ai_extraction"  # Default to AI extraction
            if "Q1:" in req.content or "Question:" in req.content:
                extraction_method = "csv_direct"
        
        # Log model parameter for operational visibility
        logger.info(f"Extract questions request with model: {req.model_name or 'default'}")
        
        # Use the actual QuestionExtractionService
        service = QuestionExtractionService()
        success, questions, info = service.extract_questions(
            content=req.content,
            extraction_method=extraction_method,
            custom_prompt=req.custom_prompt or "",
            metadata={"document_name": req.document_name},
            model_name=req.model_name
        )
        
        # Track the extraction event (non-blocking)
        try:
            processing_time = (datetime.now() - start_time).total_seconds()
            
            analytics_service = AnalyticsService()
            # Count individual questions properly (including nested sub-questions)
            questions_count = 0
            
            if success and questions:
                # Use the already instantiated service to count questions
                questions_count = service._count_total_questions(questions)
            
            # Prepare questions data for structured tracking
            questions_data = questions if success and questions else None
                
            # Use user-entered document name for consistent analytics tracking
            analytics_service.track_rfi_extraction(
                session_id=request.headers.get('X-Session-Id', 'unknown'),
                document_name=req.document_name,  # Use user-entered document name consistently
                extraction_method=extraction_method,
                questions_extracted=questions_count,
                model_used=req.model_name or "default",
                custom_prompt=req.custom_prompt if req.custom_prompt and req.custom_prompt.strip() else None,
                processing_time_seconds=processing_time,
                questions_data=questions_data,
                headers=dict(request.headers)
            )
        except Exception as analytics_error:
            logger.warning(f"Failed to track extraction analytics: {analytics_error}")
        
        # Return questions exactly as they come from the extraction service (1:1)
        return ExtractQuestionsResponse(
            success=success,
            questions=questions if success else [],
            info=info
        )
    except Exception as e:
        return ExtractQuestionsResponse(
            success=False,
            questions=[],
            info={"error": str(e)}
        )


@app.post("/api/generate-answers", response_model=GenerateAnswersResponse)
def generate_answers(req: GenerateAnswersRequest, request: Request) -> GenerateAnswersResponse:
    """Generate AI answers for extracted questions using topic-based grouping."""
    start_time = datetime.now()
    
    try:
        # Log request for operational visibility
        logger.info(f"Answer generation request for {len(req.questions)} questions")
        
        # Use the actual AnswerGenerationService with the exact same flow as Streamlit
        service = AnswerGenerationService()
        
        # Use questions exactly as they come from extraction (1:1 with Streamlit flow)
        questions_for_service = req.questions
        
        # Use user-entered document name for consistent analytics tracking
        
        session_id = request.headers.get('X-Session-Id', 'unknown')
        
        # Generate answers using the service (handles topic grouping internally)
        success, answers, info = service.generate_answers(
            questions=questions_for_service,
            custom_prompt=req.settings.get("custom_prompt", ""),
            session_id=session_id,
            document_name=req.document_name,  # Use user-entered document name
            headers=dict(request.headers),
            is_regeneration=req.is_regeneration
        )
        
        # Note: Analytics tracking is now handled within the AnswerGenerationService
        # with question-level granularity and session aggregation
        
        # Return answers exactly as they come from the service (1:1 with Streamlit)
        return GenerateAnswersResponse(
            success=success,
            answers=answers if success else [],
            info=info
        )
    except Exception as e:
        return GenerateAnswersResponse(
            success=False,
            answers=[],
            info={"error": str(e)}
        )


@app.post("/api/generate-answers-stream")
def generate_answers_stream(req: GenerateAnswersRequest):
    """Generate AI answers with real progressive streaming - one question at a time."""
    from fastapi.responses import StreamingResponse
    import json
    from typing import Iterator
    
    def progress_generator() -> Iterator[str]:
        """Generate answers one by one and stream immediately."""
        try:
            service = AnswerGenerationService()
            questions = req.questions
            total_questions = len(questions)
            
            # Send start event
            yield f"data: {json.dumps({'type': 'start', 'total_questions': total_questions})}\n\n"
            
            completed_questions = 0
            failed_questions = 0
            
            # Process each question individually
            for i, question in enumerate(questions):
                try:
                    question_id = question.get('sub_question', question.get('question_id', f'Q{i+1}'))
                    question_text = question.get('text', question.get('question', ''))[:100]
                    
                    # Send progress update
                    progress_data = {
                        "type": "progress",
                        "current": i + 1,
                        "total": total_questions,
                        "question_id": question_id,
                        "status": f"Processing: {question_text}..."
                    }
                    yield f"data: {json.dumps(progress_data)}\n\n"
                    
                    # Process single question using individual processing
                    success, answers = service._generate_individual(
                        [question],  # Single question
                        req.settings.get("custom_prompt", ""),
                        service.settings.get_auth_headers()
                    )
                    
                    if success and answers:
                        completed_questions += 1
                        
                        # Send answer immediately
                        answer_data = {
                            "type": "answer",
                            "question_id": question_id,
                            "answer": answers[0],  # First (and only) answer
                            "progress": {
                                "completed": completed_questions,
                                "failed": failed_questions,
                                "total": total_questions,
                                "percentage": round((completed_questions / total_questions) * 100, 1)
                            }
                        }
                        yield f"data: {json.dumps(answer_data)}\n\n"
                        
                    else:
                        failed_questions += 1
                        
                        # Send error for this question
                        error_data = {
                            "type": "question_error",
                            "question_id": question_id,
                            "error": f"Failed to generate answer for question {question_id}",
                            "progress": {
                                "completed": completed_questions,
                                "failed": failed_questions,
                                "total": total_questions,
                                "percentage": round(((completed_questions + failed_questions) / total_questions) * 100, 1)
                            }
                        }
                        yield f"data: {json.dumps(error_data)}\n\n"
                        
                except Exception as e:
                    failed_questions += 1
                    logger.error(f"Error processing question {i+1}: {str(e)}")
                    
                    error_data = {
                        "type": "question_error",
                        "question_id": question.get('sub_question', f'Q{i+1}'),
                        "error": str(e),
                        "progress": {
                            "completed": completed_questions,
                            "failed": failed_questions,
                            "total": total_questions,
                            "percentage": round(((completed_questions + failed_questions) / total_questions) * 100, 1)
                        }
                    }
                    yield f"data: {json.dumps(error_data)}\n\n"
            
            # Send completion
            final_data = {
                "type": "complete",
                "success": True,
                "summary": {
                    "total": total_questions,
                    "completed": completed_questions,
                    "failed": failed_questions,
                    "success_rate": round((completed_questions / total_questions) * 100, 1) if total_questions > 0 else 0
                }
            }
            yield f"data: {json.dumps(final_data)}\n\n"
                
        except Exception as e:
            logger.error(f"Fatal error in answer generation: {str(e)}")
            error_data = {
                "type": "fatal_error",
                "error": str(e)
            }
            yield f"data: {json.dumps(error_data)}\n\n"
    
    return StreamingResponse(
        progress_generator(),
        media_type="text/plain",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Content-Type": "text/event-stream"
        }
    )


@app.post("/api/chat", response_model=ChatResponse)
def chat(req: ChatRequest, request: Request) -> ChatResponse:
    """Handle chat/question answering requests."""
    start_time = datetime.now()
    
    try:
        # Use the real ChatService to call Databricks Model Serving
        service = ChatService()
        
        success, answer, sources, chat_info = service.chat(
            question=req.question,
            context=req.context,
            model_name=None  # Use default model
        )
        
        # Track the chat interaction (non-blocking)
        try:
            response_time = (datetime.now() - start_time).total_seconds()
            
            # Generate a chat session ID from session or create one
            session_id = request.headers.get('X-Session-Id', 'unknown')
            chat_session_id = f"chat-{session_id}-{int(start_time.timestamp())}"
            
            # Prepare structured data for chat analytics
            question_data = {
                "text": req.question,
                "length_chars": len(req.question)
            }
            
            response_data = {
                "text": answer if success else "Error occurred",
                "length_chars": len(answer if success and answer else "Error occurred"),
                "model_used": chat_info.get('model_used', 'default'),
                "response_time_seconds": response_time,
                "copied_to_clipboard": False  # Will be updated later if user copies
            }
            
            analytics_service = AnalyticsService()
            analytics_service.track_chat_question(
                session_id=session_id,
                chat_session_id=chat_session_id,
                question_data=question_data,
                response_data=response_data,
                headers=dict(request.headers)
            )
        except Exception as analytics_error:
            logger.warning(f"Failed to track chat analytics: {analytics_error}")
        
        if success:
            return ChatResponse(
                success=True,
                answer=answer,
                sources=sources,
                info={
                    **chat_info,
                    "timestamp": datetime.now().isoformat(),
                    "response_type": "databricks_model_serving"
                }
            )
        else:
            return ChatResponse(
                success=False,
                answer=f"I'm sorry, I encountered an issue: {answer}",
                sources=sources,
                info={
                    **chat_info,
                    "timestamp": datetime.now().isoformat(),
                    "error": answer
                }
            )
        
    except Exception as e:
        return ChatResponse(
            success=False,
            answer=f"I'm sorry, I encountered an error while processing your question: {str(e)}",
            sources=None,
            info={
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            }
        )


@app.post("/api/analytics/track", response_model=AnalyticsTrackingResponse)
def track_analytics_event(req: AnalyticsTrackingRequest, request: Request) -> AnalyticsTrackingResponse:
    """Track frontend analytics events (exports, chat copies, etc.)."""
    try:
        # Extract headers for user identification
        headers = dict(request.headers)
        
        analytics_service = AnalyticsService()
        success = False
        
        if req.event_type == "rfi_export":
            success = analytics_service.track_rfi_export(
                session_id=req.session_id,
                document_name=req.document_name or "Unknown Document",
                export_format=req.export_format or "unknown",
                total_questions=req.total_questions or 0,
                total_answers=req.total_answers or 0,
                headers=headers
            )
        elif req.event_type == "chat_copy":
            # Prepare structured data for chat copy analytics
            question_data = {
                "text": req.question_text or "",
                "length_chars": len(req.question_text or "")
            }
            
            response_data = {
                "text": req.response_text or "",
                "length_chars": len(req.response_text or ""),
                "model_used": req.model_used or "unknown",
                "response_time_seconds": req.response_time_seconds or 0.0,
                "copied_to_clipboard": req.copied_to_clipboard or False
            }
            
            success = analytics_service.track_chat_question(
                session_id=req.session_id,
                chat_session_id=req.chat_session_id or "unknown",
                question_data=question_data,
                response_data=response_data,
                headers=headers
            )
        elif req.event_type == "chat_session":
            # Handle chat session tracking (session completion/summary)
            success = analytics_service.track_chat_session(
                session_id=req.session_id,
                chat_session_id=req.chat_session_id or "unknown",
                total_questions=getattr(req, 'total_questions', 0),
                total_responses=getattr(req, 'total_responses', 0),
                session_duration_seconds=getattr(req, 'session_duration_seconds', 0),
                final_status=getattr(req, 'final_status', 'completed'),
                headers=headers
            )
        else:
            logger.warning(f"Unknown analytics event type: {req.event_type}")
            return AnalyticsTrackingResponse(
                success=False,
                info={"error": f"Unknown event type: {req.event_type}"}
            )
        
        return AnalyticsTrackingResponse(
            success=success,
            info={"event_type": req.event_type, "tracked": success}
        )
        
    except Exception as e:
        logger.warning(f"Failed to track analytics event: {e}")
        return AnalyticsTrackingResponse(
            success=False,
            info={"error": str(e)}
        )


@app.get("/healthz")
def healthz() -> Dict[str, str]:
    """Health check endpoint for readiness probes."""
    return {"status": "ok"}


@app.get("/debug/config")
def debug_config() -> Dict[str, Any]:
    """Debug endpoint to check configuration status."""
    import os
    from aria.config import settings
    
    # Check environment detection
    is_databricks_apps = bool(os.getenv('DATABRICKS_CLIENT_ID'))
    
    # Test authentication
    auth_headers = settings.get_auth_headers()
    has_auth = bool(auth_headers)
    
    config_info = {
        "environment": "Databricks Apps" if is_databricks_apps else "Local Development",
        "auth_headers_available": has_auth,
        "databricks_host": settings.databricks.host,
        "warehouse_id_set": bool(settings.databricks.warehouse_id),
        "question_extraction_model": settings.question_extraction_model,
        "answer_generation_model": settings.answer_generation_model,
        "document_checker_model": settings.document_checker_model,
        "chat_model": settings.chat_model,
        "env_vars_present": {
            "DATABRICKS_HOST": bool(os.getenv('DATABRICKS_HOST')),
            "DATABRICKS_CLIENT_ID": bool(os.getenv('DATABRICKS_CLIENT_ID')),
            "DATABRICKS_TOKEN": bool(os.getenv('DATABRICKS_TOKEN')),
            "DATABRICKS_WAREHOUSE_ID": bool(os.getenv('DATABRICKS_WAREHOUSE_ID')),
        },
        "services_configured": len(settings.services),
        "tracking_enabled": settings.tracking.enabled,
        "debug_mode": settings.app.debug,
    }
    
    # Test model endpoint URL construction
    try:
        model_endpoint = settings.databricks.get_model_endpoint_url(settings.question_extraction_model)
        config_info["model_endpoint"] = model_endpoint
        config_info["model_endpoint_reachable"] = "unknown"  # Would need actual HTTP test
    except Exception as e:
        config_info["model_endpoint_error"] = str(e)
    
    return config_info


@app.get("/debug/headers")
def debug_headers(request: Request) -> Dict[str, Any]:
    """Debug endpoint to show all received headers and user info extraction."""
    headers_dict = dict(request.headers)
    
    # Extract user info using our analytics method
    from aria.services.analytics_service import AnalyticsService
    analytics = AnalyticsService()
    user_info = analytics._get_user_info_from_headers(headers_dict)
    
    return {
        "environment": "local" if not any(k.lower().startswith('x-forwarded') for k in headers_dict.keys()) else "databricks_app",
        "all_headers": headers_dict,
        "user_info_extracted": user_info,
        "databricks_headers": {
            k: v for k, v in headers_dict.items() 
            if k.lower().startswith(('x-forwarded', 'x-real'))
        },
        "user_related_headers": {
            k: v for k, v in headers_dict.items() 
            if any(term in k.lower() for term in ['user', 'forwarded', 'real', 'ip'])
        },
        "header_count": len(headers_dict),
        "expected_databricks_headers": [
            "X-Forwarded-User",
            "X-Forwarded-Email", 
            "X-Forwarded-Preferred-Username",
            "X-Real-Ip"
        ]
    }


@app.post("/debug/test-extraction")
def test_extraction() -> Dict[str, Any]:
    """Test question extraction with minimal content to debug issues."""
    from aria.services.question_extraction import QuestionExtractionService
    
    test_content = "Q1: What is artificial intelligence? Q2: How does machine learning work?"
    
    try:
        service = QuestionExtractionService()
        
        # Test auth headers
        auth_headers = service.settings.get_auth_headers()
        
        result = {
            "test_content": test_content,
            "auth_headers_available": bool(auth_headers),
            "model_name": service.settings.question_extraction_model,
            "timeout": service.timeout,
        }
        
        # Test extraction
        success, questions, info = service.extract_questions(
            content=test_content,
            extraction_method="ai_extraction",
            custom_prompt="Just extract these two questions as JSON.",
            metadata={"test": True}
        )
        
        result.update({
            "extraction_success": success,
            "questions_found": len(questions) if questions else 0,
            "extraction_info": info,
            "questions_preview": questions[:2] if questions else [],
        })
        
        return result
        
    except Exception as e:
        return {
            "error": str(e),
            "error_type": type(e).__name__,
            "test_content": test_content,
        }


@app.post("/debug/streaming-test")
def streaming_test() -> Dict[str, Any]:
    """Specific test to prove if streaming is working."""
    from aria.services.question_extraction import QuestionExtractionService
    
    try:
        extraction_service = QuestionExtractionService()
        
        # Simple test content
        test_content = "This is a test document. Q1: Does streaming work? Q2: How many chunks do we get?"
        
        # Call extraction with explicit model (same params as HTTP API)
        success, questions, info = extraction_service.extract_questions(
            content=test_content,
            extraction_method="ai_extraction",
            custom_prompt="",
            metadata={"document_name": "streaming-test.html"},
            model_name="databricks-claude-sonnet-4"
        )
        
        return {
            "streaming_test_success": success,
            "questions_count": len(questions),
            "streaming_used": info.get("streaming_used", False),
            "has_streaming_info": "streaming_used" in info,
            "info_keys": list(info.keys()),
            "full_info": info,
            "direct_service_call": True
        }
        
    except Exception as e:
        return {
            "streaming_test_success": False,
            "error": str(e)
        }

@app.get("/metrics")
def metrics() -> PlainTextResponse:
    """Metrics endpoint for monitoring (Databricks Apps infrastructure)."""
    metrics_content = """# HELP aria_requests_total Total number of requests
# TYPE aria_requests_total counter
aria_requests_total 1

# HELP aria_up Service is up and running  
# TYPE aria_up gauge
aria_up 1

# HELP aria_info Information about the service
# TYPE aria_info gauge
aria_info{version="1.0.0",service="aria-backend"} 1
"""
    return PlainTextResponse(
        content=metrics_content,
        media_type="text/plain; version=0.0.4; charset=utf-8"
    )


# To run locally:
# uvicorn aria.api.app:app --reload --host 0.0.0.0 --port 8000


