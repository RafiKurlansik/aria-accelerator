"""Answer generation service for ARIA application.

This module handles generating responses to questions using AI models,
with support for both individual and batch processing.
"""

import re
import time
from typing import Dict, List, Optional, Any, Tuple, Callable
import requests
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from aria.core.logging_config import get_logger
from aria.core.base_service import BaseService
from aria.core.error_handler import error_handler
from aria.core.exceptions import TemporaryServiceError
from aria.config import settings
from aria.services.analytics_service import AnalyticsService
from aria.config.constants import (
    DEFAULT_GENERATION_PROMPT,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_RETRIES,
    RETRY_WAIT_SECONDS
)

logger = get_logger(__name__)


class AnswerGenerationService(BaseService):
    """Service for generating answers to questions using AI."""
    
    def __init__(self) -> None:
        """Initialize the answer generation service."""
        super().__init__()
        # Use longer timeout for cold start scenarios - model serving endpoints scale from zero
        self.timeout = 600  # 10 minutes to handle cold starts
    
    def generate_answers(
        self,
        questions: List[Dict[str, Any]],
        custom_prompt: str = "",
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        session_id: Optional[str] = None,
        document_name: str = "Unknown Document",
        headers: Optional[Dict[str, str]] = None,
        is_regeneration: bool = False
    ) -> Tuple[bool, List[Dict[str, Any]], Dict[str, Any]]:
        """Generate answers for a list of questions.
        
        Args:
            questions: List of question dictionaries
            custom_prompt: Custom prompt for answer generation
            progress_callback: Optional callback for progress updates (current, total, status)
            session_id: Session ID for analytics tracking
            document_name: Document name for analytics tracking
            headers: HTTP headers for analytics tracking
            is_regeneration: Whether this is a user-initiated regeneration (increments version)
            
        Returns:
            Tuple of (success, answers_list, generation_info)
        """
        generation_info = self._create_info_dict("answer_generation")
        generation_info.update({
            "questions_processed": 0,
            "answers_generated": 0,
        })
        
        # Initialize analytics tracking
        analytics_service = AnalyticsService()
        logger.info(f"🔧 [DEBUG] Analytics service initialized: {analytics_service}")
        
        # Get the correct session version (increments for regenerations)
        session_version = self._get_next_session_version(session_id, analytics_service, is_regeneration)
        
        session_analytics = {
            "session_id": session_id,
            "document_name": document_name,
            "headers": headers,
            "analytics_service": analytics_service,
            "session_version": session_version,  # Properly versioned for regenerations
            "custom_prompt": custom_prompt if custom_prompt and custom_prompt.strip() else None,
            "generation_rounds": [],
            "question_attempts": {},
            "final_questions_state": []
        }
        
        try:
            start_time = time.time()
            
            # Check if we have authentication configured
            auth_success, auth_headers = self._check_authentication(generation_info)
            if not auth_success:
                return False, [], generation_info
            
            # Use enhanced generation with comprehensive tracking and retries
            logger.info(f"Starting answer generation for {len(questions)} questions")
            success, answers = self._generate_with_tracking(questions, custom_prompt, auth_headers, progress_callback, generation_info, session_analytics)
            
            generation_info["processing_time"] = time.time() - start_time
            generation_info["questions_processed"] = len(questions)
            generation_info["answers_generated"] = len(answers) if answers else 0
            
            # Critical validation: Ensure we have an answer for every question
            success = self._validate_complete_answers(questions, answers, generation_info)
            
            if success:
                logger.info(f"✅ Successfully generated {len(answers)} answers for {len(questions)} questions in {generation_info['processing_time']:.2f} seconds")
            else:
                logger.warning(f"❌ Answer generation incomplete: {len(answers)} answers for {len(questions)} questions")
                if len(answers) < len(questions):
                    missing_count = len(questions) - len(answers)
                    generation_info["errors"].append(f"Missing {missing_count} answers - some questions were not processed")
            
            # Session-level analytics no longer tracked - using batch-level tracking only
            logger.info(f"🔧 [DEBUG] Session completed with {len(answers)} answers generated")
            
            return success, answers, generation_info
            
        except Exception as e:
            logger.error(f"Error in answer generation: {str(e)}")
            generation_info["errors"].append(f"Generation error: {str(e)}")
            return False, [], generation_info
    
    def _generate_with_tracking(
        self,
        questions: List[Dict[str, Any]],
        custom_prompt: str,
        auth_headers: Dict[str, str],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        generation_info: Dict[str, Any] = None,
        session_analytics: Dict[str, Any] = None
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """Generate answers with comprehensive tracking, retries, and validation.
        
        Args:
            questions: List of question dictionaries
            custom_prompt: Custom prompt for generation
            auth_headers: Authentication headers
            progress_callback: Progress callback function
            generation_info: Generation info dictionary for tracking
            
        Returns:
            Tuple of (success, answers_list)
        """
        try:
            # Initialize tracking structures
            question_tracker = {
                'total_questions': len(questions),
                'processed_questions': set(),
                'failed_questions': [],
                'answers': {},
                'retry_attempts': 0
            }
            
            # Determine processing method
            if self._has_hierarchical_structure(questions):
                logger.info("Using topic-based batch processing with retry logic")
                if generation_info:
                    generation_info["method"] = "topic_batch_with_retry"
                success = self._generate_by_topics_with_retry(
                    questions, custom_prompt, auth_headers, progress_callback, question_tracker, session_analytics
                )
            else:
                logger.info("Using individual question processing with retry logic")
                if generation_info:
                    generation_info["method"] = "individual_with_retry"
                success = self._generate_individual_with_retry(
                    questions, custom_prompt, auth_headers, progress_callback, question_tracker, session_analytics
                )
            
            # Convert answers dict to list, maintaining order
            answers_list = []
            for question in questions:
                question_id = self._get_question_id(question)
                if question_id in question_tracker['answers']:
                    answers_list.append(question_tracker['answers'][question_id])
                else:
                    # Create placeholder for any remaining missing questions
                    placeholder_answer = {
                        "question_id": question_id,
                        "question_text": question.get('text', question.get('Question', '')),
                        "answer": "❌ Failed to generate answer after multiple retries",
                        "topic": question.get('topic', 'Unknown'),
                        "error": True
                    }
                    answers_list.append(placeholder_answer)
                    question_tracker['failed_questions'].append(question_id)
            
            # Update generation info
            if generation_info:
                generation_info["retry_attempts"] = question_tracker['retry_attempts']
                generation_info["failed_questions"] = question_tracker['failed_questions']
                generation_info["success_rate"] = (len(answers_list) - len(question_tracker['failed_questions'])) / len(questions) * 100
            
            logger.info(f"Question tracking summary: {len(answers_list)} total answers, {len(question_tracker['failed_questions'])} failed questions")
            
            return True, answers_list
            
        except Exception as e:
            logger.error(f"Error in _generate_with_tracking: {str(e)}")
            return False, []
    
    def _validate_complete_answers(
        self, 
        questions: List[Dict[str, Any]], 
        answers: List[Dict[str, Any]], 
        generation_info: Dict[str, Any]
    ) -> bool:
        """Validate that we have complete answers for all questions.
        
        Args:
            questions: Original questions
            answers: Generated answers
            generation_info: Generation info for tracking
            
        Returns:
            True if all questions have answers, False otherwise
        """
        if len(answers) != len(questions):
            logger.warning(f"Answer count mismatch: {len(answers)} answers for {len(questions)} questions")
            generation_info["validation_error"] = f"Expected {len(questions)} answers, got {len(answers)}"
            return False
        
        # Check for placeholder/error answers
        error_answers = [ans for ans in answers if ans.get('error', False) or 'Failed to generate answer' in ans.get('answer', '')]
        
        if error_answers:
            logger.warning(f"Found {len(error_answers)} error/placeholder answers")
            generation_info["error_answers"] = len(error_answers)
            # Still return True if we have *some* answer for every question, even if some are errors
            # This prevents the system from completely failing when only a few questions fail
        
        logger.info(f"✅ Validation passed: {len(answers)} answers for {len(questions)} questions")
        return True
    
    def _get_question_id(self, question: Dict[str, Any]) -> str:
        """Extract a consistent question ID from a question dictionary.
        
        Args:
            question: Question dictionary
            
        Returns:
            Question ID string
        """
        return question.get('sub_question', question.get('id', question.get('ID', question.get('question_id', 'unknown'))))
    
    def _has_hierarchical_structure(self, questions: List[Dict[str, Any]]) -> bool:
        """Check if questions have hierarchical structure (topics, sub-questions).
        
        Args:
            questions: List of question dictionaries
            
        Returns:
            True if hierarchical structure is detected
        """
        if not questions:
            return False
        
        # Check if questions have the expected hierarchical fields
        sample_question = questions[0]
        required_fields = ['question', 'topic', 'sub_question', 'text']
        
        return all(field in sample_question for field in required_fields)
    
    def _generate_by_topics(
        self,
        questions: List[Dict[str, Any]],
        custom_prompt: str,
        auth_headers: Dict[str, str],
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """Generate answers by grouping questions by topic.
        
        Args:
            questions: List of question dictionaries
            custom_prompt: Custom prompt for generation
            auth_headers: Authentication headers (will be refreshed as needed)
            progress_callback: Progress callback function
            
        Returns:
            Tuple of (success, answers_list)
        """
        try:
            # Group questions by topic
            df = pd.DataFrame(questions)
            grouped_df = self._group_questions_by_topic(df)
            
            total_topics = len(grouped_df)
            all_answers = []
            
            for idx, row in grouped_df.iterrows():
                if progress_callback:
                    model_name = self.settings.answer_generation_model
                    progress_callback(idx + 1, total_topics, f"Calling {model_name} for topic: {row['topic']}")
                
                # Get fresh auth headers for each topic to handle potential token expiration
                # This is especially important for long-running batch processes
                current_auth_headers = self.settings.get_auth_headers()
                if not current_auth_headers:
                    logger.error("Authentication headers are no longer available")
                    break
                
                # Generate answers for this topic
                success, topic_answers, error_info = self._generate_topic_answers(
                    row, custom_prompt, current_auth_headers
                )
                
                if success:
                    all_answers.extend(topic_answers)
                else:
                    error_msg = f"Failed to generate answers for topic: {row['topic']}"
                    if error_info:
                        error_msg += f" - {error_info.get('error_message', 'Unknown error')}"
                    logger.warning(error_msg)
            
            return True, all_answers
            
        except Exception as e:
            logger.error(f"Error in topic-based generation: {str(e)}")
            return False, []
    
    def _generate_individual(
        self,
        questions: List[Dict[str, Any]],
        custom_prompt: str,
        auth_headers: Dict[str, str],
        progress_callback: Optional[Callable[[int, int, str], None]] = None
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """Generate answers for individual questions.
        
        Args:
            questions: List of question dictionaries
            custom_prompt: Custom prompt for generation
            auth_headers: Authentication headers (will be refreshed as needed)
            progress_callback: Progress callback function
            
        Returns:
            Tuple of (success, answers_list)
        """
        try:
            total_questions = len(questions)
            answers = []
            
            for idx, question in enumerate(questions):
                if progress_callback:
                    question_preview = question.get('text', question.get('Question', ''))[:50]
                    model_name = self.settings.answer_generation_model
                    progress_callback(idx + 1, total_questions, f"Calling {model_name}: {question_preview}...")
                
                # Get fresh auth headers for each question to handle potential token expiration
                current_auth_headers = self.settings.get_auth_headers()
                if not current_auth_headers:
                    logger.error("Authentication headers are no longer available")
                    break
                
                # Generate answer for this question
                success, answer = self._generate_single_answer(
                    question, custom_prompt, current_auth_headers
                )
                
                if success:
                    answers.append(answer)
                else:
                    # Add a placeholder answer for failed questions
                    answers.append({
                        "question_id": question.get('id', f"Q{idx+1}"),
                        "question_text": question.get('text', question.get('Question', '')),
                        "answer": "Error: Failed to generate answer",
                        "topic": question.get('topic', 'Unknown')
                    })
            
            return True, answers
            
        except Exception as e:
            logger.error(f"Error in individual generation: {str(e)}")
            return False, []
    
    def _generate_by_topics_with_retry(
        self,
        questions: List[Dict[str, Any]],
        custom_prompt: str,
        auth_headers: Dict[str, str],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        question_tracker: Dict[str, Any] = None,
        session_analytics: Dict[str, Any] = None
    ) -> bool:
        """Generate answers by topics with automatic retry for failed topics.
        
        Args:
            questions: List of question dictionaries
            custom_prompt: Custom prompt for generation
            auth_headers: Authentication headers
            progress_callback: Progress callback function
            question_tracker: Question tracking dictionary
            
        Returns:
            True if processing completed (even with some failures)
        """
        try:
            # Group questions by topic
            df = pd.DataFrame(questions)
            grouped_df = self._group_questions_by_topic(df)
            
            total_topics = len(grouped_df)
            max_retries = 3
            
            # Track topics and their questions
            topic_question_map = {}
            for idx, row in grouped_df.iterrows():
                topic = row['topic']
                topic_questions = row['json_payload']['questions']
                topic_question_map[topic] = topic_questions
            
            # Process each topic with retry logic
            for idx, row in grouped_df.iterrows():
                topic = row['topic']
                topic_questions = topic_question_map[topic]
                
                if progress_callback:
                    model_name = self.settings.answer_generation_model
                    progress_callback(idx + 1, total_topics, f"Processing topic: {topic} ({len(topic_questions)} questions)")
                
                # Try to process this topic with retries
                topic_success = False
                retry_count = 0
                
                while not topic_success and retry_count < max_retries:
                    attempt_start_time = time.time()
                    try:
                        # Get fresh auth headers for each attempt
                        current_auth_headers = self.settings.get_auth_headers()
                        if not current_auth_headers:
                            logger.error("Authentication headers are no longer available")
                            break
                        
                        # Generate answers for this topic
                        success, topic_answers, error_info = self._generate_topic_answers(
                            row, custom_prompt, current_auth_headers
                        )
                        
                        if success and topic_answers:
                            # Store answers in tracker
                            attempt_processing_time = time.time() - attempt_start_time
                            
                            for answer in topic_answers:
                                question_id = answer.get('question_id')
                                if question_id:
                                    question_tracker['answers'][question_id] = answer
                                    question_tracker['processed_questions'].add(question_id)
                            
                            # Track this batch/topic immediately
                            if session_analytics and session_analytics.get("analytics_service"):
                                self._track_topic_batch(
                                    session_analytics, topic, topic_questions, topic_answers,
                                    retry_count + 1, "success", attempt_processing_time, None
                                )
                            
                            topic_success = True
                            logger.info(f"✅ Successfully processed topic '{topic}' with {len(topic_answers)} answers")
                        else:
                            retry_count += 1
                            question_tracker['retry_attempts'] += 1
                            attempt_processing_time = time.time() - attempt_start_time
                            
                            # Enhanced error logging with error_info details
                            error_details = f"❌ Topic '{topic}' failed, attempt {retry_count}/{max_retries}"
                            if error_info:
                                error_details += f" - {error_info.get('error_type', 'unknown')}: {error_info.get('error_message', 'No details')}"
                                if not error_info.get('is_retryable', True) and retry_count < max_retries:
                                    logger.warning(f"Topic '{topic}' failed with non-retryable error, skipping remaining retries")
                                    retry_count = max_retries  # Skip remaining retries for non-retryable errors
                            logger.warning(error_details)
                            
                            # Track failed batch/topic with detailed error info
                            failure_reason = error_info.get('error_message', f"Topic '{topic}' processing failed") if error_info else f"Topic '{topic}' processing failed"
                            if session_analytics and session_analytics.get("analytics_service"):
                                self._track_topic_batch(
                                    session_analytics, topic, topic_questions, [],
                                    retry_count, "failed", attempt_processing_time, failure_reason
                                )
                            
                            if retry_count < max_retries:
                                # Wait before retry
                                time.sleep(2 ** retry_count)  # Exponential backoff
                    
                    except Exception as e:
                        retry_count += 1
                        question_tracker['retry_attempts'] += 1
                        attempt_processing_time = time.time() - attempt_start_time
                        logger.error(f"Exception processing topic '{topic}', attempt {retry_count}/{max_retries}: {str(e)}")
                        
                        # Track failed batch/topic due to exception
                        if session_analytics and session_analytics.get("analytics_service"):
                            self._track_topic_batch(
                                session_analytics, topic, topic_questions, [],
                                retry_count, "failed", attempt_processing_time, str(e)
                            )
                        
                        if retry_count < max_retries:
                            time.sleep(2 ** retry_count)
                
                # If topic failed after all retries, create placeholder answers
                if not topic_success:
                    logger.error(f"❌ Topic '{topic}' failed after {max_retries} retries - creating placeholder answers")
                    for question in topic_questions:
                        question_id = question.get('question_id')
                        if question_id and question_id not in question_tracker['processed_questions']:
                            placeholder_answer = {
                                "question_id": question_id,
                                "question_text": question.get('text', ''),
                                "answer": f"❌ Topic '{topic}' failed to process after {max_retries} retries",
                                "topic": topic,
                                "error": True
                            }
                            question_tracker['answers'][question_id] = placeholder_answer
                            question_tracker['failed_questions'].append(question_id)
            
            logger.info(f"Topic-based processing complete: {len(question_tracker['processed_questions'])} successful, {len(question_tracker['failed_questions'])} failed")
            return True
            
        except Exception as e:
            logger.error(f"Error in _generate_by_topics_with_retry: {str(e)}")
            return False
    
    def _generate_individual_with_retry(
        self,
        questions: List[Dict[str, Any]],
        custom_prompt: str,
        auth_headers: Dict[str, str],
        progress_callback: Optional[Callable[[int, int, str], None]] = None,
        question_tracker: Dict[str, Any] = None,
        session_analytics: Dict[str, Any] = None
    ) -> bool:
        """Generate answers for individual questions with automatic retry for failures.
        
        Args:
            questions: List of question dictionaries
            custom_prompt: Custom prompt for generation
            auth_headers: Authentication headers
            progress_callback: Progress callback function
            question_tracker: Question tracking dictionary
            
        Returns:
            True if processing completed (even with some failures)
        """
        try:
            total_questions = len(questions)
            max_retries = 3
            
            for idx, question in enumerate(questions):
                question_id = self._get_question_id(question)
                question_preview = question.get('text', question.get('Question', ''))[:50]
                
                if progress_callback:
                    model_name = self.settings.answer_generation_model
                    progress_callback(idx + 1, total_questions, f"Processing: {question_preview}...")
                
                # Try to process this question with retries
                question_success = False
                retry_count = 0
                
                while not question_success and retry_count < max_retries:
                    attempt_start_time = time.time()
                    attempt_number = retry_count + 1
                    
                    try:
                        # Get fresh auth headers for each attempt
                        current_auth_headers = self.settings.get_auth_headers()
                        if not current_auth_headers:
                            logger.error("Authentication headers are no longer available")
                            break
                        
                        # Generate answer for this question
                        success, answer = self._generate_single_answer(
                            question, custom_prompt, current_auth_headers
                        )
                        
                        # Calculate processing time for this attempt
                        attempt_processing_time = time.time() - attempt_start_time
                        
                        # Individual question tracking no longer used - using batch-level tracking only
                        logger.debug(f"Question {question_id} attempt {attempt_number}: {'success' if success else 'failed'}")
                        
                        if success and answer:
                            # Store answer in tracker
                            question_tracker['answers'][question_id] = answer
                            question_tracker['processed_questions'].add(question_id)
                            question_success = True
                            logger.debug(f"✅ Successfully processed question {question_id}")
                        else:
                            retry_count += 1
                            question_tracker['retry_attempts'] += 1
                            logger.warning(f"❌ Question {question_id} failed, attempt {retry_count}/{max_retries}")
                            
                            if retry_count < max_retries:
                                # Wait before retry
                                time.sleep(1 + retry_count)  # Linear backoff for individual questions
                    
                    except Exception as e:
                        retry_count += 1
                        question_tracker['retry_attempts'] += 1
                        attempt_processing_time = time.time() - attempt_start_time
                        
                        # Individual question tracking no longer used - using batch-level tracking only
                        logger.debug(f"Question {question_id} attempt {attempt_number} failed: {str(e)}")
                        
                        logger.error(f"Exception processing question {question_id}, attempt {retry_count}/{max_retries}: {str(e)}")
                        
                        if retry_count < max_retries:
                            time.sleep(1 + retry_count)
                
                # If question failed after all retries, create placeholder answer
                if not question_success:
                    logger.error(f"❌ Question {question_id} failed after {max_retries} retries - creating placeholder answer")
                    placeholder_answer = {
                        "question_id": question_id,
                        "question_text": question.get('text', question.get('Question', '')),
                        "answer": f"❌ Failed to generate answer after {max_retries} retries",
                        "topic": question.get('topic', 'Unknown'),
                        "error": True
                    }
                    question_tracker['answers'][question_id] = placeholder_answer
                    question_tracker['failed_questions'].append(question_id)
            
            logger.info(f"Individual processing complete: {len(question_tracker['processed_questions'])} successful, {len(question_tracker['failed_questions'])} failed")
            return True
            
        except Exception as e:
            logger.error(f"Error in _generate_individual_with_retry: {str(e)}")
            return False
    
    # Individual question attempt tracking removed - now using batch-level tracking only
    
    def _track_topic_batch(
        self,
        session_analytics: Dict[str, Any],
        topic_name: str,
        topic_questions: List[Dict[str, Any]],
        topic_answers: List[Dict[str, Any]],
        attempt_number: int,
        batch_status: str,
        processing_time: float,
        error_message: Optional[str]
    ) -> None:
        """Track individual topic/batch processing.
        
        Args:
            session_analytics: Session analytics tracking data
            topic_name: Name of the topic/batch
            topic_questions: Questions sent to AI for this topic
            topic_answers: Answers received from AI for this topic
            attempt_number: Attempt number for this batch
            batch_status: Status (success, failed, partial)
            processing_time: Time taken for this batch
            error_message: Error message if batch failed
        """
        try:
            analytics_service = session_analytics.get("analytics_service")
            if not analytics_service or not session_analytics.get("session_id"):
                return
            
            # Prepare questions data for batch tracking
            questions_data = []
            for q in topic_questions:
                questions_data.append({
                    "question_id": q.get('question_id', ''),
                    "text": q.get('text', ''),
                    "topic": topic_name
                })
            
            # Prepare answers data for batch tracking
            answers_data = []
            for answer in topic_answers:
                answers_data.append({
                    "question_id": answer.get('question_id', ''),
                    "question_text": answer.get('question_text', ''),
                    "answer": answer.get('answer', ''),
                    "topic": topic_name,
                    "references": answer.get('references', [])
                })
            
            # Track the batch
            analytics_service.track_generation_batch(
                session_id=session_analytics["session_id"],
                session_version=session_analytics.get("session_version", 1),
                topic_name=topic_name,
                batch_attempt_number=attempt_number,
                batch_status=batch_status,
                batch_processing_time_seconds=processing_time,
                model_used=self.settings.answer_generation_model or "default",
                custom_prompt=session_analytics.get("custom_prompt"),
                questions_in_batch=questions_data,
                answers_in_batch=answers_data,
                document_name=session_analytics.get("document_name", "Unknown"),
                error_message=error_message,
                headers=session_analytics.get("headers")
            )
            
            logger.info(f"📊 [ANALYTICS BATCH] Tracked batch: {session_analytics['session_id']} topic={topic_name} status={batch_status} questions={len(questions_data)} answers={len(answers_data)}")
            
        except Exception as e:
            logger.warning(f"Failed to track topic batch analytics: {e}")
    
    # Session-level generation tracking removed - now using batch-level tracking only
    
    def _get_next_session_version(self, session_id: str, analytics_service, is_regeneration: bool = False) -> int:
        """Get the session version - simple and predictable.
        
        Args:
            session_id: Session identifier to check
            analytics_service: Analytics service instance (not used anymore)
            is_regeneration: Whether this is an explicit regeneration (increments version)
            
        Returns:
            Session version (1 for new sessions, incremented only for explicit regenerations)
        """
        try:
            if not session_id or session_id == 'unknown':
                return 1
            
            # Query the batches table to find the highest existing version for this session
            workspace_client = analytics_service.workspace_client
            warehouse_id = analytics_service.settings.databricks.warehouse_id
            
            result = workspace_client.statement_execution.execute_statement(
                statement=f"""
                SELECT COALESCE(MAX(session_version), 0) as max_version 
                FROM {analytics_service.rfi_generation_batches_table} 
                WHERE session_id = '{session_id}'
                """,
                warehouse_id=warehouse_id,
                wait_timeout='10s'
            )
            
            if (hasattr(result, 'result') and hasattr(result.result, 'data_array') and 
                result.result.data_array):
                current_max = int(result.result.data_array[0][0] or 0)
                
                if is_regeneration:
                    # Only increment for explicit regenerations
                    version = current_max + 1
                    logger.info(f"🔄 [SESSION VERSION] Regeneration requested for {session_id}, using version {version}")
                else:
                    # Use existing version or 1 for new sessions
                    version = max(current_max, 1)
                    if current_max > 0:
                        logger.info(f"🔄 [SESSION VERSION] Continuing session {session_id}, using existing version {version}")
                    else:
                        logger.info(f"🆕 [SESSION VERSION] New session {session_id}, using version {version}")
                
                return version
            else:
                logger.info(f"🆕 [SESSION VERSION] New session {session_id}, using version 1")
                return 1
                
        except Exception as e:
            logger.warning(f"Failed to get session version for {session_id}: {e}. Defaulting to version 1")
            return 1
    
    def _group_questions_by_topic(self, df: pd.DataFrame) -> pd.DataFrame:
        """Group questions by topic for JSON-based processing.
        
        Args:
            df: DataFrame with questions
            
        Returns:
            DataFrame grouped by topic with JSON payload
        """
        try:
            # Group by topic and create JSON payloads
            grouped_data = []
            
            for topic, group in df.groupby('topic'):
                # Create questions array for this topic
                questions = []
                for _, row in group.iterrows():
                    questions.append({
                        "question_id": row.get('sub_question', ''),
                        "text": row.get('text', ''),
                        "topic": topic
                    })
                
                # Create JSON payload for this topic
                json_payload = {
                    "topic": topic,
                    "questions": questions
                }
                
                grouped_data.append({
                    'topic': topic,
                    'question_count': len(questions),
                    'json_payload': json_payload,
                    'original_questions': group.to_dict('records')
                })
            
            grouped_df = pd.DataFrame(grouped_data)
            logger.info(f"Grouped {len(df)} questions into {len(grouped_df)} topics with JSON payloads")
            return grouped_df
            
        except Exception as e:
            logger.error(f"Error grouping questions by topic: {str(e)}")
            return pd.DataFrame()
    
    def _generate_topic_answers(
        self,
        topic_row: pd.Series,
        custom_prompt: str,
        auth_headers: Dict[str, str]
    ) -> Tuple[bool, List[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Generate answers for all questions in a topic using JSON.
        
        Args:
            topic_row: Row from grouped DataFrame containing JSON payload
            custom_prompt: Custom prompt for generation
            auth_headers: Authentication headers
            
        Returns:
            Tuple of (success, answers_list, error_info)
        """
        error_info = None
        try:
            topic = topic_row['topic']
            json_payload = topic_row['json_payload']
            
            # Add custom instructions to the JSON payload
            if custom_prompt and custom_prompt.strip():
                json_payload['custom_instructions'] = custom_prompt
            
            # Build prompt for this topic
            system_prompt = self._build_generation_prompt_json()
            user_prompt = self._build_json_user_prompt(json_payload)
            
            # Make API call
            success, response_text = self._call_generation_api(system_prompt, user_prompt, auth_headers)
            
            if not success:
                error_info = {
                    "topic": topic,
                    "error_type": "api_call_failed",
                    "error_message": "API call failed for topic",
                    "is_retryable": True,
                    "questions_affected": len(json_payload.get('questions', []))
                }
                return False, [], error_info
            
            # Parse the JSON response
            answers = self._parse_json_response(response_text, topic)
            
            return True, answers, None
            
        except TemporaryServiceError as e:
            # This is a retryable error (timeout, 504, etc.)
            error_info = {
                "topic": topic_row.get('topic', 'Unknown'),
                "error_type": "temporary_service_error",
                "error_message": str(e),
                "is_retryable": True,
                "questions_affected": len(topic_row.get('json_payload', {}).get('questions', []))
            }
            logger.warning(f"Temporary service error for topic '{topic_row.get('topic')}': {str(e)}")
            raise  # Re-raise to trigger retry
        except Exception as e:
            error_info = {
                "topic": topic_row.get('topic', 'Unknown'),
                "error_type": "unexpected_error",
                "error_message": str(e),
                "is_retryable": False,
                "questions_affected": len(topic_row.get('json_payload', {}).get('questions', []))
            }
            logger.error(f"Unexpected error generating topic answers: {str(e)}")
            return False, [], error_info
    
    def _generate_single_answer(
        self,
        question: Dict[str, Any],
        custom_prompt: str,
        auth_headers: Dict[str, str]
    ) -> Tuple[bool, Dict[str, Any]]:
        """Generate answer for a single question.
        
        Args:
            question: Question dictionary
            custom_prompt: Custom prompt for generation
            auth_headers: Authentication headers
            
        Returns:
            Tuple of (success, answer_dict)
        """
        try:
            question_text = question.get('text', question.get('Question', ''))
            question_id = question.get('sub_question', question.get('id', question.get('ID', 'Q1')))
            
            # Build prompt for this question
            system_prompt = self._build_generation_prompt()
            user_prompt = self._build_single_user_prompt(question_text, custom_prompt)
            
            # Make API call
            success, response_text = self._call_generation_api(system_prompt, user_prompt, auth_headers)
            
            if not success:
                return False, {}
            
            # Create answer dictionary
            answer = {
                "question_id": question_id,
                "question_text": question_text,
                "answer": response_text.strip(),
                "topic": question.get('topic', 'General')
            }
            
            return True, answer
            
        except Exception as e:
            logger.error(f"Error generating single answer: {str(e)}")
            return False, {}
    
    def _build_generation_prompt(self) -> str:
        """Build the system prompt for answer generation (legacy text-based).
        
        Returns:
            System prompt for AI
        """
        # Use configured prompt if available, otherwise fallback to hardcoded prompt
        # Get custom prompt from service config if available
        service_config = self.settings.get_service_config('answer_generation')
        configured_prompt = service_config.prompt.strip() if service_config and service_config.prompt else DEFAULT_GENERATION_PROMPT.strip()
        if configured_prompt:
            return configured_prompt
        
        # Fallback to existing hardcoded prompt
        return """You are Claude, an AI assistant specialized in analyst relations serving as an expert on enterprise software solutions. 

Your task is to generate informative, concise, and accurate responses to analyst questions about our product capabilities.

Follow these specific guidelines:
- Focus on providing direct answers based only on the information in our product capabilities
- Use confident, professional language focusing on strengths without overselling
- Be truthful and accurate about our capabilities
- Use clear, structured responses with bullet points where appropriate
- Highlight unique differentiators but avoid marketing language
- Present answers in 2-5 paragraphs typically
- Format using Markdown for clear structure"""
    
    def _build_generation_prompt_json(self) -> str:
        """Build the system prompt for JSON-based answer generation.
        
        Returns:
            System prompt for AI
        """
        return """You are Claude, an AI assistant specialized in analyst relations serving as an expert on enterprise software solutions.

Your task is to generate informative, concise, and accurate responses to analyst questions about our product capabilities.

INPUT FORMAT: You will receive a JSON object with the following structure:
{
  "topic": "Topic Name",
  "questions": [
    {
      "question_id": "1.01",
      "text": "Question text here?",
      "topic": "Topic Name"
    }
  ],
  "custom_instructions": "Optional additional context"
}

OUTPUT FORMAT: You must respond with ONLY valid JSON in exactly this structure:
{
  "topic": "Topic Name",
  "answers": [
    {
      "question_id": "1.01",
      "question_text": "Question text here?",
      "answer": "Your detailed answer here...",
      "references": ["https://docs.example.com/feature1", "Documentation link 2"]
    }
  ]
}

ANSWER GUIDELINES:
- Focus on providing direct answers based only on the information in our product capabilities
- Use confident, professional language focusing on strengths without overselling
- Be truthful and accurate about our capabilities
- Use clear, structured responses with bullet points where appropriate
- Highlight unique differentiators but avoid marketing language
- Present answers in 2-5 paragraphs typically
- Format using Markdown for clear structure within the answer field
- Include relevant documentation links in the references array (use placeholder URLs if specific links unknown)
- Ensure question_id and question_text match exactly from the input

CRITICAL: Output ONLY the JSON response. No markdown code blocks, no explanations, no preambles."""
    
    def _build_topic_user_prompt(self, topic: str, question_text: str, custom_prompt: str = "") -> str:
        """Build user prompt for topic-based generation.
        
        Args:
            topic: Topic name
            question_text: Combined question text for the topic
            custom_prompt: Custom instructions
            
        Returns:
            User prompt for AI
        """
        base_prompt = f"""Please answer the following group of related questions about {topic}:

{question_text}"""
        
        if custom_prompt and custom_prompt.strip():
            base_prompt += f"\n\nAdditional context/instructions: {custom_prompt}"
        
        return base_prompt
    
    def _build_single_user_prompt(self, question_text: str, custom_prompt: str = "") -> str:
        """Build user prompt for single question generation.
        
        Args:
            question_text: The question to answer
            custom_prompt: Custom instructions
            
        Returns:
            User prompt for AI
        """
        base_prompt = f"Please provide a detailed answer to the following question about our product capabilities:\n\n{question_text}"
        
        if custom_prompt and custom_prompt.strip():
            base_prompt += f"\n\nAdditional context/instructions: {custom_prompt}"
        
        return base_prompt
    
    def _build_json_user_prompt(self, json_payload: Dict[str, Any]) -> str:
        """Build user prompt for JSON-based generation.
        
        Args:
            json_payload: JSON payload with topic and questions
            
        Returns:
            JSON string to send to AI
        """
        import json
        return json.dumps(json_payload, indent=2)
    
    @retry(
        stop=stop_after_attempt(MAX_RETRIES), 
        wait=wait_exponential(multiplier=2, min=4, max=30),  # Exponential backoff for cold starts
        retry=retry_if_exception_type(TemporaryServiceError)
    )
    def _call_generation_api(
        self,
        system_prompt: str,
        user_prompt: str,
        auth_headers: Dict[str, str]
    ) -> Tuple[bool, str]:
        """Call the AI API for answer generation.
        
        Args:
            system_prompt: System prompt for AI
            user_prompt: User prompt with content
            auth_headers: Authentication headers
            
        Returns:
            Tuple of (success, response_text)
        """
        try:
            # Use configured model name
            model_name = self.settings.answer_generation_model
            endpoint_url = self.settings.databricks.get_model_endpoint_url(model_name)
            
            payload = {
                "messages": [
                    {"role": "user", "content": user_prompt}  # Agent has prompt bound to it
                ],
                "max_tokens": 25000,
                "temperature": 0.1
            }
            
            logger.info(f"Sending request to {model_name} for answer generation")
            
            logger.info(f"Calling {model_name} for answer generation: {endpoint_url}")
            
            response = requests.post(
                endpoint_url,
                headers=auth_headers,
                json=payload,
                timeout=self.timeout
            )
            
            # Check for JWT token expiration (403 with specific error pattern)
            if response.status_code == 403:
                response_text = response.text
                
                # Check if this is a JWT expiration error
                if ("ExpiredJwtException" in response_text or 
                    "JWT expired" in response_text or
                    "token expired" in response_text.lower()):
                    
                    logger.warning("JWT token expired, attempting to refresh authentication")
                    
                    # Get fresh authentication headers
                    fresh_auth_headers = self.settings.get_auth_headers()
                    
                    if fresh_auth_headers and fresh_auth_headers != auth_headers:
                        logger.info("Retrieved fresh authentication token, retrying API call")
                        
                        # Retry with fresh token
                        retry_response = requests.post(
                            endpoint_url,
                            headers=fresh_auth_headers,
                            json=payload,
                            timeout=self.timeout
                        )
                        
                        if retry_response.status_code == 200:
                            response_data = retry_response.json()
                            
                            # Extract response text
                            if "choices" in response_data and len(response_data["choices"]) > 0:
                                choice = response_data["choices"][0]
                                
                                # Check for message format (OpenAI/Claude style API)
                                if 'message' in choice and 'content' in choice['message']:
                                    response_text = choice['message']['content']
                                # Check for text format (older API style)
                                elif 'text' in choice:
                                    response_text = choice['text']
                                else:
                                    logger.warning("Unexpected response format")
                                    response_text = str(choice)
                                
                                logger.info(f"{model_name} generation API call successful after token refresh")
                                return True, response_text
                            else:
                                logger.error("Unexpected API response format after token refresh")
                                return False, ""
                        else:
                            logger.error(f"API call failed even after token refresh with status {retry_response.status_code}: {retry_response.text}")
                            return False, ""
                    else:
                        logger.error("Could not refresh authentication token or got same token")
                        return False, ""
                else:
                    # Non-token related 403 error
                    logger.error(f"Authentication error (403): {response_text}")
                    return False, ""
            
            elif response.status_code == 200:
                response_data = response.json()
                
                # Extract response text
                if "choices" in response_data and len(response_data["choices"]) > 0:
                    choice = response_data["choices"][0]
                    
                    # Check for message format (OpenAI/Claude style API)
                    if 'message' in choice and 'content' in choice['message']:
                        response_text = choice['message']['content']
                    # Check for text format (older API style)
                    elif 'text' in choice:
                        response_text = choice['text']
                    else:
                        logger.warning("Unexpected response format")
                        response_text = str(choice)
                    
                    logger.info(f"{model_name} generation API call successful")
                    return True, response_text
                else:
                    logger.error("Unexpected API response format")
                    return False, ""
            elif response.status_code in [500, 502, 503, 504]:
                # Server errors - raise TemporaryServiceError to trigger retry
                error_msg = f"Server temporarily unavailable ({response.status_code}): {response.text}"
                logger.warning(f"🔄 Server error {response.status_code} - will retry: {error_msg}")
                raise TemporaryServiceError(error_msg)
            else:
                logger.error(f"API call failed with status {response.status_code}: {response.text}")
                return False, ""
                
        except TemporaryServiceError:
            # Re-raise to trigger retry
            raise
        except requests.exceptions.Timeout as e:
            # Handle timeout errors - could be cold start
            error_msg = f"Request timed out after {self.timeout}s - model may be cold starting"
            logger.warning(f"⏰ Timeout error calling {model_name} - will retry: {error_msg}")
            raise TemporaryServiceError(error_msg)
        except requests.exceptions.RequestException as e:
            # Handle other network errors
            error_msg = f"Network error: {str(e)}"
            logger.warning(f"🔄 Network error calling {model_name} - will retry: {error_msg}")
            raise TemporaryServiceError(error_msg)
        except Exception as e:
            logger.error(f"Unexpected error calling {model_name} generation API: {str(e)}")
            return False, ""
    
    def _parse_topic_response(
        self,
        response_text: str,
        topic: str,
        sub_question_ids: List[str],
        original_questions: List[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """Parse AI response for topic-based generation.
        
        Args:
            response_text: Raw response from AI
            topic: Topic name
            sub_question_ids: List of sub-question IDs
            original_questions: List of original question dictionaries
            
        Returns:
            List of answer dictionaries
        """
        try:
            answers = []
            
            # Create a mapping from sub_question_id to original question text
            question_text_map = {}
            if original_questions:
                for q in original_questions:
                    sub_q_id = q.get('sub_question', '')
                    question_text = q.get('text', '')
                    if sub_q_id and question_text:
                        question_text_map[sub_q_id] = question_text
            
            # Parse with regex to match sub-question IDs with their answers
            pattern = r'(\d+(?:\.\d+)*):?\s*(.*?)(?=\n\n\d+(?:\.\d+)*:|\Z)'
            matches = re.findall(pattern, response_text, flags=re.DOTALL)
            
            if matches:
                # Build a dictionary of sub_question_id -> answer
                answer_dict = {q_id.strip(): answer.strip() for q_id, answer in matches}
                
                # Match each sub_question_id to the extracted answers
                for sub_q in sub_question_ids:
                    answer_text = ""
                    
                    # Try to find an exact match first
                    if sub_q in answer_dict:
                        answer_text = answer_dict[sub_q]
                    else:
                        # Try to find partial matches
                        for q_id in answer_dict:
                            if (sub_q.endswith(q_id) or 
                                q_id in sub_q or 
                                sub_q.replace('.', '') == q_id.replace('.', '')):
                                answer_text = answer_dict[q_id]
                                break
                    
                    # If no match found, use the full response
                    if not answer_text:
                        answer_text = response_text.strip()
                    
                    # Get the original question text from the mapping
                    original_question_text = question_text_map.get(sub_q, f"Question {sub_q}")
                    
                    answers.append({
                        "question_id": sub_q,
                        "question_text": original_question_text,
                        "answer": answer_text,
                        "topic": topic
                    })
            else:
                # If no structured parsing worked, create one answer with the full response
                answers.append({
                    "question_id": "combined",
                    "question_text": f"Combined questions for {topic}",
                    "answer": response_text.strip(),
                    "topic": topic
                })
            
            logger.info(f"Parsed {len(answers)} answers for topic {topic}")
            return answers
            
        except Exception as e:
            logger.error(f"Error parsing topic response: {str(e)}")
            return []
    
    def _parse_json_response(
        self,
        response_text: str,
        topic: str
    ) -> List[Dict[str, Any]]:
        """Parse JSON response from AI model.
        
        Args:
            response_text: Raw JSON response from AI
            topic: Topic name for validation
            
        Returns:
            List of answer dictionaries
        """
        try:
            import json
            
            logger.info(f"Parsing JSON response for topic {topic}")
            logger.info(f"Received response for parsing ({len(response_text)} chars)")
            
            # Clean up the response text
            json_text = response_text.strip()
            
            # Remove markdown code blocks if present
            if json_text.startswith('```'):
                lines = json_text.split('\n')
                if lines[0].startswith('```'):
                    lines = lines[1:]
                if lines and lines[-1].strip() == '```':
                    lines = lines[:-1]
                json_text = '\n'.join(lines)
            
            # Parse JSON
            try:
                response_data = json.loads(json_text)
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error for topic '{topic}': {e}")
                logger.error(f"Failed JSON text (first 500 chars): {json_text[:500]}...")
                logger.error(f"TROUBLESHOOTING: Agent returned plain text instead of JSON for topic '{topic}'. Check Agent prompt configuration.")
                # Drop this response - no fallback rows
                return []
            
            # Validate JSON structure
            if not isinstance(response_data, dict):
                raise ValueError("Response is not a JSON object")
            
            if 'answers' not in response_data:
                raise ValueError("Response missing 'answers' field")
            
            # Extract answers and ensure consistent format
            answers = []
            logger.info(f"DEBUG: Model returned {len(response_data['answers'])} answers")
            for i, answer_item in enumerate(response_data['answers']):
                logger.info(f"DEBUG: Answer {i} raw item: {answer_item}")
                logger.info(f"DEBUG: Answer {i} has 'references' key: {'references' in answer_item}")
                logger.info(f"DEBUG: Answer {i} references value: {answer_item.get('references', 'KEY_NOT_FOUND')}")
                
                answer = {
                    "question_id": answer_item.get('question_id', 'unknown'),
                    "question_text": answer_item.get('question_text', ''),
                    "answer": answer_item.get('answer', '').strip(),
                    "topic": topic,  # Use the topic we know, not from response
                    "references": answer_item.get('references', [])
                }
                logger.info(f"DEBUG: Answer {i} final references: {answer['references']}")
                answers.append(answer)
            
            logger.info(f"Successfully parsed {len(answers)} JSON answers for topic {topic}")
            return answers
            
        except Exception as e:
            logger.error(f"Error parsing JSON response for topic '{topic}': {str(e)}")
            logger.error(f"Raw response that caused error (first 500 chars): {response_text[:500]}...")
            logger.error(f"TROUBLESHOOTING: Failed to parse Agent response for topic '{topic}'. Check Agent prompt and output format.")
            
            # Drop this response - no fallback rows
            return []
    
 