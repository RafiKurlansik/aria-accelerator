"""Analytics service for ARIA application.

This module handles logging user interactions and document processing
data to Unity Catalog Delta tables for analytics and usage tracking.
"""

import uuid
from datetime import datetime
from typing import Optional, Dict, Any, List
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.sql import StatementParameterListItem

from aria.core.logging_config import get_logger
from aria.core.base_service import BaseService
from aria.config import settings

logger = get_logger(__name__)


class AnalyticsService(BaseService):
    """Service for logging analytics data to Unity Catalog Delta tables."""
    
    def __init__(self) -> None:
        """Initialize the Analytics service."""
        super().__init__()
        self.workspace_client: Optional[WorkspaceClient] = None
        
        logger.info("🔧 [ANALYTICS INIT] Initializing AnalyticsService...")
        
        # Get analytics configuration from YAML
        analytics_config = self.settings._raw_yaml.get('analytics', {})
        unity_config = analytics_config.get('unity_catalog', {})
        
        # Use explicit catalog/schema if available, otherwise parse instance
        self.catalog = unity_config.get('catalog', 'users')
        self.schema = unity_config.get('schema', 'rafi_kurlansik')
        
        # Build table names from configuration
        tables_config = analytics_config.get('tables', {})
        self.rfi_uploads_table = f"{self.catalog}.{self.schema}.{tables_config.get('rfi_uploads', 'rfi_uploads')}"
        self.rfi_extractions_table = f"{self.catalog}.{self.schema}.{tables_config.get('rfi_extractions', 'rfi_extractions')}"
        # Legacy rfi_generations table removed - now using rfi_generation_batches for answer tracking
        self.rfi_exports_table = f"{self.catalog}.{self.schema}.{tables_config.get('rfi_exports', 'rfi_exports')}"
        self.chat_sessions_table = f"{self.catalog}.{self.schema}.{tables_config.get('chat_sessions', 'chat_sessions')}"
        self.chat_questions_table = f"{self.catalog}.{self.schema}.{tables_config.get('chat_questions', 'chat_questions')}"
        
        # Batch-level tracking table
        self.rfi_generation_batches_table = f"{self.catalog}.{self.schema}.{tables_config.get('rfi_generation_batches', 'rfi_generation_batches')}"
        
        self.analytics_enabled = analytics_config.get('enabled', False)
        
        logger.info(f"🔧 [ANALYTICS INIT] Analytics enabled: {self.analytics_enabled}")
        logger.info(f"🔧 [ANALYTICS INIT] Target schema: {self.catalog}.{self.schema}")
        
        if self.analytics_enabled:
            self._initialize_connection()
            logger.info(f"🔧 [ANALYTICS INIT] AnalyticsService initialization complete")
        else:
            logger.warning(f"⚠️ [ANALYTICS INIT] Analytics is DISABLED - no data will be tracked")
    
    def _initialize_connection(self) -> None:
        """Initialize connection to Databricks workspace."""
        try:
            self.workspace_client = self.settings.get_workspace_client()
            if self.workspace_client is None:
                logger.warning("Failed to create workspace client for Analytics service")
                return
            
            # Test connection and ensure tables exist
            self._ensure_tables_exist()
            
            # Ensure all required columns exist (handle schema evolution)
            self._ensure_columns_exist()
            
            logger.info("Analytics service initialized successfully")
            
        except Exception as e:
            logger.warning(f"Failed to initialize Analytics service: {e}")
            self.workspace_client = None
    
    def _ensure_tables_exist(self) -> None:
        """Ensure required tables exist, create them if they don't."""
        if not self.workspace_client or not self.settings.databricks.warehouse_id:
            logger.warning("Cannot create tables: missing workspace client or warehouse ID")
            return
        
        logger.info(f"🔧 [ANALYTICS TABLES] Starting table creation in {self.catalog}.{self.schema}")
        
        # First, validate that the schema exists
        try:
            logger.info(f"📋 [ANALYTICS TABLES] Validating schema {self.catalog}.{self.schema}...")
            # Test with a simple query to validate schema access
            test_query = f"SHOW TABLES IN {self.catalog}.{self.schema} LIMIT 1"
            self.workspace_client.statement_execution.execute_statement(
                statement=test_query,
                warehouse_id=self.settings.databricks.warehouse_id,
                wait_timeout="10s"
            )
            logger.info(f"✅ [ANALYTICS TABLES] Schema {self.catalog}.{self.schema} validated successfully")
        except Exception as schema_error:
            logger.error(f"❌ [ANALYTICS TABLES] Schema validation failed: {schema_error}")
            logger.error(f"❌ [ANALYTICS TABLES] Cannot create tables - schema {self.catalog}.{self.schema} may not exist or lack permissions")
            return
        
        try:
            # Define all table DDL statements
            tables_ddl = {
                "rfi_uploads": f"""
                CREATE TABLE IF NOT EXISTS {self.rfi_uploads_table} (
                    event_id STRING NOT NULL,
                    user_id STRING,
                    session_id STRING,
                    timestamp TIMESTAMP NOT NULL,
                    ip_address STRING,
                    user_agent STRING,
                    document_name STRING,
                    file_size_bytes BIGINT,
                    file_type STRING,
                    processing_method STRING,
                    created_at TIMESTAMP
                ) USING DELTA
                """,
                
                "rfi_extractions": f"""
                CREATE TABLE IF NOT EXISTS {self.rfi_extractions_table} (
                    event_id STRING NOT NULL,
                    user_id STRING,
                    session_id STRING,
                    timestamp TIMESTAMP NOT NULL,
                    ip_address STRING,
                    user_agent STRING,
                    document_name STRING,
                    extraction_method STRING,
                    questions_extracted INT,
                    model_used STRING,
                    custom_prompt STRING,
                    processing_time_seconds DOUBLE,
                    questions ARRAY<STRUCT<
                        question: STRING,
                        sub_topics: ARRAY<STRUCT<
                            topic: STRING,
                            sub_questions: ARRAY<STRUCT<
                                sub_question: STRING,
                                text: STRING
                            >>
                        >>
                    >>,
                    created_at TIMESTAMP
                ) USING DELTA
                """,
                
                # Legacy rfi_generations table removed - replaced by rfi_generation_batches
                
                "rfi_exports": f"""
                CREATE TABLE IF NOT EXISTS {self.rfi_exports_table} (
                    event_id STRING NOT NULL,
                    user_id STRING,
                    session_id STRING,
                    timestamp TIMESTAMP NOT NULL,
                    ip_address STRING,
                    user_agent STRING,
                    document_name STRING,
                    export_format STRING,
                    total_questions INT,
                    total_answers INT,
                    created_at TIMESTAMP
                ) USING DELTA
                """,
                
                "chat_sessions": f"""
                CREATE TABLE IF NOT EXISTS {self.chat_sessions_table} (
                    event_id STRING NOT NULL,
                    user_id STRING,
                    session_id STRING,
                    timestamp TIMESTAMP NOT NULL,
                    ip_address STRING,
                    user_agent STRING,
                    chat_session_id STRING,
                    total_questions INT,
                    total_responses INT,
                    session_duration_seconds DOUBLE,
                    final_status STRING,
                    created_at TIMESTAMP
                ) USING DELTA
                """,
                
                "chat_questions": f"""
                CREATE TABLE IF NOT EXISTS {self.chat_questions_table} (
                    event_id STRING NOT NULL,
                    user_id STRING,
                    session_id STRING,
                    timestamp TIMESTAMP NOT NULL,
                    ip_address STRING,
                    user_agent STRING,
                    chat_session_id STRING,
                    question STRUCT<
                        text: STRING,
                        length_chars: INT
                    >,
                    response STRUCT<
                        text: STRING,
                        length_chars: INT,
                        model_used: STRING,
                        response_time_seconds: DOUBLE,
                        copied_to_clipboard: BOOLEAN
                    >,
                    created_at TIMESTAMP
                ) USING DELTA
                """,
                

                
                "rfi_generation_batches": f"""
                CREATE TABLE IF NOT EXISTS {self.rfi_generation_batches_table} (
                    batch_id STRING NOT NULL,
                    session_id STRING NOT NULL,
                    session_version INT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    user_id STRING,
                    ip_address STRING,
                    user_agent STRING,
                    document_name STRING,
                    topic_name STRING,
                    batch_attempt_number INT,
                    batch_status STRING,
                    batch_processing_time_seconds DOUBLE,
                    model_used STRING,
                    custom_prompt STRING,
                    questions_in_batch ARRAY<STRUCT<
                        question_id: STRING,
                        text: STRING,
                        topic: STRING
                    >>,
                    answers_in_batch ARRAY<STRUCT<
                        question_id: STRING,
                        question_text: STRING,
                        answer: STRING,
                        topic: STRING,
                        references: ARRAY<STRING>
                    >>,
                    error_message STRING,
                    created_at TIMESTAMP
                ) USING DELTA
                """
            }
            
            # Execute DDL statements with comprehensive logging
            successful_tables = []
            failed_tables = []
            
            for table_name, ddl in tables_ddl.items():
                table_full_name = f"{self.catalog}.{self.schema}.{table_name}"
                logger.info(f"🔨 [ANALYTICS TABLES] Creating table {table_full_name}...")
                
                try:
                    result = self.workspace_client.statement_execution.execute_statement(
                        statement=ddl,
                        warehouse_id=self.settings.databricks.warehouse_id,
                        wait_timeout="30s"
                    )
                    
                    # Check the result status (handle enum comparison properly)
                    if hasattr(result, 'status') and hasattr(result.status, 'state'):
                        state_str = str(result.status.state)
                        if 'SUCCEEDED' in state_str:
                            logger.info(f"✅ [ANALYTICS TABLES] Table {table_full_name} created successfully")
                            successful_tables.append(table_name)
                        else:
                            error_obj = getattr(result.status, 'error', None)
                            if error_obj:
                                # Handle ServiceError object properly - access attributes directly
                                error_msg = getattr(error_obj, 'message', None) or getattr(error_obj, 'detail', None) or str(error_obj)
                            else:
                                error_msg = f'State: {state_str}'
                            logger.error(f"❌ [ANALYTICS TABLES] Table {table_full_name} creation failed: {error_msg}")
                            failed_tables.append((table_name, error_msg))
                    else:
                        # Assume success if no status info
                        logger.info(f"✅ [ANALYTICS TABLES] Table {table_full_name} created (status unknown)")
                        successful_tables.append(table_name)
                        
                except Exception as e:
                    logger.error(f"❌ [ANALYTICS TABLES] Table {table_full_name} creation failed: {e}")
                    failed_tables.append((table_name, str(e)))
            
            # Summary logging
            if successful_tables:
                logger.info(f"🎉 [ANALYTICS TABLES] Successfully created {len(successful_tables)} tables: {', '.join(successful_tables)}")
            
            if failed_tables:
                logger.error(f"⚠️ [ANALYTICS TABLES] Failed to create {len(failed_tables)} tables:")
                for table_name, error in failed_tables:
                    logger.error(f"⚠️ [ANALYTICS TABLES]   - {table_name}: {error}")
                logger.error(f"❌ [ANALYTICS TABLES] CRITICAL: Some tables failed to create. Analytics data may not be stored properly!")
            else:
                logger.info(f"🎉 [ANALYTICS TABLES] All analytics tables ready in {self.catalog}.{self.schema}")
            
        except Exception as e:
            logger.error(f"🚫 [ANALYTICS TABLES] Critical failure during table creation: {e}")
            import traceback
            logger.error(f"🚫 [ANALYTICS TABLES] Full traceback: {traceback.format_exc()}")
            raise  # Re-raise to ensure failures are visible
    
    def drop_all_analytics_tables(self) -> bool:
        """Drop all analytics tables for fresh start.
        
        Returns:
            True if all tables dropped successfully, False otherwise
        """
        if not self.workspace_client:
            logger.error("❌ [ANALYTICS DROP] No Databricks workspace client available")
            return False
            
        tables_to_drop = [
            self.rfi_uploads_table,
            self.rfi_extractions_table, 
            # self.rfi_generations_table,  # Legacy table removed
            self.rfi_generation_batches_table,
            self.rfi_exports_table,
            self.chat_sessions_table,
            self.chat_questions_table
        ]
        
        logger.info(f"🗑️ [ANALYTICS DROP] Starting to drop {len(tables_to_drop)} analytics tables")
        
        dropped_count = 0
        for table_name in tables_to_drop:
            try:
                logger.info(f"🗑️ [ANALYTICS DROP] Attempting to drop table: {table_name}")
                
                drop_sql = f"DROP TABLE IF EXISTS {table_name}"
                
                result = self.workspace_client.statement_execution.execute_statement(
                    statement=drop_sql,
                    warehouse_id=self.settings.databricks.warehouse_id,
                    wait_timeout="30s"
                )
                
                if result and hasattr(result, 'status'):
                    state_str = str(result.status.state)
                    if 'SUCCEEDED' in state_str:
                        logger.info(f"✅ [ANALYTICS DROP] Successfully dropped table: {table_name}")
                        dropped_count += 1
                    else:
                        logger.error(f"❌ [ANALYTICS DROP] Failed to drop table {table_name}: {state_str}")
                else:
                    logger.warning(f"⚠️ [ANALYTICS DROP] Drop operation for {table_name} completed but status unknown")
                    dropped_count += 1
                    
            except Exception as e:
                error_message = getattr(e, 'message', None) or getattr(e, 'detail', None) or str(e)
                logger.error(f"❌ [ANALYTICS DROP] Exception dropping table {table_name}: {error_message}")
        
        logger.info(f"🗑️ [ANALYTICS DROP] Completed: {dropped_count}/{len(tables_to_drop)} tables dropped successfully")
        return dropped_count == len(tables_to_drop)

    def _ensure_columns_exist(self) -> None:
        """Ensure all required columns exist in tables, add missing ones."""
        if not self.workspace_client or not self.settings.databricks.warehouse_id:
            logger.warning("Cannot check columns: missing workspace client or warehouse ID")
            return
        
        # Define missing columns that might need to be added
        # Note: For new STRUCT/ARRAY columns, we'll need to handle migration separately
        column_fixes = {
            self.rfi_extractions_table: [
                ("questions", "ARRAY<STRUCT<question:STRING,sub_topics:ARRAY<STRUCT<topic:STRING,sub_questions:ARRAY<STRUCT<sub_question:STRING,text:STRING>>>>>>", "Structured questions data"),
                ("questions_json", "STRING", "Legacy JSON string - will be migrated to questions column")
            ],
            # self.rfi_generations_table: [  # Legacy table removed
            #     ("questions_answers_json", "STRING", "Legacy JSON string - replaced by granular tracking")
            # ],

            self.rfi_generation_batches_table: [
                ("questions_in_batch", "ARRAY<STRUCT<question_id:STRING,text:STRING,topic:STRING>>", "Questions sent to AI in this batch"),
                ("answers_in_batch", "ARRAY<STRUCT<question_id:STRING,question_text:STRING,answer:STRING,topic:STRING,references:ARRAY<STRING>>>", "Answers received from AI for this batch")
            ],
            self.chat_questions_table: [
                ("question", "STRUCT<text:STRING,length_chars:INT>", "Structured question data"),
                ("response", "STRUCT<text:STRING,length_chars:INT,model_used:STRING,response_time_seconds:DOUBLE,copied_to_clipboard:BOOLEAN>", "Structured response data")
            ]
        }
        
        for table_name, columns in column_fixes.items():
            logger.info(f"🔧 [ANALYTICS COLUMNS] Checking columns for {table_name}")
            
            for column_name, column_type, description in columns:
                try:
                    # Try to describe the table to see if column exists
                    describe_query = f"DESCRIBE TABLE {table_name}"
                    result = self.workspace_client.statement_execution.execute_statement(
                        statement=describe_query,
                        warehouse_id=self.settings.databricks.warehouse_id,
                        wait_timeout="10s"
                    )
                    
                    # Check if the column exists in the result
                    column_exists = False
                    if hasattr(result, 'result') and hasattr(result.result, 'data_array'):
                        for row in result.result.data_array:
                            if row and len(row) > 0 and row[0] == column_name:
                                column_exists = True
                                break
                    
                    if not column_exists:
                        logger.info(f"🔨 [ANALYTICS COLUMNS] Adding missing column {column_name} to {table_name}")
                        alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                        
                        alter_result = self.workspace_client.statement_execution.execute_statement(
                            statement=alter_query,
                            warehouse_id=self.settings.databricks.warehouse_id,
                            wait_timeout="30s"
                        )
                        
                        # Check result status
                        if hasattr(alter_result, 'status') and hasattr(alter_result.status, 'state'):
                            state_str = str(alter_result.status.state)
                            if 'SUCCEEDED' in state_str:
                                logger.info(f"✅ [ANALYTICS COLUMNS] Successfully added column {column_name} to {table_name}")
                            else:
                                error_obj = getattr(alter_result.status, 'error', None)
                                if error_obj:
                                    error_msg = getattr(error_obj, 'message', None) or getattr(error_obj, 'detail', None) or str(error_obj)
                                else:
                                    error_msg = f'State: {state_str}'
                                logger.error(f"❌ [ANALYTICS COLUMNS] Failed to add column {column_name}: {error_msg}")
                        else:
                            logger.info(f"✅ [ANALYTICS COLUMNS] Column {column_name} added (status unknown)")
                    else:
                        logger.debug(f"✅ [ANALYTICS COLUMNS] Column {column_name} already exists in {table_name}")
                        
                except Exception as e:
                    logger.error(f"❌ [ANALYTICS COLUMNS] Error checking/adding column {column_name} to {table_name}: {e}")

        # Schema evolution: Change custom_prompt_used BOOLEAN to custom_prompt STRING
        logger.info("🔄 [ANALYTICS SCHEMA] Checking for schema evolution (custom_prompt column type change)")

        schema_evolution_changes = [
            (self.rfi_extractions_table, "custom_prompt_used", "custom_prompt", "STRING"),
            (self.rfi_generation_batches_table, "custom_prompt_used", "custom_prompt", "STRING")
        ]

        for table_name, old_column, new_column, new_type in schema_evolution_changes:
            try:
                logger.info(f"🔄 [ANALYTICS SCHEMA] Checking schema evolution for {table_name}")

                # Check if old column exists and new column doesn't
                describe_query = f"DESCRIBE TABLE {table_name}"
                result = self.workspace_client.statement_execution.execute_statement(
                    statement=describe_query,
                    warehouse_id=self.settings.databricks.warehouse_id,
                    wait_timeout="10s"
                )

                old_column_exists = False
                new_column_exists = False

                if hasattr(result, 'result') and hasattr(result.result, 'data_array'):
                    for row in result.result.data_array:
                        if row and len(row) > 0:
                            if row[0] == old_column:
                                old_column_exists = True
                            elif row[0] == new_column:
                                new_column_exists = True

                if old_column_exists and not new_column_exists:
                    logger.info(f"🔄 [ANALYTICS SCHEMA] Performing schema evolution: {old_column} → {new_column} ({new_type}) in {table_name}")

                    # Rename the old column to the new name and change type
                    alter_sql = f"ALTER TABLE {table_name} ALTER COLUMN {old_column} TYPE {new_type}"
                    alter_result = self.workspace_client.statement_execution.execute_statement(
                        statement=alter_sql,
                        warehouse_id=self.settings.databricks.warehouse_id,
                        wait_timeout="30s"
                    )

                    # Then rename the column
                    rename_sql = f"ALTER TABLE {table_name} RENAME COLUMN {old_column} TO {new_column}"
                    rename_result = self.workspace_client.statement_execution.execute_statement(
                        statement=rename_sql,
                        warehouse_id=self.settings.databricks.warehouse_id,
                        wait_timeout="30s"
                    )

                    logger.info(f"✅ [ANALYTICS SCHEMA] Schema evolution completed for {table_name}: {old_column} → {new_column}")

                elif new_column_exists:
                    logger.debug(f"✅ [ANALYTICS SCHEMA] Schema evolution already completed for {table_name}")

                else:
                    logger.info(f"ℹ️ [ANALYTICS SCHEMA] No schema evolution needed for {table_name} (columns don't exist yet)")

            except Exception as e:
                logger.error(f"❌ [ANALYTICS SCHEMA] Schema evolution failed for {table_name}: {e}")

    def is_enabled(self) -> bool:
        """Check if Analytics logging is enabled and properly configured."""
        return (
            self.analytics_enabled and 
            self.workspace_client is not None and
            self.settings.databricks.warehouse_id is not None
        )
    
    def _column_exists(self, table_name: str, column_name: str) -> bool:
        """Check if a column exists in a table."""
        try:
            describe_query = f"DESCRIBE TABLE {table_name}"
            result = self.workspace_client.statement_execution.execute_statement(
                statement=describe_query,
                warehouse_id=self.settings.databricks.warehouse_id,
                wait_timeout="10s"
            )
            
            if result.result and result.result.data_array:
                for row in result.result.data_array:
                    if row and len(row) > 0 and row[0] == column_name:
                        return True
            return False
        except Exception as e:
            logger.warning(f"⚠️ [ANALYTICS SCHEMA] Error checking column {column_name} in {table_name}: {e}")
            return False
    
    def _get_user_info_from_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, Optional[str]]:
        """Extract user information from HTTP headers.
        
        According to Databricks Apps documentation, the following headers are available:
        - X-Forwarded-User: The user identifier provided by IdP
        - X-Forwarded-Email: The user email provided by IdP  
        - X-Forwarded-Preferred-Username: The user name provided by IdP
        - X-Real-Ip: The IP address of the client that made the original request
        
        Args:
            headers: HTTP headers dictionary
            
        Returns:
            Dictionary with user_id, ip_address, user_agent
        """
        if not headers:
            headers = {}
        
        # COMPREHENSIVE DEBUG: Log ALL headers when in production environment
        if headers:
            logger.info(f"🔍 [ANALYTICS HEADERS] ALL HEADERS RECEIVED ({len(headers)} total):")
            for key, value in sorted(headers.items()):
                # Mask potentially sensitive values but show structure
                if 'token' in key.lower() or 'auth' in key.lower() or 'secret' in key.lower():
                    logger.info(f"🔍 [ANALYTICS HEADERS]   {key}: [MASKED]")
                else:
                    logger.info(f"🔍 [ANALYTICS HEADERS]   {key}: {value}")
        else:
            logger.warning(f"🔍 [ANALYTICS HEADERS] NO HEADERS RECEIVED!")
        
        # Debug: Log relevant headers for troubleshooting
        relevant_headers = {k: v for k, v in headers.items() if any(
            prefix.lower() in k.lower() for prefix in [
                'x-forwarded', 'x-real', 'x-user', 'user-agent', 'remote'
            ]
        )}
        logger.info(f"🔍 [ANALYTICS HEADERS] Relevant headers: {relevant_headers}")
        
        # Extract user ID from Databricks Apps headers
        # PREFER readable email/username over encoded user ID
        # Check both capitalized and lowercase variations
        user_id_candidates = [
            headers.get('X-Forwarded-Email'),  # First: readable email (capitalized)
            headers.get('x-forwarded-email'),  # First: readable email (lowercase)
            headers.get('X-Forwarded-Preferred-Username'),  # Second: readable username (capitalized)
            headers.get('x-forwarded-preferred-username'),  # Second: readable username (lowercase)
            headers.get('X-Forwarded-User'),  # Fallback: encoded user ID (capitalized)
            headers.get('x-forwarded-user'),  # Fallback: encoded user ID (lowercase)
        ]
        user_id = next((uid for uid in user_id_candidates if uid), None)
        
        # Extract IP address from Databricks Apps headers
        # Check both capitalized and lowercase variations
        ip_candidates = [
            headers.get('X-Real-Ip'),  # Primary: Databricks Apps client IP (capitalized)
            headers.get('x-real-ip'),  # Primary: Databricks Apps client IP (lowercase)
            headers.get('X-Forwarded-For'),  # Fallback: standard proxy header (capitalized)
            headers.get('x-forwarded-for'),  # Fallback: standard proxy header (lowercase)
        ]
        ip_address = next((ip for ip in ip_candidates if ip), None)
        
        # Log extracted values and candidates for debugging
        logger.info(f"🔍 [ANALYTICS HEADERS] USER ID CANDIDATES: {[f'{i}: {uid}' for i, uid in enumerate(user_id_candidates) if uid]}")
        logger.info(f"🔍 [ANALYTICS HEADERS] IP CANDIDATES: {[f'{i}: {ip}' for i, ip in enumerate(ip_candidates) if ip]}")
        logger.info(f"🔍 [ANALYTICS HEADERS] FINAL EXTRACTED - user_id: '{user_id}', ip_address: '{ip_address}'")
        
        # Extract user agent (check both cases)
        user_agent = headers.get('User-Agent') or headers.get('user-agent')
        
        return {
            'user_id': user_id,
            'ip_address': ip_address,
            'user_agent': user_agent
        }
    
    def track_rfi_upload(
        self,
        session_id: str,
        document_name: str,
        file_size_bytes: int,
        file_type: str,
        processing_method: str,
        headers: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Track RFI document upload event.
        
        Args:
            session_id: Browser session identifier
            document_name: Name of uploaded document
            file_size_bytes: File size in bytes
            file_type: File type (pdf, docx, csv, etc.)
            processing_method: How document will be processed
            headers: HTTP headers for user identification
            timestamp: Event timestamp (defaults to now)
            
        Returns:
            True if successfully logged, False otherwise
        """
        logger.info(f"🚀 [ANALYTICS CALL] track_rfi_upload() called: {document_name} ({file_type})")
        
        if not self.is_enabled():
            logger.warning(f"⚠️ [ANALYTICS CALL] track_rfi_upload() skipped - analytics disabled")
            return False
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        user_info = self._get_user_info_from_headers(headers)
        event_id = str(uuid.uuid4())
        
        try:
            insert_sql = f"""
            INSERT INTO {self.rfi_uploads_table} 
            (event_id, user_id, session_id, timestamp, ip_address, user_agent, 
             document_name, file_size_bytes, file_type, processing_method)
            VALUES (:event_id, :user_id, :session_id, :timestamp, :ip_address, :user_agent, 
                    :document_name, :file_size_bytes, :file_type, :processing_method)
            """
            
            # Convert parameters to StatementParameterListItem objects
            parameters = [
                StatementParameterListItem(name="event_id", value=event_id),
                StatementParameterListItem(name="user_id", value=user_info['user_id']),
                StatementParameterListItem(name="session_id", value=session_id),
                StatementParameterListItem(name="timestamp", value=timestamp.isoformat()),
                StatementParameterListItem(name="ip_address", value=user_info['ip_address']),
                StatementParameterListItem(name="user_agent", value=user_info['user_agent']),
                StatementParameterListItem(name="document_name", value=document_name),
                StatementParameterListItem(name="file_size_bytes", value=str(file_size_bytes)),
                StatementParameterListItem(name="file_type", value=file_type),
                StatementParameterListItem(name="processing_method", value=processing_method)
            ]
            
            logger.info(f"📊 [ANALYTICS INSERT] ATTEMPTING RFI upload insert: {document_name} ({file_type})")
            
            result = self.workspace_client.statement_execution.execute_statement(
                statement=insert_sql,
                parameters=parameters,
                warehouse_id=self.settings.databricks.warehouse_id,
                wait_timeout="30s"
                )
            
            # Check execution status
            if result and hasattr(result, 'status'):
                state_str = str(result.status.state)
                if 'SUCCEEDED' in state_str:
                    logger.info(f"✅ [ANALYTICS INSERT] RFI upload insert SUCCEEDED: {document_name} ({file_type}) -> {self.rfi_uploads_table}")
                else:
                    error_obj = getattr(result.status, 'error', None)
                    error_message = getattr(error_obj, 'message', None) or getattr(error_obj, 'detail', None) or str(error_obj) if error_obj else "Unknown error"
                    logger.error(f"❌ [ANALYTICS INSERT] RFI upload insert FAILED: {document_name} ({file_type}) - {error_message}")
                    return False
            else:
                logger.warning(f"⚠️ [ANALYTICS INSERT] RFI upload insert completed but status unknown: {document_name} ({file_type})")
            
            return True
            
        except Exception as e:
            logger.warning(f"Failed to track RFI upload: {e}")
            return False
    
    def track_rfi_extraction(
        self,
        session_id: str,
        document_name: str,
        extraction_method: str,
        questions_extracted: int,
        model_used: str,
        custom_prompt: Optional[str],
        processing_time_seconds: float,
        questions_data: Optional[List[Dict[str, Any]]] = None,
        headers: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Track RFI question extraction event.
        
        Args:
            session_id: Browser session identifier
            document_name: Name of processed document
            extraction_method: Method used for extraction
            questions_extracted: Number of questions extracted
            model_used: AI model used for extraction
            custom_prompt: Custom prompt content used, or None if default prompt
            processing_time_seconds: Time taken for processing
            questions_data: Structured questions data as list of dictionaries
            headers: HTTP headers for user identification
            timestamp: Event timestamp (defaults to now)
            
        Returns:
            True if successfully logged, False otherwise
        """
        logger.info(f"🚀 [ANALYTICS CALL] track_rfi_extraction() called: {document_name} ({questions_extracted} questions)")
        
        if not self.is_enabled():
            logger.warning(f"⚠️ [ANALYTICS CALL] track_rfi_extraction() skipped - analytics disabled")
            return False
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        user_info = self._get_user_info_from_headers(headers)
        event_id = str(uuid.uuid4())
        
        try:
            # Check if custom_prompt column exists
            has_custom_prompt = self._column_exists(self.rfi_extractions_table, "custom_prompt")
            
            if has_custom_prompt:
                insert_sql = f"""
                INSERT INTO {self.rfi_extractions_table}
                (event_id, user_id, session_id, timestamp, ip_address, user_agent,
                 document_name, extraction_method, questions_extracted, model_used,
                 custom_prompt, processing_time_seconds, questions, created_at)
                VALUES (:event_id, :user_id, :session_id, :timestamp, :ip_address, :user_agent,
                        :document_name, :extraction_method, :questions_extracted, :model_used,
                        :custom_prompt, :processing_time_seconds,
                        from_json(:questions, 'ARRAY<STRUCT<question:STRING,sub_topics:ARRAY<STRUCT<topic:STRING,sub_questions:ARRAY<STRUCT<sub_question:STRING,text:STRING>>>>>>'),
                        :created_at)
                """
            else:
                logger.warning(f"⚠️ [ANALYTICS SCHEMA] custom_prompt column not found in {self.rfi_extractions_table}, inserting without it")
                insert_sql = f"""
                INSERT INTO {self.rfi_extractions_table}
                (event_id, user_id, session_id, timestamp, ip_address, user_agent,
                 document_name, extraction_method, questions_extracted, model_used,
                 processing_time_seconds, questions, created_at)
                VALUES (:event_id, :user_id, :session_id, :timestamp, :ip_address, :user_agent,
                        :document_name, :extraction_method, :questions_extracted, :model_used,
                        :processing_time_seconds,
                        from_json(:questions, 'ARRAY<STRUCT<question:STRING,sub_topics:ARRAY<STRUCT<topic:STRING,sub_questions:ARRAY<STRUCT<sub_question:STRING,text:STRING>>>>>>'),
                        :created_at)
                """
            
            # Convert parameters to StatementParameterListItem objects
            logger.info(f"📊 [ANALYTICS INSERT] Inserting RFI extraction record: {document_name} ({questions_extracted} questions)")
            
            # Convert questions_data to JSON string for STRUCT/ARRAY parameter
            questions_json = None
            if questions_data:
                try:
                    import json
                    questions_json = json.dumps(questions_data, ensure_ascii=False)
                except Exception as json_error:
                    logger.warning(f"Failed to serialize questions_data: {json_error}")
                    questions_json = None
            
            parameters = [
                StatementParameterListItem(name="event_id", value=event_id),
                StatementParameterListItem(name="user_id", value=user_info['user_id']),
                StatementParameterListItem(name="session_id", value=session_id),
                StatementParameterListItem(name="timestamp", value=timestamp.isoformat()),
                StatementParameterListItem(name="ip_address", value=user_info['ip_address']),
                StatementParameterListItem(name="user_agent", value=user_info['user_agent']),
                StatementParameterListItem(name="document_name", value=document_name),
                StatementParameterListItem(name="extraction_method", value=extraction_method),
                StatementParameterListItem(name="questions_extracted", value=str(questions_extracted)),
                StatementParameterListItem(name="model_used", value=model_used),
                StatementParameterListItem(name="processing_time_seconds", value=str(processing_time_seconds)),
                StatementParameterListItem(name="questions", value=questions_json),
                StatementParameterListItem(name="created_at", value=timestamp.isoformat())
            ]
            
            # Add custom_prompt parameter only if column exists
            if has_custom_prompt:
                parameters.insert(-3, StatementParameterListItem(name="custom_prompt", value=custom_prompt or ""))
            
            logger.info(f"📊 [ANALYTICS INSERT] ATTEMPTING RFI extraction insert: {document_name} ({questions_extracted} questions)")
            
            result = self.workspace_client.statement_execution.execute_statement(
                statement=insert_sql,
                parameters=parameters,
                warehouse_id=self.settings.databricks.warehouse_id,
                wait_timeout="30s"
            )
            
            # Check result status
            if hasattr(result, 'status') and hasattr(result.status, 'state'):
                state_str = str(result.status.state)
                if 'SUCCEEDED' in state_str:
                    logger.info(f"✅ [ANALYTICS INSERT] Successfully tracked RFI extraction: {document_name} ({questions_extracted} questions) -> {self.rfi_extractions_table}")
                else:
                    error_obj = getattr(result.status, 'error', None)
                    if error_obj:
                        # Handle ServiceError object properly - access attributes directly
                        error_msg = getattr(error_obj, 'message', None) or getattr(error_obj, 'detail', None) or str(error_obj)
                    else:
                        error_msg = f'State: {state_str}'
                    logger.error(f"❌ [ANALYTICS INSERT] Failed to track RFI extraction: {error_msg}")
                    return False
            else:
                logger.info(f"✅ [ANALYTICS INSERT] RFI extraction tracked (status unknown): {document_name} ({questions_extracted} questions) -> {self.rfi_extractions_table}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ [ANALYTICS ERROR] Failed to track RFI extraction: {e}")
            import traceback
            logger.debug(f"❌ [ANALYTICS ERROR] Full traceback: {traceback.format_exc()}")
            return False
    
    # Legacy track_rfi_generation method removed
    # Now using batch-level tracking via track_generation_batch()
    # This provides better business KPI visibility and prevents double-counting
    

    
    def track_generation_batch(
        self,
        session_id: str,
        session_version: int,
        topic_name: str,
        batch_attempt_number: int,
        batch_status: str,
        batch_processing_time_seconds: float,
        model_used: str,
        custom_prompt: Optional[str],
        questions_in_batch: List[Dict[str, Any]],
        answers_in_batch: List[Dict[str, Any]],
        document_name: str,
        error_message: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Track individual batch/topic processing.
        
        Args:
            session_id: Session identifier
            session_version: Session version (for regenerations)
            topic_name: Name of the topic/batch processed
            batch_attempt_number: Attempt number for this batch (for retries)
            batch_status: Status (success, failed, partial)
            batch_processing_time_seconds: Time taken for this batch
            model_used: AI model used
            custom_prompt: Custom prompt content used, or None if default prompt
            questions_in_batch: Questions sent to AI for this batch
            answers_in_batch: Answers received from AI for this batch
            document_name: Source document name
            error_message: Error message if batch failed
            headers: HTTP headers for user identification
            timestamp: Event timestamp (defaults to now)
            
        Returns:
            True if successfully logged, False otherwise
        """
        if not self.is_enabled():
            logger.debug("Analytics logging disabled or not configured")
            return False
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        user_info = self._get_user_info_from_headers(headers)
        batch_id = f"{session_id}-{topic_name}-{int(timestamp.timestamp())}-{batch_attempt_number}"
        
        try:
            # Serialize structured data to JSON for STRUCT/ARRAY parameters
            import json
            questions_json = json.dumps(questions_in_batch, ensure_ascii=False) if questions_in_batch else "[]"
            answers_json = json.dumps(answers_in_batch, ensure_ascii=False) if answers_in_batch else "[]"
            
            # Check if custom_prompt column exists
            has_custom_prompt = self._column_exists(self.rfi_generation_batches_table, "custom_prompt")
            
            if has_custom_prompt:
                insert_sql = f"""
                INSERT INTO {self.rfi_generation_batches_table}
                (batch_id, session_id, session_version, timestamp, user_id, ip_address, user_agent,
                 document_name, topic_name, batch_attempt_number, batch_status, batch_processing_time_seconds,
                 model_used, custom_prompt, questions_in_batch, answers_in_batch, error_message, created_at)
                VALUES (:batch_id, :session_id, :session_version, :timestamp, :user_id, :ip_address, :user_agent,
                        :document_name, :topic_name, :batch_attempt_number, :batch_status, :batch_processing_time_seconds,
                        :model_used, :custom_prompt,
                        from_json(:questions_in_batch, 'ARRAY<STRUCT<question_id:STRING,text:STRING,topic:STRING>>'),
                        from_json(:answers_in_batch, 'ARRAY<STRUCT<question_id:STRING,question_text:STRING,answer:STRING,topic:STRING,references:ARRAY<STRING>>>'),
                        :error_message, :created_at)
                """
            else:
                logger.warning(f"⚠️ [ANALYTICS SCHEMA] custom_prompt column not found in {self.rfi_generation_batches_table}, inserting without it")
                insert_sql = f"""
                INSERT INTO {self.rfi_generation_batches_table}
                (batch_id, session_id, session_version, timestamp, user_id, ip_address, user_agent,
                 document_name, topic_name, batch_attempt_number, batch_status, batch_processing_time_seconds,
                 model_used, questions_in_batch, answers_in_batch, error_message, created_at)
                VALUES (:batch_id, :session_id, :session_version, :timestamp, :user_id, :ip_address, :user_agent,
                        :document_name, :topic_name, :batch_attempt_number, :batch_status, :batch_processing_time_seconds,
                        :model_used,
                        from_json(:questions_in_batch, 'ARRAY<STRUCT<question_id:STRING,text:STRING,topic:STRING>>'),
                        from_json(:answers_in_batch, 'ARRAY<STRUCT<question_id:STRING,question_text:STRING,answer:STRING,topic:STRING,references:ARRAY<STRING>>>'),
                        :error_message, :created_at)
                """
            
            parameters = [
                StatementParameterListItem(name="batch_id", value=batch_id),
                StatementParameterListItem(name="session_id", value=session_id),
                StatementParameterListItem(name="session_version", value=str(session_version)),
                StatementParameterListItem(name="timestamp", value=timestamp.isoformat()),
                StatementParameterListItem(name="user_id", value=user_info['user_id']),
                StatementParameterListItem(name="ip_address", value=user_info['ip_address']),
                StatementParameterListItem(name="user_agent", value=user_info['user_agent']),
                StatementParameterListItem(name="document_name", value=document_name),
                StatementParameterListItem(name="topic_name", value=topic_name),
                StatementParameterListItem(name="batch_attempt_number", value=str(batch_attempt_number)),
                StatementParameterListItem(name="batch_status", value=batch_status),
                StatementParameterListItem(name="batch_processing_time_seconds", value=str(batch_processing_time_seconds)),
                StatementParameterListItem(name="model_used", value=model_used),
                StatementParameterListItem(name="questions_in_batch", value=questions_json),
                StatementParameterListItem(name="answers_in_batch", value=answers_json),
                StatementParameterListItem(name="error_message", value=error_message or ""),
                StatementParameterListItem(name="created_at", value=timestamp.isoformat())
            ]
            
            # Add custom_prompt parameter only if column exists
            if has_custom_prompt:
                parameters.insert(-3, StatementParameterListItem(name="custom_prompt", value=custom_prompt or ""))
            
            logger.info(f"📊 [ANALYTICS INSERT] ATTEMPTING batch insert: {batch_id} (topic: {topic_name}, status: {batch_status})")
            
            result = self.workspace_client.statement_execution.execute_statement(
                statement=insert_sql,
                parameters=parameters,
                warehouse_id=self.settings.databricks.warehouse_id,
                wait_timeout="30s"
                )
            
            # Check execution status
            if result and hasattr(result, 'status'):
                state_str = str(result.status.state)
                if 'SUCCEEDED' in state_str:
                    logger.info(f"✅ [ANALYTICS INSERT] Batch insert SUCCEEDED: {batch_id} -> {self.rfi_generation_batches_table}")
                else:
                    error_obj = getattr(result.status, 'error', None)
                    error_message = getattr(error_obj, 'message', None) or getattr(error_obj, 'detail', None) or str(error_obj) if error_obj else "Unknown error"
                    logger.error(f"❌ [ANALYTICS INSERT] Batch insert FAILED: {batch_id} - {error_message}")
                    return False
            else:
                logger.warning(f"⚠️ [ANALYTICS INSERT] Batch insert completed but status unknown: {batch_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ [ANALYTICS ERROR] Failed to track generation batch: {e}")
            return False
    
    def track_rfi_export(
        self,
        session_id: str,
        document_name: str,
        export_format: str,
        total_questions: int,
        total_answers: int,
        headers: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Track RFI export event.
        
        Args:
            session_id: Browser session identifier
            document_name: Name of exported document
            export_format: Format (csv, html, pdf)
            total_questions: Number of questions exported
            total_answers: Number of answers exported
            headers: HTTP headers for user identification
            timestamp: Event timestamp (defaults to now)
            
        Returns:
            True if successfully logged, False otherwise
        """
        if not self.is_enabled():
            logger.debug("Analytics logging disabled or not configured")
            return False
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        user_info = self._get_user_info_from_headers(headers)
        event_id = str(uuid.uuid4())
        
        try:
            insert_sql = f"""
            INSERT INTO {self.rfi_exports_table} 
            (event_id, user_id, session_id, timestamp, ip_address, user_agent,
             document_name, export_format, total_questions, total_answers)
            VALUES (:event_id, :user_id, :session_id, :timestamp, :ip_address, :user_agent,
                    :document_name, :export_format, :total_questions, :total_answers)
            """
            
            # Convert parameters to StatementParameterListItem objects
            parameters = [
                StatementParameterListItem(name="event_id", value=event_id),
                StatementParameterListItem(name="user_id", value=user_info['user_id']),
                StatementParameterListItem(name="session_id", value=session_id),
                StatementParameterListItem(name="timestamp", value=timestamp.isoformat()),
                StatementParameterListItem(name="ip_address", value=user_info['ip_address']),
                StatementParameterListItem(name="user_agent", value=user_info['user_agent']),
                StatementParameterListItem(name="document_name", value=document_name),
                StatementParameterListItem(name="export_format", value=export_format),
                StatementParameterListItem(name="total_questions", value=str(total_questions)),
                StatementParameterListItem(name="total_answers", value=str(total_answers))
            ]
            
            self.workspace_client.statement_execution.execute_statement(
                statement=insert_sql,
                parameters=parameters,
                warehouse_id=self.settings.databricks.warehouse_id,
                wait_timeout="30s"
                )
            
            logger.debug(f"Tracked RFI export: {document_name} ({export_format})")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to track RFI export: {e}")
            return False
    
    def track_chat_session(
        self,
        session_id: str,
        chat_session_id: str,
        total_questions: int,
        total_responses: int,
        session_duration_seconds: float,
        final_status: str,
        headers: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Track chat session completion.
        
        Args:
            session_id: Browser session identifier
            chat_session_id: Unique chat session identifier
            total_questions: Number of questions in session
            total_responses: Number of responses in session
            session_duration_seconds: Total session duration
            final_status: Session status (completed, abandoned)
            headers: HTTP headers for user identification
            timestamp: Event timestamp (defaults to now)
            
        Returns:
            True if successfully logged, False otherwise
        """
        if not self.is_enabled():
            logger.debug("Analytics logging disabled or not configured")
            return False
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        user_info = self._get_user_info_from_headers(headers)
        event_id = str(uuid.uuid4())
        
        try:
            insert_sql = f"""
            INSERT INTO {self.chat_sessions_table} 
            (event_id, user_id, session_id, timestamp, ip_address, user_agent,
             chat_session_id, total_questions, total_responses, session_duration_seconds,
             final_status)
            VALUES (:event_id, :user_id, :session_id, :timestamp, :ip_address, :user_agent,
                    :chat_session_id, :total_questions, :total_responses, :session_duration_seconds,
                    :final_status)
            """
            
            # Convert parameters to StatementParameterListItem objects
            parameters = [
                StatementParameterListItem(name="event_id", value=event_id),
                StatementParameterListItem(name="user_id", value=user_info['user_id']),
                StatementParameterListItem(name="session_id", value=session_id),
                StatementParameterListItem(name="timestamp", value=timestamp.isoformat()),
                StatementParameterListItem(name="ip_address", value=user_info['ip_address']),
                StatementParameterListItem(name="user_agent", value=user_info['user_agent']),
                StatementParameterListItem(name="chat_session_id", value=chat_session_id),
                StatementParameterListItem(name="total_questions", value=str(total_questions)),
                StatementParameterListItem(name="total_responses", value=str(total_responses)),
                StatementParameterListItem(name="session_duration_seconds", value=str(session_duration_seconds)),
                StatementParameterListItem(name="final_status", value=final_status)
            ]
            
            self.workspace_client.statement_execution.execute_statement(
                statement=insert_sql,
                parameters=parameters,
                warehouse_id=self.settings.databricks.warehouse_id,
                wait_timeout="30s"
                )
            
            logger.debug(f"Tracked chat session: {chat_session_id} ({total_questions} questions)")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to track chat session: {e}")
            return False
    
    def track_chat_question(
        self,
        session_id: str,
        chat_session_id: str,
        question_data: Dict[str, Any],
        response_data: Dict[str, Any],
        headers: Optional[Dict[str, str]] = None,
        timestamp: Optional[datetime] = None
    ) -> bool:
        """Track individual chat question/response.
        
        Args:
            session_id: Browser session identifier
            chat_session_id: Unique chat session identifier
            question_data: Question details (text, length, etc.)
            response_data: Response details (text, model, timing, etc.)
            headers: HTTP headers for user identification
            timestamp: Event timestamp (defaults to now)
            
        Returns:
            True if successfully logged, False otherwise
        """
        if not self.is_enabled():
            logger.debug("Analytics logging disabled or not configured")
            return False
        
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        user_info = self._get_user_info_from_headers(headers)
        event_id = str(uuid.uuid4())
        
        try:
            insert_sql = f"""
            INSERT INTO {self.chat_questions_table} 
            (event_id, user_id, session_id, timestamp, ip_address, user_agent,
             chat_session_id, question, response, created_at)
            VALUES (:event_id, :user_id, :session_id, :timestamp, :ip_address, :user_agent,
                    :chat_session_id, 
                    from_json(:question, 'STRUCT<text:STRING,length_chars:INT>'),
                    from_json(:response, 'STRUCT<text:STRING,length_chars:INT,model_used:STRING,response_time_seconds:DOUBLE,copied_to_clipboard:BOOLEAN>'),
                    :created_at)
            """
            
            # Serialize complex data to JSON for STRUCT parameters
            import json
            question_json = json.dumps(question_data, ensure_ascii=False) if question_data else None
            response_json = json.dumps(response_data, ensure_ascii=False) if response_data else None
            
            # Convert parameters to StatementParameterListItem objects
            parameters = [
                StatementParameterListItem(name="event_id", value=event_id),
                StatementParameterListItem(name="user_id", value=user_info['user_id']),
                StatementParameterListItem(name="session_id", value=session_id),
                StatementParameterListItem(name="timestamp", value=timestamp.isoformat()),
                StatementParameterListItem(name="ip_address", value=user_info['ip_address']),
                StatementParameterListItem(name="user_agent", value=user_info['user_agent']),
                StatementParameterListItem(name="chat_session_id", value=chat_session_id),
                StatementParameterListItem(name="question", value=question_json),
                StatementParameterListItem(name="response", value=response_json),
                StatementParameterListItem(name="created_at", value=timestamp.isoformat())
            ]
            
            self.workspace_client.statement_execution.execute_statement(
                statement=insert_sql,
                parameters=parameters,
                warehouse_id=self.settings.databricks.warehouse_id,
                wait_timeout="30s"
                )
            
            logger.debug(f"Tracked chat question: {chat_session_id}")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to track chat question: {e}")
            return False
    
    def generate_event_id(self) -> str:
        """Generate a unique event ID."""
        return str(uuid.uuid4())
    
    def generate_chat_session_id(self) -> str:
        """Generate a unique chat session ID."""
        return str(uuid.uuid4())
