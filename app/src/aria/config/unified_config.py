"""Unified configuration management for ARIA application.

This module consolidates all configuration sources into a single, consistent interface
with proper environment variable loading, YAML configuration, and validation.
"""

import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any, Union
from pydantic import BaseModel, validator, Field
from dotenv import load_dotenv

# Import BaseSettings with fallback for deployment environments
try:
    from pydantic_settings import BaseSettings
except ImportError:
    print("[Config] WARNING: pydantic_settings not found, attempting to install...")
    try:
        import subprocess
        import sys
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pydantic-settings==2.6.1"])
        from pydantic_settings import BaseSettings
        print("[Config] Successfully installed and imported pydantic_settings")
    except Exception as e:
        print(f"[Config] CRITICAL: Failed to install pydantic_settings: {e}")
        print("[Config] Attempting fallback import from pydantic (legacy mode)")
        try:
            from pydantic import BaseSettings
            print("[Config] WARNING: Using legacy BaseSettings from pydantic - this may not work correctly")
        except ImportError:
            print("[Config] FATAL: Cannot import BaseSettings from any source")
            raise ImportError(
                "Cannot import BaseSettings. Please ensure pydantic-settings is installed: "
                "pip install pydantic-settings"
            )

# Load environment variables from .env file only in local development
# In Databricks Apps, all environment variables are provided automatically
if not os.getenv('DATABRICKS_CLIENT_ID'):
    # Only load .env if we're not in a Databricks Apps environment
    load_dotenv()


class DatabaseSettings(BaseSettings):
    """Databricks workspace configuration."""
    
    host: str = Field(..., description="Databricks workspace host URL")
    token: Optional[str] = Field(None, description="Personal access token (for local dev)")
    warehouse_id: Optional[str] = Field(None, description="SQL warehouse ID for query execution")
    
    class Config:
        env_prefix = "DATABRICKS_"
        case_sensitive = False
    
    @validator("host")
    def normalize_host_url(cls, v: str) -> str:
        """Ensure the host URL has the proper https:// prefix."""
        if not v:
            raise ValueError("Databricks host URL is required")
        
        if not v.startswith(('http://', 'https://')):
            return f"https://{v}"
        return v
    
    def get_model_endpoint_url(self, model_name: str) -> str:
        """Construct the full endpoint URL for a given model name."""
        return f"{self.host}/serving-endpoints/{model_name}/invocations"


class ApplicationSettings(BaseSettings):
    """General application configuration."""
    
    debug: bool = Field(False, description="Enable debug mode")
    development_mode: bool = Field(False, description="Enable development mode")
    max_file_size_mb: int = Field(50, description="Maximum file upload size in MB")
    session_timeout_hours: int = Field(24, description="Session timeout in hours")
    
    class Config:
        env_prefix = "APP_"
        case_sensitive = False


class TrackingSettings(BaseSettings):
    """Usage tracking configuration."""
    
    enabled: bool = Field(False, description="Enable usage tracking") 
    volume_path: str = Field("/Volumes/main/default/tracking", description="Volume path for tracking data")
    
    class Config:
        env_prefix = "TRACKING_"
        case_sensitive = False


class ServiceConfig(BaseModel):
    """Configuration for a single backend service."""
    
    model: str = Field(..., description="Model name for this service")
    prompt: Optional[str] = Field(None, description="Custom prompt for this service")
    enabled: bool = Field(True, description="Whether this service is enabled")


class UnifiedSettings:
    """Centralized configuration manager that consolidates all sources."""
    
    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """Initialize unified configuration.
        
        Args:
            config_path: Optional path to YAML configuration file
        """
        print("[Config] Initializing unified ARIA configuration...")
        
        # Load Pydantic settings from environment variables
        try:
            self.databricks = DatabaseSettings()
            self.app = ApplicationSettings()
            self.tracking = TrackingSettings()
            print("[Config] ✅ Environment-based settings loaded")
        except Exception as e:
            print(f"[Config] ❌ Failed to load environment settings: {e}")
            raise
        
        # Load service configuration from YAML
        try:
            self._load_yaml_config(config_path)
            print("[Config] ✅ YAML service configuration loaded")
        except Exception as e:
            print(f"[Config] ❌ Failed to load YAML config: {e}")
            raise
        
        # Initialize computed properties
        self._init_computed_properties()
        print("[Config] ✅ Configuration initialization complete")
    
    def _load_yaml_config(self, config_path: Optional[Union[str, Path]] = None) -> None:
        """Load service configuration from YAML file."""
        if config_path is None:
            config_path = Path(__file__).parent / "backend_services.yaml"
        
        config_path = Path(config_path)
        
        try:
            if not config_path.exists():
                raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
            with open(config_path, 'r') as f:
                yaml_config = yaml.safe_load(f)
            
            # Parse service configurations
            self.services = {}
            for service_name, service_config in yaml_config.items():
                if isinstance(service_config, dict) and 'model' in service_config:
                    self.services[service_name] = ServiceConfig(**service_config)
            
            # Legacy compatibility - direct access to model names
            self.question_extraction_model = self.services.get('question_extraction', ServiceConfig(model='databricks-claude-sonnet-4')).model
            self.answer_generation_model = self.services.get('answer_generation', ServiceConfig(model='agents_users-rafi_kurlansik-auto_rfi')).model
            self.document_checker_model = self.services.get('document_checker', ServiceConfig(model='agents_users-rafi_kurlansik-audit_agent')).model
            self.chat_model = self.services.get('chat', ServiceConfig(model='agents_users-rafi_kurlansik-auto_rfi')).model
            
            # Store raw YAML for backwards compatibility
            self._raw_yaml = yaml_config
            
        except Exception as e:
            print(f"[Config] Warning: Failed to load YAML config: {e}")
            print("[Config] Using default service configuration")
            self._load_default_service_config()
    
    def _load_default_service_config(self) -> None:
        """Load default service configuration as fallback."""
        default_configs = {
            'question_extraction': ServiceConfig(model='databricks-claude-sonnet-4'),
            'answer_generation': ServiceConfig(model='agents_users-rafi_kurlansik-auto_rfi'),
            'document_checker': ServiceConfig(model='agents_users-rafi_kurlansik-audit_agent'),
            'chat': ServiceConfig(model='agents_users-rafi_kurlansik-auto_rfi'),
        }
        
        self.services = default_configs
        self.question_extraction_model = default_configs['question_extraction'].model
        self.answer_generation_model = default_configs['answer_generation'].model
        self.document_checker_model = default_configs['document_checker'].model
        self.chat_model = default_configs['chat'].model
        self._raw_yaml = {}
    
    def _init_computed_properties(self) -> None:
        """Initialize computed properties and endpoint URLs."""
        try:
            # Model endpoint URLs
            self.question_extraction_endpoint = self.databricks.get_model_endpoint_url(self.question_extraction_model)
            self.answer_generation_endpoint = self.databricks.get_model_endpoint_url(self.answer_generation_model)
            self.document_checker_endpoint = self.databricks.get_model_endpoint_url(self.document_checker_model)
            self.chat_endpoint = self.databricks.get_model_endpoint_url(self.chat_model)
            
        except Exception as e:
            print(f"[Config] Warning: Failed to initialize computed properties: {e}")
    
    def get_auth_headers(self) -> Optional[Dict[str, str]]:
        """Get authentication headers for API calls.
        
        Uses Databricks SDK's authentication system which supports:
        - Service principal authentication (Databricks Apps)
        - Personal access tokens (environment variables)
        - Databricks CLI authentication (databricks auth login)
        - Azure CLI / AWS CLI authentication
        
        Returns:
            Dictionary with authorization headers or None if no auth available
        """
        try:
            # Get workspace client with SDK's built-in authentication
            workspace_client = self.get_workspace_client()
            if not workspace_client:
                return None
            
            # Use the SDK's authentication mechanism (supports CLI auth)
            # Method 1: Try to get auth headers directly from the config
            try:
                auth_provider = workspace_client.config.authenticate()
                
                # Check if auth_provider is already a dict (some auth methods return headers directly)
                if isinstance(auth_provider, dict) and 'Authorization' in auth_provider:
                    return {
                        'Authorization': auth_provider['Authorization'],
                        'Content-Type': 'application/json'
                    }
                
                # If it's callable, try to call it with a dummy request
                elif callable(auth_provider):
                    import requests
                    dummy_request = requests.Request('GET', f"{self.databricks.host}/api/2.0/clusters/list")
                    authenticated_request = auth_provider(dummy_request)
                    
                    if authenticated_request.headers and 'Authorization' in authenticated_request.headers:
                        return {
                            'Authorization': authenticated_request.headers['Authorization'],
                            'Content-Type': 'application/json'
                        }
                
            except Exception:
                pass
            
            # Method 2: Fallback to direct token access for PAT authentication
            try:
                if hasattr(workspace_client.config, 'token') and workspace_client.config.token:
                    return {
                        'Authorization': f'Bearer {workspace_client.config.token}',
                        'Content-Type': 'application/json'
                    }
            except Exception:
                pass
            
            return None
        except Exception:
            return None
    
    def get_workspace_client(self):
        """Get a Databricks workspace client with SDK authentication.
        
        The SDK automatically handles multiple authentication methods:
        - Environment variables (DATABRICKS_TOKEN, DATABRICKS_HOST)  
        - CLI authentication (databricks auth login)
        - Azure CLI / AWS CLI
        - Instance profiles
        
        Returns:
            WorkspaceClient instance or None if authentication fails
        """
        try:
            from databricks.sdk import WorkspaceClient
            
            # Let the SDK handle authentication automatically
            # It will use CLI auth if available, otherwise env vars
            client = WorkspaceClient()
            return client
        except Exception as e:
            print(f"[Config] Failed to create workspace client: {e}")
            return None
    
    def get_service_config(self, service_name: str) -> Optional[ServiceConfig]:
        """Get configuration for a specific service.
        
        Args:
            service_name: Name of the service (e.g., 'question_extraction')
            
        Returns:
            ServiceConfig object or None if service not found
        """
        return self.services.get(service_name)
    
    def is_service_enabled(self, service_name: str) -> bool:
        """Check if a service is enabled.
        
        Args:
            service_name: Name of the service
            
        Returns:
            True if service is enabled, False otherwise
        """
        service_config = self.get_service_config(service_name)
        return service_config.enabled if service_config else False


# Global settings instance
settings = UnifiedSettings()
