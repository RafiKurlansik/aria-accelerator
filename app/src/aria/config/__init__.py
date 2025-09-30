"""Configuration module for ARIA application.

This module provides centralized configuration management with environment variable
loading, validation, and type safety using a unified configuration approach.
"""

from .unified_config import settings as unified_settings, UnifiedSettings

# Legacy settings no longer imported to avoid dual initialization
LegacySettings = None

# Export the unified settings as the primary settings object
settings = unified_settings

__all__ = ["settings", "UnifiedSettings", "LegacySettings"]
