#!/usr/bin/env python3
"""Test script to verify Python imports for ARIA application."""

import sys
import os

# Add src to path if not already there
src_path = os.path.join(os.path.dirname(__file__), 'src')
if src_path not in sys.path:
    sys.path.insert(0, src_path)

try:
    print("Testing ARIA imports...")
    
    # Test core imports
    from aria.config import settings
    print("✅ aria.config.settings imported successfully")
    
    from aria.api.app import app
    print("✅ aria.api.app imported successfully")
    
    from aria.services.document_processor import DocumentProcessor
    print("✅ aria.services.document_processor imported successfully")
    
    from aria.services.question_extraction import QuestionExtractionService
    print("✅ aria.services.question_extraction imported successfully")
    
    from aria.services.answer_generation import AnswerGenerationService
    print("✅ aria.services.answer_generation imported successfully")
    
    from aria.services.chat_service import ChatService
    print("✅ aria.services.chat_service imported successfully")
    
    print("\n🎉 All imports successful!")
    sys.exit(0)
    
except ImportError as e:
    print(f"❌ Import failed: {e}")
    print(f"Python path: {sys.path}")
    sys.exit(1)
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    sys.exit(1)
