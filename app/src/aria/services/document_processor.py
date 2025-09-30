"""Document processing service for ARIA application.

This module handles document analysis, validation, and preparation
for question extraction.
"""

import os
import pandas as pd
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
import io

from aria.core.logging_config import get_logger
from aria.config.constants import SUPPORTED_FILE_TYPES, MAX_FILE_SIZE_MB

logger = get_logger(__name__)


class DocumentProcessor:
    """Service for processing and analyzing uploaded documents."""
    
    def __init__(self) -> None:
        """Initialize the document processor."""
        self.supported_types = SUPPORTED_FILE_TYPES
        self.max_size_mb = MAX_FILE_SIZE_MB
    
    def validate_file(self, file_path: str) -> Tuple[bool, List[str]]:
        """Validate an uploaded file.
        
        Args:
            file_path: Path to the file to validate
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check if file exists
        if not os.path.exists(file_path):
            errors.append(f"File not found: {file_path}")
            return False, errors
        
        # Check file extension
        file_extension = Path(file_path).suffix.lower()
        if file_extension not in self.supported_types:
            errors.append(f"Unsupported file type: {file_extension}. Supported types: {', '.join(self.supported_types)}")
        
        # Check file size
        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        if file_size_mb > self.max_size_mb:
            errors.append(f"File too large: {file_size_mb:.1f}MB. Maximum size: {self.max_size_mb}MB")
        
        # Check if file is readable
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                f.read(100)  # Try to read first 100 characters
        except UnicodeDecodeError:
            # Try with different encoding
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    f.read(100)
            except Exception as e:
                errors.append(f"File encoding error: {str(e)}")
        except Exception as e:
            errors.append(f"File read error: {str(e)}")
        
        return len(errors) == 0, errors
    

    

    

    
    def prepare_for_extraction(self, file_path: str) -> Dict[str, Any]:
        """Prepare a file for question extraction.
        
        Args:
            file_path: Path to the file to prepare
            
        Returns:
            Dictionary containing preparation results and extracted content
        """
        preparation = {
            "file_path": file_path,
            "ready_for_extraction": False,
            "extraction_method": "",
            "content": "",
            "metadata": {},
            "errors": []
        }
        
        try:
            # Validate the file first
            is_valid, errors = self.validate_file(file_path)
            if not is_valid:
                preparation["errors"] = errors
                return preparation
            
            # Get basic file info
            file_name = os.path.basename(file_path)
            file_type = Path(file_path).suffix.lower()
            
            preparation["metadata"] = {
                "file_name": file_name,
                "file_type": file_type
            }
            
            # Prepare content based on file type
            if file_type == ".csv":
                preparation.update(self._prepare_csv(file_path))
            elif file_type in [".html", ".htm"]:
                preparation.update(self._prepare_html(file_path))
            else:
                preparation["errors"].append(f"Unsupported file type for extraction: {file_type}")
            
            logger.info(f"File preparation completed for {file_name}")
            
        except Exception as e:
            logger.error(f"Error preparing file {file_path}: {str(e)}")
            preparation["errors"].append(f"Preparation error: {str(e)}")
        
        return preparation

    def read_text_from_any(self, file_path: str) -> Dict[str, Any]:
        """Read raw text from CSV or HTML to support document checking.
        
        Args:
            file_path: Path to the file
        
        Returns:
            Dict with keys: ready (bool), content (str), metadata (dict), errors (list)
        """
        result: Dict[str, Any] = {
            "ready": False,
            "content": "",
            "metadata": {},
            "errors": []
        }
        try:
            is_valid, errors = self.validate_file(file_path)
            if not is_valid:
                result["errors"] = errors
                return result
            file_type = Path(file_path).suffix.lower()
            result["metadata"] = {"file_type": file_type, "file_name": os.path.basename(file_path)}
            if file_type == ".csv":
                df = pd.read_csv(file_path)
                text_columns = [c for c in df.columns if df[c].dtype == object]
                joined = []
                for col in text_columns:
                    joined.append(f"# Column: {col}\n" + "\n".join([str(v) for v in df[col].dropna().tolist()]))
                result["content"] = "\n\n".join(joined)
                result["ready"] = True
            elif file_type in [".html", ".htm"]:
                with open(file_path, "r", encoding="utf-8") as f:
                    html = f.read()
                try:
                    from bs4 import BeautifulSoup
                    soup = BeautifulSoup(html, "html.parser")
                    for s in soup(["script", "style"]):
                        s.decompose()
                    text = soup.get_text(" ", strip=True)
                except Exception:
                    import re
                    text = re.sub(r"<[^>]+>", " ", html)
                result["content"] = text
                result["ready"] = True
            else:
                result["errors"].append(f"Unsupported file type: {file_type}")
        except Exception as exc:
            result["errors"].append(str(exc))
        return result
    
    def _prepare_csv(self, file_path: str) -> Dict[str, Any]:
        """Prepare a CSV file for question extraction.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dictionary containing CSV preparation results
        """
        preparation = {
            "ready_for_extraction": False,
            "extraction_method": "csv_direct",
            "content": "",
            "question_column": None
        }
        
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            # Look for question columns
            question_keywords = ['question', 'questions', 'query', 'queries', 'q']
            question_columns = []
            
            for col in df.columns:
                if any(keyword in col.lower() for keyword in question_keywords):
                    question_columns.append(col)
            
            if not question_columns:
                preparation["errors"] = ["No question columns found in CSV file"]
                return preparation
            
            # Use the first question column found
            question_column = question_columns[0]
            preparation["question_column"] = question_column
            
            # Read the questions from the CSV
            questions = df[question_column].dropna().tolist()
            
            # Convert to text format for consistency
            preparation["content"] = "\n".join([f"Q{i+1}: {q}" for i, q in enumerate(questions)])
            preparation["ready_for_extraction"] = True
            
            logger.info(f"CSV prepared: {len(questions)} questions from column '{question_column}'")
            
        except Exception as e:
            logger.error(f"Error preparing CSV: {str(e)}")
            preparation["errors"] = [f"CSV preparation error: {str(e)}"]
        
        return preparation
    
    def _prepare_html(self, file_path: str) -> Dict[str, Any]:
        """Prepare an HTML file for question extraction.
        
        Args:
            file_path: Path to the HTML file
            
        Returns:
            Dictionary containing HTML preparation results
        """
        preparation = {
            "ready_for_extraction": False,
            "extraction_method": "ai_extraction",
            "content": ""
        }
        
        try:
            # Read the HTML content
            with open(file_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # For AI extraction, we'll pass the HTML content directly
            # The AI service will handle the actual extraction
            preparation["content"] = html_content
            preparation["ready_for_extraction"] = True
            
            logger.info(f"HTML prepared: {len(html_content)} characters ready for AI extraction")
            
        except Exception as e:
            logger.error(f"Error preparing HTML: {str(e)}")
            preparation["errors"] = [f"HTML preparation error: {str(e)}"]
        
        return preparation 