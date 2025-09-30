#!/usr/bin/env python3
"""
Document Diff Analyzer
A simple script to compare document versions and extract meaningful metrics.
"""

import os
import re
from pathlib import Path
from typing import List, Tuple, Dict, Any

# Core libraries
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from diff_match_patch import diff_match_patch

# Document processing libraries
from docx import Document
from bs4 import BeautifulSoup


class DocumentDiffAnalyzer:
    """Simple document comparison analyzer using diff-match-patch."""
    
    def __init__(self):
        self.dmp = diff_match_patch()
        
    def extract_text_from_file(self, file_path: str) -> str:
        """Extract plain text from various file formats."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
            
        if file_path.suffix.lower() == '.docx':
            return self._extract_from_docx(file_path)
        elif file_path.suffix.lower() == '.html':
            return self._extract_from_html(file_path)
        elif file_path.suffix.lower() == '.txt':
            return self._extract_from_txt(file_path)
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    def _extract_from_docx(self, file_path: Path) -> str:
        """Extract text from .docx file."""
        doc = Document(file_path)
        text_parts = []
        
        for paragraph in doc.paragraphs:
            if paragraph.text.strip():
                text_parts.append(paragraph.text)
                
        return '\n'.join(text_parts)
    
    def _extract_from_html(self, file_path: Path) -> str:
        """Extract text from .html file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'html.parser')
            
        # Remove script and style elements
        for script in soup(["script", "style"]):
            script.extract()
            
        text = soup.get_text()
        # Clean up whitespace
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)
        
        return text
    
    def _extract_from_txt(self, file_path: Path) -> str:
        """Extract text from .txt file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    
    def normalize_text(self, text: str) -> str:
        """Normalize whitespace and basic punctuation."""
        # Replace multiple whitespace with single space
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text
    
    def compute_diffs(self, original_text: str, revised_text: str) -> List[Tuple[int, str]]:
        """Compute diffs between two texts."""
        # Normalize texts
        original_text = self.normalize_text(original_text)
        revised_text = self.normalize_text(revised_text)
        
        # Compute diffs
        diffs = self.dmp.diff_main(original_text, revised_text)
        self.dmp.diff_cleanupSemantic(diffs)
        
        return diffs
    
    def extract_metrics(self, diffs: List[Tuple[int, str]], original_text: str) -> Dict[str, Any]:
        """Extract word-level metrics from diffs."""
        original_word_count = len(original_text.split()) if original_text.strip() else 1
        
        inserted_words = 0
        deleted_words = 0
        equal_words = 0
        
        for op, text in diffs:
            word_count = len(text.split()) if text.strip() else 0
            
            if op == -1:  # Delete
                deleted_words += word_count
            elif op == 1:  # Insert
                inserted_words += word_count
            elif op == 0:  # Equal
                equal_words += word_count
        
        total_changes = inserted_words + deleted_words
        percent_changed = (total_changes / original_word_count) * 100 if original_word_count > 0 else 0
        
        return {
            'original_word_count': original_word_count,
            'inserted_words': inserted_words,
            'deleted_words': deleted_words,
            'equal_words': equal_words,
            'total_changes': total_changes,
            'percent_changed': round(percent_changed, 2)
        }
    
    def analyze_files(self, original_file: str, revised_file: str) -> Dict[str, Any]:
        """Complete analysis of two document files."""
        # Extract texts
        print(f"Extracting text from {original_file}...")
        original_text = self.extract_text_from_file(original_file)
        
        print(f"Extracting text from {revised_file}...")
        revised_text = self.extract_text_from_file(revised_file)
        
        # Compute diffs
        print("Computing differences...")
        diffs = self.compute_diffs(original_text, revised_text)
        
        # Extract metrics
        print("Extracting metrics...")
        metrics = self.extract_metrics(diffs, original_text)
        
        # Add file information
        metrics['original_file'] = Path(original_file).name
        metrics['revised_file'] = Path(revised_file).name
        
        return {
            'metrics': metrics,
            'diffs': diffs,
            'original_text': original_text,
            'revised_text': revised_text
        }
    
    def print_summary(self, analysis_result: Dict[str, Any]):
        """Print a summary of the analysis."""
        metrics = analysis_result['metrics']
        
        print("\n" + "="*50)
        print("DOCUMENT COMPARISON SUMMARY")
        print("="*50)
        print(f"Original file: {metrics['original_file']}")
        print(f"Revised file: {metrics['revised_file']}")
        print(f"Original word count: {metrics['original_word_count']:,}")
        print(f"Words inserted: {metrics['inserted_words']:,}")
        print(f"Words deleted: {metrics['deleted_words']:,}")
        print(f"Total changes: {metrics['total_changes']:,}")
        print(f"Percent changed: {metrics['percent_changed']:.2f}%")
        print("="*50)
    
    def create_visualization(self, analysis_result: Dict[str, Any], save_path: str = None):
        """Create a simple visualization of the metrics."""
        metrics = analysis_result['metrics']
        
        # Create figure with subplots
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))
        
        # Word count comparison
        categories = ['Original', 'Equal', 'Inserted', 'Deleted']
        values = [
            metrics['original_word_count'],
            metrics['equal_words'],
            metrics['inserted_words'],
            metrics['deleted_words']
        ]
        colors = ['lightblue', 'lightgreen', 'lightcoral', 'lightsalmon']
        
        ax1.bar(categories, values, color=colors)
        ax1.set_title('Word Count Breakdown')
        ax1.set_ylabel('Word Count')
        
        # Changes pie chart
        if metrics['total_changes'] > 0:
            change_labels = ['Unchanged', 'Changed']
            change_values = [
                metrics['equal_words'],
                metrics['total_changes']
            ]
            ax2.pie(change_values, labels=change_labels, autopct='%1.1f%%', startangle=90)
            ax2.set_title(f'Document Changes\n({metrics["percent_changed"]:.1f}% modified)')
        else:
            ax2.text(0.5, 0.5, 'No Changes\nDetected', ha='center', va='center', 
                    transform=ax2.transAxes, fontsize=14)
            ax2.set_title('Document Changes')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Visualization saved to: {save_path}")
        
        plt.show()
    
    def save_detailed_report(self, analysis_result: Dict[str, Any], output_file: str = "diff_report.txt"):
        """Save a detailed report of the differences."""
        metrics = analysis_result['metrics']
        diffs = analysis_result['diffs']
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("DOCUMENT DIFF ANALYSIS REPORT\n")
            f.write("="*50 + "\n\n")
            
            # Summary
            f.write("SUMMARY:\n")
            f.write(f"Original file: {metrics['original_file']}\n")
            f.write(f"Revised file: {metrics['revised_file']}\n")
            f.write(f"Original word count: {metrics['original_word_count']:,}\n")
            f.write(f"Words inserted: {metrics['inserted_words']:,}\n")
            f.write(f"Words deleted: {metrics['deleted_words']:,}\n")
            f.write(f"Total changes: {metrics['total_changes']:,}\n")
            f.write(f"Percent changed: {metrics['percent_changed']:.2f}%\n\n")
            
            # Detailed diffs
            f.write("DETAILED CHANGES:\n")
            f.write("-" * 30 + "\n")
            
            for i, (op, text) in enumerate(diffs):
                if op == -1:
                    f.write(f"[DELETED] {text}\n")
                elif op == 1:
                    f.write(f"[INSERTED] {text}\n")
                # Skip unchanged text for brevity
        
        print(f"Detailed report saved to: {output_file}")