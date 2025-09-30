/**
 * ARIA Frontend JavaScript
 * Main application logic for the Node.js frontend
 */

// Configuration
const API_BASE = window.location.origin;

// Session ID persistence across page navigations
function getOrCreateSessionId() {
  let sessionId = localStorage.getItem('aria-session-id');
  if (!sessionId) {
    sessionId = 'session-' + Math.random().toString(36).substr(2, 9);
    localStorage.setItem('aria-session-id', sessionId);
  }
  return sessionId;
}

let sessionId = getOrCreateSessionId();

// Debug: log session ID
console.log('ARIA Session ID:', sessionId);

// Session management
async function getSession() {
  try {
    const response = await fetch(`${API_BASE}/api/session`, {
      headers: { 'X-Session-Id': sessionId }
    });
    return await response.json();
  } catch (error) {
    console.error('Error getting session:', error);
    return {};
  }
}

async function updateSession(data) {
  try {
    const payload = JSON.stringify(data);
    console.log(`📤 Updating session: ${Math.round(payload.length / 1024)}KB payload`);
    
    const response = await fetch(`${API_BASE}/api/session`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Session-Id': sessionId
      },
      body: payload
    });

    if (!response.ok) {
      const errorText = await response.text();
      const errorMessage = `Session update failed: ${response.status} ${response.statusText}`;
      console.error('❌ Session update failed:', {
        status: response.status,
        statusText: response.statusText,
        payloadSize: payload.length,
        errorResponse: errorText
      });
      
      // Show user-friendly error message
      if (response.status === 413) {
        showAlert('Session data too large. Please contact support if this persists.', 'error');
      } else if (response.status >= 500) {
        showAlert('Server error saving session. Please try again.', 'error');
      } else {
        showAlert(`Session save failed: ${response.statusText}`, 'error');
      }
      
      throw new Error(errorMessage);
    }
    
    console.log('✅ Session updated successfully');
    return true;
  } catch (error) {
    console.error('Error updating session:', error);
    
    // Show network error if not already handled above
    if (!error.message.includes('Session update failed:')) {
      showAlert('Network error saving session. Please check your connection.', 'error');
    }
    
    throw error; // Re-throw so callers can handle the error
  }
}

// Clear session (useful for testing or starting fresh)
function clearSession() {
  localStorage.removeItem('aria-session-id');
  sessionId = getOrCreateSessionId();
  console.log('Session cleared, new ID:', sessionId);
}

// UI utilities
function showAlert(message, type = 'info') {
  const alertsContainer = document.getElementById('alerts') || createAlertsContainer();
  const alert = document.createElement('div');
  alert.className = `alert alert-${type}`;
  alert.textContent = message;
  alertsContainer.appendChild(alert);
  
  // Auto-remove after 5 seconds
  setTimeout(() => alert.remove(), 5000);
}

function createAlertsContainer() {
  const container = document.createElement('div');
  container.id = 'alerts';
  container.style.position = 'fixed';
  container.style.top = '20px';
  container.style.right = '20px';
  container.style.zIndex = '9999';
  container.style.maxWidth = '400px';
  document.body.appendChild(container);
  return container;
}

function showLoading(element, show = true) {
  if (show) {
    const originalText = element.textContent;
    element.dataset.originalText = originalText;
    element.innerHTML = '<span class="loading"></span> Processing...';
    element.disabled = true;
  } else {
    element.textContent = element.dataset.originalText || 'Submit';
    element.disabled = false;
  }
}

// File upload handling
function setupFileUpload() {
  const fileInput = document.getElementById('fileInput');
  const dropZone = document.getElementById('dropZone');
  const fileInfo = document.getElementById('fileInfo');

  if (!fileInput || !dropZone) return;

  // Drag and drop
  dropZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    dropZone.classList.add('dragover');
  });

  dropZone.addEventListener('dragleave', () => {
    dropZone.classList.remove('dragover');
  });

  dropZone.addEventListener('drop', (e) => {
    e.preventDefault();
    dropZone.classList.remove('dragover');
    const files = e.dataTransfer.files;
    if (files.length > 0) {
      handleFileSelect(files[0]);
    }
  });

  // Click to upload
  dropZone.addEventListener('click', () => fileInput.click());
  fileInput.addEventListener('change', (e) => {
    if (e.target.files.length > 0) {
      handleFileSelect(e.target.files[0]);
    }
  });

  function handleFileSelect(file) {
    // Validate file type
    const allowedTypes = ['text/csv', 'text/html', 'text/plain'];
    const allowedExtensions = ['.csv', '.html', '.htm', '.txt'];
    
    const isValidType = allowedTypes.includes(file.type) || 
                       allowedExtensions.some(ext => file.name.toLowerCase().endsWith(ext));
    
    if (!isValidType) {
      showAlert('Please select a CSV, HTML, or text file.', 'error');
      return;
    }

    // Validate file size (50MB limit)
    if (file.size > 50 * 1024 * 1024) {
      showAlert('File size must be less than 50MB.', 'error');
      return;
    }

    // Auto-populate document name with full file name (including extension)
    const documentNameInput = document.getElementById('documentName');
    if (documentNameInput) {
      // Use full filename as document name (user can edit if desired)
      documentNameInput.value = file.name;
    }

    // Update UI
    if (fileInfo) {
      fileInfo.textContent = `Selected: ${file.name} (${formatFileSize(file.size)})`;
      fileInfo.style.display = 'block';
    }

    // Store file for upload
    fileInput.selectedFile = file;
  }
}

function formatFileSize(bytes) {
  if (bytes === 0) return '0 Bytes';
  const k = 1024;
  const sizes = ['Bytes', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

// Navigation
function navigateToMode(mode) {
  const routes = {
    'document': '/',
    'audit': '/audit',
    'chat': '/chat'
  };
  
  if (routes[mode]) {
    window.location.href = routes[mode];
  }
}

function navigateToStep(step) {
  const routes = {
    'upload': '/upload',
    'extract': '/extract', 
    'generate': '/generate',
    'download': '/download'
  };
  
  if (routes[step]) {
    window.location.href = routes[step];
  }
}

// Audit categories management
function setupAuditCategories() {
  const checkboxes = document.querySelectorAll('input[name="auditCategory"]');
  const submitBtn = document.getElementById('submitAudit');
  const rulesPreview = document.getElementById('rulesPreview');

  if (!checkboxes.length) return;

  checkboxes.forEach(checkbox => {
    checkbox.addEventListener('change', updateAuditUI);
  });

  function updateAuditUI() {
    const selected = Array.from(checkboxes).filter(cb => cb.checked);
    
    // Enable/disable submit button
    if (submitBtn) {
      submitBtn.disabled = selected.length === 0;
    }

    // Update rules preview
    if (rulesPreview) {
      const categories = selected.map(cb => cb.value);
      updateRulesPreview(categories);
    }
  }

  // Initial state
  updateAuditUI();
}

function updateRulesPreview(categories) {
  const rulesPreview = document.getElementById('rulesPreview');
  if (!rulesPreview) return;

  // This would normally fetch the actual rules from the backend
  // For now, show a simple preview
  const categoryLabels = {
    'factual_accuracy': 'Factual Accuracy',
    'tone': 'Tone & Professionalism', 
    'messaging': 'Messaging Alignment'
  };

  if (categories.length === 0) {
    rulesPreview.innerHTML = '<em>Select at least one audit category to see preview</em>';
    return;
  }

  const previewHTML = categories.map(cat => {
    const label = categoryLabels[cat] || cat;
    return `<strong>${label}</strong>: Active`;
  }).join('<br>');

  rulesPreview.innerHTML = previewHTML;
}

// Results display with tabs
function setupResultsTabs() {
  const tabs = document.querySelectorAll('.results-tab');
  const contents = document.querySelectorAll('.tab-content');

  tabs.forEach(tab => {
    tab.addEventListener('click', () => {
      const targetId = tab.dataset.tab;
      
      // Update tabs
      tabs.forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      
      // Update content
      contents.forEach(content => {
        content.style.display = content.id === targetId ? 'block' : 'none';
      });
    });
  });

  // Show first tab by default
  if (tabs.length > 0) {
    tabs[0].click();
  }
}

// Audit text submission
async function submitAuditText() {
  const textInput = document.getElementById('auditText');
  const categoriesInputs = document.querySelectorAll('input[name="auditCategory"]:checked');
  const guidelinesInput = document.getElementById('brandGuidelines');
  const submitBtn = document.getElementById('submitAudit');

  if (!textInput || !submitBtn) return;

  const text = textInput.value.trim();
  if (!text) {
    showAlert('Please enter text to audit.', 'error');
    return;
  }

  const categories = Array.from(categoriesInputs).map(input => input.value);
  if (categories.length === 0) {
    showAlert('Please select at least one audit category.', 'error');
    return;
  }

  showLoading(submitBtn);

  try {
    const response = await fetch(`${API_BASE}/api/audit-text`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Session-Id': sessionId
      },
      body: JSON.stringify({
        text,
        auditCategories: categories,
        brandGuidelines: guidelinesInput?.value || '',
        modelName: null
      })
    });

    const result = await response.json();

    if (result.success) {
      displayAuditResults(result);
      showAlert('Audit completed successfully!', 'success');
    } else {
      showAlert(result.error || 'Audit failed', 'error');
    }
  } catch (error) {
    console.error('Audit error:', error);
    showAlert('Failed to process audit request', 'error');
  } finally {
    showLoading(submitBtn, false);
  }
}

// Audit file submission
async function submitAuditFile() {
  const fileInput = document.getElementById('fileInput');
  const categoriesInputs = document.querySelectorAll('input[name="auditCategory"]:checked');
  const guidelinesInput = document.getElementById('brandGuidelines');
  const submitBtn = document.getElementById('submitAuditFile');

  if (!fileInput?.selectedFile || !submitBtn) return;

  const categories = Array.from(categoriesInputs).map(input => input.value);
  if (categories.length === 0) {
    showAlert('Please select at least one audit category.', 'error');
    return;
  }

  showLoading(submitBtn);

  try {
    const formData = new FormData();
    formData.append('file', fileInput.selectedFile);
    formData.append('auditCategories', JSON.stringify(categories));
    
    if (guidelinesInput?.value) {
      formData.append('brandGuidelines', guidelinesInput.value);
    }

    const response = await fetch(`${API_BASE}/api/audit-file`, {
      method: 'POST',
      headers: {
        'X-Session-Id': sessionId
      },
      body: formData
    });

    const result = await response.json();

    if (result.success) {
      displayAuditResults(result);
      showAlert('File audit completed successfully!', 'success');
    } else {
      showAlert(result.error || 'File audit failed', 'error');
    }
  } catch (error) {
    console.error('File audit error:', error);
    showAlert('Failed to process file audit request', 'error');
  } finally {
    showLoading(submitBtn, false);
  }
}

// Display audit results
function displayAuditResults(result) {
  const resultsContainer = document.getElementById('auditResults');
  if (!resultsContainer) return;

  // Show results section
  resultsContainer.style.display = 'block';
  
  // Show debug info
  const debugInfo = document.getElementById('debugInfo');
  const debugContent = document.getElementById('debugContent');
  if (debugInfo && debugContent) {
    debugInfo.style.display = 'block';
    debugContent.innerHTML = `
      <p><strong>Success:</strong> ${result.success}</p>
      <p><strong>Audit Items:</strong> ${result.info?.audit_items?.length || 0}</p>
      <p><strong>Original Text Length:</strong> ${result.info?.original_text?.length || 0}</p>
      <details>
        <summary>Raw Response</summary>
        <pre style="background: white; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-size: 12px; max-height: 200px; overflow-y: auto;">${JSON.stringify(result, null, 2)}</pre>
      </details>
    `;
  }

  // If there are structured results, populate the annotated view
  if (result.info && result.info.audit_items && result.info.original_text) {
    setupAnnotatedView(result.info.original_text, result.info.audit_items);
  } else {
    const annotatedTab = document.getElementById('annotatedResults');
    if (annotatedTab) {
      annotatedTab.innerHTML = '<div class="alert alert-warning">No audit items found to annotate.</div>';
    }
  }
}

// Setup annotated view with RecogitoJS
function setupAnnotatedView(documentText, auditItems) {
  const annotatedTab = document.getElementById('annotatedResults');
  if (!annotatedTab || !documentText) {
    console.error('Missing annotatedTab or documentText:', { annotatedTab: !!annotatedTab, documentText: !!documentText });
    return;
  }

  console.log('Setting up annotated view with:', { 
    textLength: documentText.length, 
    itemsCount: auditItems.length,
    items: auditItems 
  });

  // Clear previous content and add document container
  // IMPORTANT: Use plain text for RecogitoJS to avoid HTML interference
  annotatedTab.innerHTML = `
    <div style="margin-bottom: 15px;">
      <p><strong>Document Text with Audit Highlights:</strong></p>
      <p style="font-size: 14px; color: #666;">Found ${auditItems.length} audit items to highlight</p>
    </div>
    <div id="recogito-doc" class="recogito-container" style="border: 2px solid #e0e0e0; padding: 20px; border-radius: 8px; background: white; line-height: 1.6; font-size: 14px; white-space: pre-wrap;">
      ${documentText}
    </div>
    <div id="recogitoStatus" style="margin-top: 10px; padding: 10px; background: #f8f9fa; border-radius: 4px; font-size: 12px;">
      Initializing RecogitoJS...
    </div>
  `;

  // Load RecogitoJS and setup annotations
  loadRecogitoJS(() => {
    initializeRecogito(documentText, auditItems);
  });
}

function loadRecogitoJS(callback) {
  // Check if already loaded
  if (window.Recogito) {
    callback();
    return;
  }

  // Load RecogitoJS from CDN
  const script = document.createElement('script');
  script.src = 'https://cdn.jsdelivr.net/npm/@recogito/recogito-js@latest/dist/recogito.min.js';
  script.onload = callback;
  document.head.appendChild(script);

  // Also load CSS
  const link = document.createElement('link');
  link.rel = 'stylesheet';
  link.href = 'https://cdn.jsdelivr.net/npm/@recogito/recogito-js@latest/dist/recogito.min.css';
  document.head.appendChild(link);
}

function initializeRecogito(documentText, auditItems) {
  const container = document.getElementById('recogito-doc');
  const status = document.getElementById('recogitoStatus');
  
  if (!container || !window.Recogito) {
    console.error('Missing container or Recogito:', { container: !!container, Recogito: !!window.Recogito });
    if (status) status.textContent = 'Error: Missing container or RecogitoJS library';
    return;
  }

  try {
    if (status) status.textContent = 'Creating annotations...';
    
    // Create annotations from audit items
    const annotations = createAnnotations(documentText, auditItems);
    console.log('Created annotations:', annotations);
    
    if (status) status.textContent = 'Initializing RecogitoJS...';
    
    // Initialize Recogito
    const r = window.Recogito.init({
      content: container,
      readOnly: true
    });

    if (status) status.textContent = 'Adding annotations...';
    
    // Add annotations to Recogito
    let addedCount = 0;
    annotations.forEach((annotation, index) => {
      try {
        r.addAnnotation(annotation);
        addedCount++;
        console.log(`Added annotation ${index + 1}:`, annotation);
      } catch (error) {
        console.error(`Failed to add annotation ${index + 1}:`, error, annotation);
      }
    });

    // Handle annotation selection
    r.on('selectAnnotation', (annotation) => {
      showAnnotationDetails(annotation);
    });

    if (status) {
      status.innerHTML = `✅ RecogitoJS initialized successfully!<br>
        <strong>Created:</strong> ${annotations.length} annotations<br>
        <strong>Added:</strong> ${addedCount} annotations<br>
        <em>Click on highlighted text to see details.</em>`;
    }
    
    console.log(`Initialized Recogito with ${addedCount}/${annotations.length} annotations`);
  } catch (error) {
    console.error('Failed to initialize Recogito:', error);
    if (status) status.textContent = `Error initializing RecogitoJS: ${error.message}`;
    container.innerHTML = `
      <div class="alert alert-warning">
        <strong>Annotation view unavailable.</strong><br>
        Error: ${error.message}<br><br>
        <div style="background: #f8f9fa; padding: 10px; border-radius: 4px; margin-top: 10px;">
          ${documentText.replace(/\n/g, '<br>')}
        </div>
      </div>
    `;
  }
}

function normalizeQuotes(text) {
  // Comprehensive normalization of quote characters and punctuation
  return text
    // All possible quote variations to straight quotes
    .replace(/[\u201C\u201D\u201E\u201F\u2033\u2036\u3003\u201A\u201B\u2039\u203A]/g, '"') // Smart quotes: " " „ ‟ ″ ‶ 〃 ‚ ‛ ‹ ›
    .replace(/[\u2018\u2019\u201A\u201B\u2032\u2035\u2039\u203A]/g, "'") // Smart apostrophes: ' ' ‚ ‛ ′ ‵ ‹ ›
    .replace(/[\u2013\u2014\u2015\u2212]/g, '-')   // Em/en dashes: – — ― − 
    .replace(/[\u2026]/g, '...')   // Ellipsis: …
    .replace(/\s+/g, ' ')          // Multiple spaces to single space
    .trim();                       // Remove leading/trailing whitespace
}

function createAnnotations(documentText, auditItems) {
  const annotations = [];
  
  // Normalize the document text
  const normalizedDoc = normalizeQuotes(documentText);
  
  auditItems.forEach((item, index) => {
    // Use flagged_text instead of claim/rule (this is what our API returns)
    let flaggedText = item.flagged_text || item.claim || item.rule || '';
    if (!flaggedText) {
      console.warn(`No flagged text found for item ${index}:`, item);
      return;
    }

    console.log(`Processing item ${index}:`, { 
      originalFlaggedText: flaggedText, 
      itemKeys: Object.keys(item) 
    });

    // Show character encoding differences for debugging
    console.log('Character comparison:', {
      flaggedPreview: flaggedText.substring(0, 50),
      docPreview: normalizedDoc.substring(0, 50),
      // Focus on quote characters specifically (including Unicode ranges)
      flaggedQuotes: Array.from(flaggedText).filter(c => /[\u2018-\u203A\u2033\u2036\u3003"''`´]/.test(c)).map(c => `${c}(U+${c.charCodeAt(0).toString(16).toUpperCase().padStart(4, '0')})`),
      docQuotes: Array.from(normalizedDoc).filter(c => /[\u2018-\u203A\u2033\u2036\u3003"''`´]/.test(c)).map(c => `${c}(U+${c.charCodeAt(0).toString(16).toUpperCase().padStart(4, '0')})`)
    });

    // Normalize the flagged text
    const normalizedFlagged = normalizeQuotes(flaggedText);
    
    console.log('After normalization:', {
      originalFlagged: flaggedText.substring(0, 50),
      normalizedFlagged: normalizedFlagged.substring(0, 50),
      normalizedDoc: normalizedDoc.substring(0, 50),
      // Check if normalization fixed quote differences
      quotesChanged: flaggedText !== normalizedFlagged ? 'YES' : 'NO',
      lengthDiff: normalizedFlagged.length - flaggedText.length
    });
    
    // Try exact match first
    let startIndex = normalizedDoc.indexOf(normalizedFlagged);
    let actualMatchText = normalizedFlagged;
    
    console.log(`Exact match attempt: "${normalizedFlagged.substring(0, 30)}..." found at index ${startIndex}`);
    
    // If exact match fails, try case-insensitive
    if (startIndex === -1) {
      const lowerDoc = normalizedDoc.toLowerCase();
      const lowerFlagged = normalizedFlagged.toLowerCase();
      startIndex = lowerDoc.indexOf(lowerFlagged);
      
      if (startIndex !== -1) {
        // Get the actual text from the document to preserve casing
        actualMatchText = normalizedDoc.substring(startIndex, startIndex + normalizedFlagged.length);
        console.log(`Found case-insensitive match for "${flaggedText.substring(0, 50)}..."`);
      }
    }
    
    // If still no match, try partial matching (first 50% of the text)
    if (startIndex === -1 && normalizedFlagged.length > 20) {
      const partialText = normalizedFlagged.substring(0, Math.floor(normalizedFlagged.length * 0.6));
      startIndex = normalizedDoc.indexOf(partialText);
      
      if (startIndex !== -1) {
        // Find the end of the sentence or logical boundary
        const maxLength = Math.min(normalizedFlagged.length * 1.2, normalizedDoc.length - startIndex);
        let endSearch = startIndex + maxLength;
        
        // Look for natural sentence boundaries
        const boundaries = ['. ', '.\n', '.\r', '!\n', '?\n'];
        for (const boundary of boundaries) {
          const boundaryIndex = normalizedDoc.indexOf(boundary, startIndex + partialText.length);
          if (boundaryIndex !== -1 && boundaryIndex < endSearch) {
            endSearch = boundaryIndex + 1;
            break;
          }
        }
        
        actualMatchText = normalizedDoc.substring(startIndex, endSearch);
        console.log(`Found partial match for "${flaggedText.substring(0, 50)}..." using first 60%`);
      }
    }
    
    if (startIndex === -1) {
      console.warn(`Could not find flagged text in document: "${flaggedText.substring(0, 100)}..."`);
      console.log('Document preview:', normalizedDoc.substring(0, 200) + '...');
      return;
    }

    const endIndex = startIndex + actualMatchText.length;
    
    // Create Web Annotation - use original document text positions
    const annotation = {
      '@context': 'http://www.w3.org/ns/anno.jsonld',
      type: 'Annotation',
      id: `annotation-${index}`,
      body: [
        {
          type: 'TextualBody',
          purpose: 'tagging',
          value: `verdict:${item.verdict || item.status || 'flagged'}`
        },
        {
          type: 'TextualBody', 
          purpose: 'commenting',
          value: item.reason || item.explanation || 'No explanation provided'
        }
      ],
      target: {
        selector: [
          {
            type: 'TextPositionSelector',
            start: startIndex,
            end: endIndex
          },
          {
            type: 'TextQuoteSelector',
            exact: actualMatchText,
            prefix: normalizedDoc.slice(Math.max(0, startIndex - 40), startIndex),
            suffix: normalizedDoc.slice(endIndex, Math.min(normalizedDoc.length, endIndex + 40))
          }
        ]
      }
    };

    console.log(`Created annotation for "${actualMatchText.substring(0, 50)}..." at position ${startIndex}-${endIndex}`);
    annotations.push(annotation);
  });

  console.log(`Created ${annotations.length} annotations from ${auditItems.length} audit items`);
  return annotations;
}

function showAnnotationDetails(annotation) {
  // Extract details from annotation body
  const bodies = annotation.body || [];
  let verdict = '';
  let explanation = '';

  bodies.forEach(body => {
    if (body.purpose === 'tagging' && body.value.startsWith('verdict:')) {
      verdict = body.value.replace('verdict:', '');
    } else if (body.purpose === 'commenting') {
      explanation = body.value;
    }
  });

  // Show in a modal or sidebar (implement as needed)
  console.log('Annotation details:', { verdict, explanation });
}

// Initialize app when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
  // Setup common functionality
  setupFileUpload();
  setupAuditCategories();
  setupResultsTabs();

  // Page-specific initialization
  const path = window.location.pathname;
  
  if (path === '/audit') {
    // Audit page specific setup
    document.getElementById('submitAudit')?.addEventListener('click', submitAuditText);
    document.getElementById('submitAuditFile')?.addEventListener('click', submitAuditFile);
  }
  
  // Load session state
  getSession().then(session => {
    console.log('Session loaded:', session);
  });
});

// Export functions for use in HTML
window.ARIA = {
  navigateToMode,
  navigateToStep,
  submitAuditText,
  submitAuditFile,
  showAlert
};
