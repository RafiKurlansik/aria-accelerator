/**
 * ARIA Frontend Error Handler
 * Provides consistent error display and user guidance across the application
 */

class ARIAErrorDisplay {
    constructor() {
        this.retryAttempts = new Map(); // Track retry attempts per operation
        this.maxRetries = 3;
    }

    /**
     * Display error with enhanced user guidance
     * @param {string} error - Error message from backend
     * @param {string} operation - What operation failed (e.g., "Chat", "Question Extraction")
     * @param {HTMLElement} targetElement - Where to show the error (optional)
     * @param {Function} retryCallback - Function to call for retry (optional)
     */
    showError(error, operation = 'Operation', targetElement = null, retryCallback = null) {
        // Parse enhanced error message
        const errorInfo = this.parseErrorMessage(error);
        
        // Create error display
        const errorHtml = this.createErrorDisplay(errorInfo, operation, retryCallback);
        
        if (targetElement) {
            // Show error in specific element
            targetElement.innerHTML = errorHtml;
            targetElement.scrollIntoView({ behavior: 'smooth', block: 'center' });
        } else {
            // Show error as alert/modal
            this.showErrorModal(errorInfo, operation, retryCallback);
        }
        
        // Log error for debugging
        console.error(`ARIA Error [${operation}]:`, error);
    }

    /**
     * Parse error message to extract structured information
     * @param {string} error - Error message
     * @returns {Object} Parsed error information
     */
    parseErrorMessage(error) {
        const lines = error.split('\n');
        const mainMessage = lines[0] || error;
        
        // Extract suggested actions if present
        const actionsStart = error.indexOf('💡 **What you can do:**');
        let suggestedActions = [];
        
        if (actionsStart !== -1) {
            const actionsText = error.substring(actionsStart);
            const actionLines = actionsText.split('\n').slice(1); // Skip the header
            
            suggestedActions = actionLines
                .filter(line => line.trim().match(/^\d+\./))
                .map(line => line.trim().replace(/^\d+\.\s*/, ''));
        }
        
        // Extract retry information
        const retryMatch = error.match(/You can try again in (\d+) seconds/);
        const retryDelay = retryMatch ? parseInt(retryMatch[1]) : null;
        
        // Determine error category from emoji/content
        let category = 'unknown';
        let severity = 'medium';
        
        if (mainMessage.includes('🔐') || mainMessage.includes('Authentication')) {
            category = 'authentication';
            severity = 'high';
        } else if (mainMessage.includes('🚫') || mainMessage.includes('permission')) {
            category = 'authorization';
            severity = 'high';
        } else if (mainMessage.includes('🌐') || mainMessage.includes('network')) {
            category = 'network';
            severity = 'medium';
        } else if (mainMessage.includes('⏱️') || mainMessage.includes('timeout')) {
            category = 'timeout';
            severity = 'medium';
        } else if (mainMessage.includes('🔧') || mainMessage.includes('unavailable')) {
            category = 'service_unavailable';
            severity = 'medium';
        } else if (mainMessage.includes('⏳') || mainMessage.includes('rate limit')) {
            category = 'rate_limit';
            severity = 'medium';
        }
        
        return {
            message: mainMessage,
            category,
            severity,
            suggestedActions,
            retryDelay,
            isRetryable: retryDelay !== null || category === 'timeout' || category === 'service_unavailable'
        };
    }

    /**
     * Create HTML for error display
     * @param {Object} errorInfo - Parsed error information
     * @param {string} operation - Operation name
     * @param {Function} retryCallback - Retry function
     * @returns {string} HTML content
     */
    createErrorDisplay(errorInfo, operation, retryCallback) {
        const severityClass = this.getSeverityClass(errorInfo.severity);
        const categoryIcon = this.getCategoryIcon(errorInfo.category);
        
        let html = `
            <div class="aria-error-display ${severityClass}">
                <div class="error-header">
                    <span class="error-icon">${categoryIcon}</span>
                    <h4 class="error-title">${operation} Error</h4>
                </div>
                
                <div class="error-message">
                    ${errorInfo.message}
                </div>
        `;
        
        // Add suggested actions
        if (errorInfo.suggestedActions.length > 0) {
            html += `
                <div class="error-actions">
                    <h5>💡 What you can do:</h5>
                    <ul>
            `;
            
            errorInfo.suggestedActions.forEach(action => {
                html += `<li>${action}</li>`;
            });
            
            html += `
                    </ul>
                </div>
            `;
        }
        
        // Add retry button if applicable
        if (retryCallback && errorInfo.isRetryable) {
            const retryId = `retry-${Date.now()}`;
            html += `
                <div class="error-retry">
            `;
            
            if (errorInfo.retryDelay) {
                html += `
                    <p class="retry-info">⏱️ You can retry in <span id="${retryId}-countdown">${errorInfo.retryDelay}</span> seconds</p>
                    <button id="${retryId}" class="btn btn-primary retry-btn" disabled>
                        🔄 Retry ${operation}
                    </button>
                `;
                
                // Start countdown
                setTimeout(() => this.startRetryCountdown(retryId, errorInfo.retryDelay, retryCallback), 100);
            } else {
                html += `
                    <button id="${retryId}" class="btn btn-primary retry-btn">
                        🔄 Retry ${operation}
                    </button>
                `;
                
                // Attach retry handler immediately
                setTimeout(() => {
                    const retryBtn = document.getElementById(retryId);
                    if (retryBtn) {
                        retryBtn.addEventListener('click', retryCallback);
                    }
                }, 100);
            }
            
            html += `
                </div>
            `;
        }
        
        html += `</div>`;
        
        return html;
    }

    /**
     * Show error in a modal dialog
     * @param {Object} errorInfo - Parsed error information
     * @param {string} operation - Operation name  
     * @param {Function} retryCallback - Retry function
     */
    showErrorModal(errorInfo, operation, retryCallback) {
        // Remove existing error modal
        const existingModal = document.getElementById('aria-error-modal');
        if (existingModal) {
            existingModal.remove();
        }
        
        const modal = document.createElement('div');
        modal.id = 'aria-error-modal';
        modal.style.cssText = `
            position: fixed; top: 0; left: 0; width: 100%; height: 100%;
            background: rgba(0,0,0,0.5); display: flex; align-items: center;
            justify-content: center; z-index: 1000;
        `;
        
        modal.innerHTML = `
            <div style="background: white; border-radius: 8px; max-width: 600px; max-height: 80vh; 
                        overflow-y: auto; padding: 0; margin: 20px; box-shadow: 0 4px 20px rgba(0,0,0,0.3);">
                <div style="padding: 20px; border-bottom: 1px solid #eee;">
                    <h3 style="margin: 0; color: #d32f2f;">Error Details</h3>
                </div>
                <div style="padding: 20px;">
                    ${this.createErrorDisplay(errorInfo, operation, retryCallback)}
                </div>
                <div style="padding: 20px; border-top: 1px solid #eee; text-align: right;">
                    <button onclick="this.closest('#aria-error-modal').remove()" 
                            class="btn btn-secondary">Close</button>
                </div>
            </div>
        `;
        
        document.body.appendChild(modal);
        
        // Close on background click
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                modal.remove();
            }
        });
    }

    /**
     * Start countdown for retry button
     * @param {string} retryId - Button ID
     * @param {number} delay - Delay in seconds
     * @param {Function} retryCallback - Retry function
     */
    startRetryCountdown(retryId, delay, retryCallback) {
        const button = document.getElementById(retryId);
        const countdown = document.getElementById(`${retryId}-countdown`);
        
        if (!button || !countdown) return;
        
        let remaining = delay;
        
        const updateCountdown = () => {
            countdown.textContent = remaining;
            
            if (remaining <= 0) {
                button.disabled = false;
                button.textContent = `🔄 Retry Now`;
                button.addEventListener('click', retryCallback);
                
                const retryInfo = button.parentNode.querySelector('.retry-info');
                if (retryInfo) {
                    retryInfo.style.display = 'none';
                }
            } else {
                remaining--;
                setTimeout(updateCountdown, 1000);
            }
        };
        
        updateCountdown();
    }

    /**
     * Get CSS class for error severity
     * @param {string} severity - Severity level
     * @returns {string} CSS class
     */
    getSeverityClass(severity) {
        switch (severity) {
            case 'low': return 'error-severity-low';
            case 'medium': return 'error-severity-medium';
            case 'high': return 'error-severity-high';
            case 'critical': return 'error-severity-critical';
            default: return 'error-severity-medium';
        }
    }

    /**
     * Get icon for error category
     * @param {string} category - Error category
     * @returns {string} Icon/emoji
     */
    getCategoryIcon(category) {
        switch (category) {
            case 'authentication': return '🔐';
            case 'authorization': return '🚫';
            case 'network': return '🌐';
            case 'timeout': return '⏱️';
            case 'service_unavailable': return '🔧';
            case 'rate_limit': return '⏳';
            case 'validation': return '📝';
            default: return '❌';
        }
    }

    /**
     * Handle operation with automatic error display
     * @param {Function} operation - Async operation to perform
     * @param {string} operationName - Name for error display
     * @param {HTMLElement} errorTarget - Where to show errors (optional)
     * @returns {Promise} Operation result
     */
    async withErrorHandling(operation, operationName, errorTarget = null) {
        try {
            return await operation();
        } catch (error) {
            let errorMessage = error.message || error.toString();
            
            // Handle fetch errors
            if (error instanceof Response) {
                try {
                    const errorData = await error.json();
                    errorMessage = errorData.error || errorData.message || `HTTP ${error.status} error`;
                } catch {
                    errorMessage = `HTTP ${error.status} error`;
                }
            }
            
            this.showError(errorMessage, operationName, errorTarget);
            throw error; // Re-throw for caller handling
        }
    }
}

// Global error handler instance
window.ariaErrorHandler = new ARIAErrorDisplay();

// Add CSS styles for error display
const errorStyles = document.createElement('style');
errorStyles.textContent = `
    .aria-error-display {
        border-radius: 8px;
        padding: 20px;
        margin: 15px 0;
        border-left: 4px solid;
        font-family: 'DM Sans', system-ui, sans-serif;
    }
    
    .error-severity-low {
        background: #fff3e0;
        border-left-color: #ff9800;
    }
    
    .error-severity-medium {
        background: #ffebee;
        border-left-color: #f44336;
    }
    
    .error-severity-high {
        background: #fce4ec;
        border-left-color: #e91e63;
    }
    
    .error-severity-critical {
        background: #f3e5f5;
        border-left-color: #9c27b0;
    }
    
    .error-header {
        display: flex;
        align-items: center;
        margin-bottom: 12px;
    }
    
    .error-icon {
        font-size: 20px;
        margin-right: 10px;
    }
    
    .error-title {
        margin: 0;
        color: #d32f2f;
        font-size: 16px;
        font-weight: 600;
    }
    
    .error-message {
        color: #424242;
        line-height: 1.5;
        margin-bottom: 15px;
    }
    
    .error-actions h5 {
        margin: 0 0 8px 0;
        color: #1976d2;
        font-size: 14px;
        font-weight: 600;
    }
    
    .error-actions ul {
        margin: 0;
        padding-left: 20px;
    }
    
    .error-actions li {
        margin: 4px 0;
        color: #424242;
        line-height: 1.4;
    }
    
    .error-retry {
        margin-top: 15px;
        padding-top: 15px;
        border-top: 1px solid #e0e0e0;
    }
    
    .retry-btn {
        margin-top: 8px;
    }
    
    .retry-btn:disabled {
        opacity: 0.6;
        cursor: not-allowed;
    }
    
    .retry-info {
        margin: 0 0 8px 0;
        color: #666;
        font-size: 14px;
    }
`;

document.head.appendChild(errorStyles);
