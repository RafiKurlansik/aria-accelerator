/**
 * ARIA Analytics Tracking
 * Client-side analytics tracking for user interactions
 */

class AnalyticsTracker {
    constructor() {
        this.sessionId = this.getOrCreateSessionId();
        this.chatSessionId = null;
        this.apiBase = window.location.origin;
        
        // Initialize chat session ID if we're on the chat page
        if (window.location.pathname === '/chat') {
            this.chatSessionId = this.generateChatSessionId();
        }
    }
    
    /**
     * Get or create session ID (reuse existing session management)
     */
    getOrCreateSessionId() {
        // Use existing session management from app.js
        if (typeof getOrCreateSessionId === 'function') {
            return getOrCreateSessionId();
        }
        
        // Fallback implementation
        let sessionId = localStorage.getItem('aria-session-id');
        if (!sessionId) {
            sessionId = 'session-' + Math.random().toString(36).substr(2, 9);
            localStorage.setItem('aria-session-id', sessionId);
        }
        return sessionId;
    }
    
    /**
     * Generate unique chat session ID
     */
    generateChatSessionId() {
        return 'chat-' + Math.random().toString(36).substr(2, 9) + '-' + Date.now();
    }
    
    /**
     * Generic event tracking method
     */
    async trackEvent(eventData) {
        try {
            const response = await fetch(`${this.apiBase}/api/analytics/track`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Session-Id': this.sessionId
                },
                body: JSON.stringify(eventData)
            });
            
            if (response.ok) {
                const result = await response.json();
                console.debug('Analytics event tracked:', eventData.event_type, result);
                return result.success;
            } else {
                console.warn('Analytics tracking failed:', response.status, response.statusText);
                return false;
            }
        } catch (error) {
            console.warn('Analytics tracking error:', error);
            // Don't block user experience for analytics failures
            return false;
        }
    }
    
    /**
     * Track RFI export event
     */
    async trackExport(format, documentName, questionCount, answerCount) {
        return this.trackEvent({
            event_type: "rfi_export",
            session_id: this.sessionId,
            document_name: documentName,
            export_format: format,
            total_questions: questionCount,
            total_answers: answerCount
        });
    }
    
    /**
     * Track chat copy event
     */
    async trackChatCopy(questionText, responseText, modelUsed = 'unknown') {
        if (!this.chatSessionId) {
            this.chatSessionId = this.generateChatSessionId();
        }
        
        return this.trackEvent({
            event_type: "chat_copy",
            session_id: this.sessionId,
            chat_session_id: this.chatSessionId,
            question_text: questionText,
            response_text: responseText,
            model_used: modelUsed,
            copied_to_clipboard: true
        });
    }
    
    /**
     * Track chat session completion (when user leaves chat or starts new conversation)
     */
    async trackChatSession(totalQuestions, totalResponses, sessionDuration, status = 'completed') {
        if (!this.chatSessionId) {
            return false;
        }
        
        return this.trackEvent({
            event_type: "chat_session",
            session_id: this.sessionId,
            chat_session_id: this.chatSessionId,
            total_questions: totalQuestions,
            total_responses: totalResponses,
            session_duration_seconds: sessionDuration,
            final_status: status
        });
    }
    
    /**
     * Get document name from page (helper method)
     */
    getDocumentName() {
        // Try multiple selectors to find document name
        const selectors = ['#docName', '.document-name', '[data-document-name]'];
        
        for (const selector of selectors) {
            const element = document.querySelector(selector);
            if (element && element.textContent.trim()) {
                return element.textContent.trim();
            }
        }
        
        // Fallback to URL or generic name
        const pathParts = window.location.pathname.split('/');
        return pathParts[pathParts.length - 1] || 'Unknown Document';
    }
    
    /**
     * Get current model being used (helper method)
     */
    getCurrentModel() {
        // Try to find model information from page
        const modelSelectors = ['[data-model]', '#currentModel', '.model-name'];
        
        for (const selector of modelSelectors) {
            const element = document.querySelector(selector);
            if (element) {
                return element.textContent.trim() || element.dataset.model;
            }
        }
        
        return 'unknown';
    }
}

// Global analytics instance - available to all pages
window.analytics = new AnalyticsTracker();

// Auto-track page navigation for analytics context
window.addEventListener('beforeunload', () => {
    // Track chat session completion if leaving chat page
    if (window.location.pathname === '/chat' && window.analytics.chatSessionId) {
        // Get basic session stats if available
        const chatMessages = document.querySelectorAll('.chat-message, .message').length;
        const sessionStart = window.analytics.sessionStartTime || Date.now();
        const sessionDuration = (Date.now() - sessionStart) / 1000;
        
        // Fire and forget - don't block navigation
        window.analytics.trackChatSession(
            Math.floor(chatMessages / 2), // Approximate questions
            Math.floor(chatMessages / 2), // Approximate responses  
            sessionDuration,
            'abandoned'
        );
    }
});

// Track session start time for duration calculations
window.analytics.sessionStartTime = Date.now();

// Export analytics functions for global use
window.trackExport = (format, documentName, questionCount, answerCount) => {
    return window.analytics.trackExport(format, documentName, questionCount, answerCount);
};

window.trackChatCopy = (questionText, responseText, modelUsed) => {
    return window.analytics.trackChatCopy(questionText, responseText, modelUsed);
};

// Debug helper for development
window.analyticsDebug = () => {
    console.log('Analytics Tracker State:', {
        sessionId: window.analytics.sessionId,
        chatSessionId: window.analytics.chatSessionId,
        currentPage: window.location.pathname,
        documentName: window.analytics.getDocumentName(),
        currentModel: window.analytics.getCurrentModel()
    });
};
