// ======================== State Management ========================

const API_URL = '';
let currentMemeId = null;
let allMemes = [];
let filteredMemes = [];
let displayedMemes = [];
const itemsPerPage = 100;
let currentOffset = 0;
let isLoading = false;
let hasMoreMemes = true;
let totalMemeCount = 0;
let searchQuery = '';
let apiOffset = 0;
let totalFetched = 0;
let csrfToken = null;
let lastRateLimitWarningTime = 0;
let maxGenerationAttempts = null;
let syncStatusPolling = null;  // Interval ID for sync status polling
let syncStartTime = null;      // Timestamp when sync started

// ======================== Utility Functions ========================

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatBytes(bytes) {
    if (!bytes && bytes !== 0) return '';
    const mb = bytes / 1024 / 1024;
    return mb.toFixed(2) + ' MiB';
}

function truncateFilename(name, maxLen) {
    if (!name) return '';
    if (name.length <= maxLen) return name;
    return name.substring(0, maxLen - 3) + '...';
}

function cssEscape(s) {
    return s.replace(/"/g, '\\"').replace(/'/g, "\\'");
}

// ======================== UI Helper Functions ========================

function showAlert(message, type = 'success', duration = 5000) {
    const alert = document.createElement('div');
    const className = type === 'error' ? 'alert-danger' : 'alert-success';
    const timeoutDuration = type === 'error' ? duration : 3000;
    
    alert.className = `alert ${className} alert-dismissible fade show position-fixed top-0 start-50 translate-middle-x mt-3`;
    alert.style.zIndex = '9999';
    alert.innerHTML = `
        ${escapeHtml(message)}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    document.body.appendChild(alert);
    setTimeout(() => alert.remove(), timeoutDuration);
}

function showError(message) {
    showAlert(message, 'error', 5000);
}

function showSuccess(message) {
    showAlert(message, 'success', 3000);
}

function showLoadingIndicator(show = true) {
    const indicator = document.getElementById('loadingIndicator');
    if (indicator) {
        indicator.style.display = show ? 'block' : 'none';
    }
}

function showEndOfList(show = true) {
    const message = document.getElementById('endOfListMessage');
    if (message) {
        message.style.display = show ? 'block' : 'none';
    }
}

function showRateLimitWarning() {
    const now = Date.now();
    const cooldownMs = 60000;  // 1 minute cooldown between warnings
    
    // Only show if enough time has passed since last warning
    if (now - lastRateLimitWarningTime >= cooldownMs) {
        lastRateLimitWarningTime = now;
        
        const alert = document.createElement('div');
        alert.className = 'alert alert-warning alert-dismissible fade show position-fixed top-0 start-50 translate-middle-x mt-3';
        alert.style.zIndex = '9999';
        alert.innerHTML = `
            <strong>⚠️ Rate Limit Reached:</strong> The AI service has paused requests. Processing will resume automatically.
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;
        document.body.appendChild(alert);
        setTimeout(() => alert.remove(), 6000);  // Auto-dismiss after 6 seconds
    }
}

// ======================== Stats Update ========================

function updateStats(stats) {
    const total = stats.total_memes || 0;
    const processed = stats.processed_memes || 0;
    const pending = stats.unprocessed_memes || 0;
    
    const statsText = `Total: ${total} | Processed: ${processed} | Pending: ${pending}`;
    const statsEl = document.getElementById('statsText');
    if (statsEl) {
        statsEl.textContent = statsText;
    } else {
        console.debug('updateStats: #statsText not found in DOM, skipping');
    }
    console.log('Stats updated:', statsText);
}

function resetMemeState() {
    allMemes = [];
    filteredMemes = [];
    displayedMemes = [];
    apiOffset = 0;
    totalFetched = 0;
    currentOffset = 0;
    hasMoreMemes = true;
    searchQuery = '';
}

// ======================== Scroll-to-Top Button ========================

function scrollToTop() {
    window.scrollTo({ top: 0, behavior: 'smooth' });
}

window.addEventListener('scroll', () => {
    const btn = document.getElementById('scrollToTopBtn');
    if (btn) {
        if (window.scrollY > 300) {
            btn.classList.add('show');
        } else {
            btn.classList.remove('show');
        }
    }
});

// ======================== Modal Setup ========================

// Set up video cleanup listeners when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    console.log('Core: DOM Content Loaded');
    
    // Set up video autoplay/stop listeners for the modal (with proper cleanup)
    const memeModalEl = document.getElementById('memeModal');
    if (memeModalEl) {
        const onHideHandler = function() {
            try {
                const video = document.getElementById('memeVideo');
                const videoSource = document.getElementById('memeVideoSource');
                if (video) {
                    video.pause();
                    video.currentTime = 0;
                    if (videoSource) {
                        videoSource.src = "";
                        video.load(); // Essential to unload the previous video
                    }
                }
            } catch (e) {
                console.error('Error in hide handler:', e);
            }
        };
        
        // Use a custom property to track if listeners were already added
        if (!memeModalEl._listenersAdded) {
            memeModalEl.addEventListener('hide.bs.modal', onHideHandler);
            memeModalEl._listenersAdded = true;
        }
    }
    
    // Set up listeners for simple preview modal
    const simpleModalEl = document.getElementById('memeSimplePreviewModal');
    if (simpleModalEl) {
        const onHideSimpleHandler = function() {
            try {
                const video = document.getElementById('simpleMemeVideo');
                const videoSource = document.getElementById('simpleMemeVideoSource');
                if (video) {
                    video.pause();
                    video.currentTime = 0;
                    if (videoSource) {
                        videoSource.src = "";
                        video.load();
                    }
                }
            } catch (e) {
                console.error('Error in simple hide handler:', e);
            }
        };
        if (!simpleModalEl._listenersAdded) {
            simpleModalEl.addEventListener('hide.bs.modal', onHideSimpleHandler);
            simpleModalEl._listenersAdded = true;
        }
    }
}, { once: true });

// Listen for modal show event to load meme data
document.addEventListener('DOMContentLoaded', function() {
    const memeModal = document.getElementById('memeModal');
    if (memeModal) {
        memeModal.addEventListener('show.bs.modal', function(event) {
            // Get the button/element that triggered the modal
            const button = event.relatedTarget;
            if (button) {
                const filename = button.getAttribute('data-meme-filename');
                if (filename) {
                    viewMeme(filename);
                }
            }
        });
    }
});
