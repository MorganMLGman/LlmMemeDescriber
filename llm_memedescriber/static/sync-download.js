// ======================== Sync Operations ========================

function setSyncButtonState(enabled) {
    const refreshBtn = document.querySelector('button[onclick="startSyncJob()"]');
    refreshBtn.disabled = !enabled;
    refreshBtn.textContent = enabled ? 'Refresh' : '⏳ Syncing...';
}

async function pollSyncStatus() {
    /**Poll the /sync/status endpoint to get current operation status and update UI.*/
    try {
        const response = await fetch('/sync/status', {
            method: 'GET',
            headers: { 'Content-Type': 'application/json' }
        });

        if (!response.ok) {
            console.warn('Failed to get sync status:', response.status);
            return;
        }

        const status = await response.json();
        updateSyncButtonStatus(status);

        // Stop polling if completed or no operation
        if (!status.operation || status.operation === 'completed') {
            if (syncStatusPolling) {
                clearInterval(syncStatusPolling);
                syncStatusPolling = null;
            }
        }
    } catch (error) {
        console.error('Error polling sync status:', error);
    }
}

function updateSyncButtonStatus(status) {
    /**Update sync button text based on current operation status.*/
    const refreshBtn = document.querySelector('button[onclick="startSyncJob()"]');
    if (!refreshBtn) return;

    const operation = status.operation;
    const progress = status.progress || {};

    if (operation === 'syncing') {
        refreshBtn.textContent = '⏳ Syncing...';
    } else if (operation === 'transcoding') {
        const transcoded = progress.transcoded || 0;
        const total = progress.total || 0;
        if (total > 0) {
            refreshBtn.textContent = `⏳ Transcoding MKVs (${transcoded}/${total})`;
        } else {
            refreshBtn.textContent = '⏳ Scanning for MKVs...';
        }
    } else if (operation === 'completed') {
        refreshBtn.textContent = '✓ Complete';
        // Will be reset to "Refresh" in finally block
    }
}

async function startSyncJob() {
    try {
        console.log('Starting sync job...');
        const refreshBtn = document.querySelector('button[onclick="startSyncJob()"]');
        refreshBtn.disabled = true;
        refreshBtn.textContent = '⏳ Starting...';

        syncStartTime = Date.now();

        const response = await fetch('/sync', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            }
        });

        if (response.status === 429) {
            showRateLimitWarning();
            showError('Rate limit reached. Processing will retry automatically on the next sync cycle.');
            return;
        }

        if (!response.ok) {
            throw new Error(`Sync failed: ${response.status} ${response.statusText}`);
        }

        // Start polling for status updates (every 2 seconds)
        syncStatusPolling = setInterval(pollSyncStatus, 2000);

        const data = await response.json();
        console.log('Sync completed:', data);

        // Stop polling
        if (syncStatusPolling) {
            clearInterval(syncStatusPolling);
            syncStatusPolling = null;
        }

        // Build success message from new response format
        let successMessage = data.message || 'Sync completed successfully';

        // Extract nested result if using new format
        const result = data.result || data;

        // Check if we got rate limited during sync
        if (result.rate_limited) {
            showRateLimitWarning();
            successMessage += `. Rate limit reached - will retry automatically on next cycle.`;
        }

        // Add MKV transcoding details if present
        if (data.result && data.result.mkv_transcoding) {
            const mkv = data.result.mkv_transcoding;
            if (mkv.total_found > 0) {
                successMessage += `\n\nMKV Transcoding:\nFound: ${mkv.total_found}, Transcoded: ${mkv.transcoded}`;
                if (mkv.failed > 0) {
                    successMessage += `, Failed: ${mkv.failed}`;
                }
            }
        }

        showAlert(successMessage, 'success');

        // Reload memes after sync
        await loadMemes();
    } catch (error) {
        console.error('Error during sync:', error);
        showAlert(`Sync failed: ${error.message}`, 'error');

        // Stop polling on error
        if (syncStatusPolling) {
            clearInterval(syncStatusPolling);
            syncStatusPolling = null;
        }
    } finally {
        const refreshBtn = document.querySelector('button[onclick="startSyncJob()"]');
        refreshBtn.disabled = false;
        refreshBtn.textContent = 'Refresh';
    }
}

// ======================== Video Download Functions ========================

let currentDownloadJobId = null;
let downloadPollingInterval = null;

function showDownloadModal() {
    // Show the download video modal
    const modal = new bootstrap.Modal(document.getElementById('downloadVideoModal'));

    // Reset form
    document.getElementById('videoUrl').value = '';
    document.getElementById('downloadProgress').style.display = 'none';
    document.getElementById('downloadError').style.display = 'none';
    document.getElementById('downloadSuccess').style.display = 'none';
    document.getElementById('submitDownloadBtn').disabled = false;

    modal.show();
}

async function submitDownload() {
    const url = document.getElementById('videoUrl').value.trim();

    if (!url) {
        showDownloadError('Please enter a video URL');
        return;
    }

    // Validate URL format
    try {
        new URL(url);
    } catch (e) {
        showDownloadError('Please enter a valid URL');
        return;
    }

    // Hide error/success messages
    document.getElementById('downloadError').style.display = 'none';
    document.getElementById('downloadSuccess').style.display = 'none';

    // Disable submit button
    document.getElementById('submitDownloadBtn').disabled = true;

    try {
        const response = await fetch('/api/download-video', {
            method: 'POST',
            headers: getSecurityHeaders(),
            credentials: 'include',
            body: JSON.stringify({ url: url })
        });

        if (!response.ok) {
            const error = await response.json();
            showDownloadError(error.detail || 'Failed to submit download request');
            document.getElementById('submitDownloadBtn').disabled = false;
            return;
        }

        const job = await response.json();
        currentDownloadJobId = job.id;

        console.log('Download job created:', job.id);

        // Show progress UI
        document.getElementById('downloadProgress').style.display = 'block';
        updateDownloadProgress(0, 'Queued for download...');

        // Start polling for progress
        startDownloadPolling(job.id);

    } catch (error) {
        console.error('Download submission error:', error);
        showDownloadError(error.message || 'Network error occurred');
        document.getElementById('submitDownloadBtn').disabled = false;
    }
}

async function startDownloadPolling(jobId) {
    console.log('Starting download polling for job', jobId);

    // Clear any existing interval
    stopDownloadPolling();

    // Poll every 2 seconds
    downloadPollingInterval = setInterval(async () => {
        try {
            const response = await fetch(`/api/download-jobs/${jobId}`, {
                credentials: 'include'
            });

            if (!response.ok) {
                console.error('Failed to fetch download job status:', response.status);
                if (response.status === 404) {
                    stopDownloadPolling();
                    showDownloadError('Download job not found');
                }
                return;
            }

            const job = await response.json();
            console.log('Download job status:', job.status, job.progress_percent + '%');

            // Update progress bar
            const progressPercent = Math.round(job.progress_percent);
            updateDownloadProgress(progressPercent, getStatusMessage(job));

            // Check if completed or failed
            if (job.status === 'completed') {
                stopDownloadPolling();
                showDownloadSuccess(job.filename || 'Video downloaded successfully!');

                // Refresh meme list after a short delay
                setTimeout(async () => {
                    await loadMemes();
                    closeDownloadModal();
                }, 2000);

            } else if (job.status === 'failed') {
                stopDownloadPolling();
                showDownloadError(job.error_message || 'Download failed');
                document.getElementById('submitDownloadBtn').disabled = false;
            }

        } catch (error) {
            console.error('Error polling download status:', error);
            // Don't stop polling on temporary errors
        }
    }, 2000);
}

function stopDownloadPolling() {
    if (downloadPollingInterval) {
        clearInterval(downloadPollingInterval);
        downloadPollingInterval = null;
    }
}

function getStatusMessage(job) {
    switch (job.status) {
        case 'pending':
            return 'Waiting in queue...';
        case 'downloading':
            if (job.video_title) {
                return `Downloading: ${job.video_title}`;
            }
            return 'Downloading video...';
        case 'processing':
            return 'Processing video...';
        case 'completed':
            return 'Download complete!';
        case 'failed':
            return 'Download failed';
        default:
            return 'Processing...';
    }
}

function updateDownloadProgress(percent, message) {
    const progressBar = document.getElementById('downloadProgressBar');
    const statusText = document.getElementById('downloadStatus');

    progressBar.style.width = `${percent}%`;
    progressBar.textContent = `${percent}%`;

    if (statusText && message) {
        statusText.textContent = message;
    }
}

function showDownloadError(message) {
    const errorDiv = document.getElementById('downloadError');
    errorDiv.textContent = message;
    errorDiv.style.display = 'block';

    // Hide success message
    document.getElementById('downloadSuccess').style.display = 'none';
}

function showDownloadSuccess(message) {
    const successDiv = document.getElementById('downloadSuccess');
    successDiv.textContent = message;
    successDiv.style.display = 'block';

    // Hide error message
    document.getElementById('downloadError').style.display = 'none';

    // Hide progress
    document.getElementById('downloadProgress').style.display = 'none';
}

function closeDownloadModal() {
    const modal = bootstrap.Modal.getInstance(document.getElementById('downloadVideoModal'));
    if (modal) {
        modal.hide();
    }
    stopDownloadPolling();
}

// Check if download feature is enabled and show/hide button
async function checkDownloadFeatureEnabled() {
    try {
        const response = await fetch('/api/stats', { credentials: 'include' });
        if (response.ok) {
            const stats = await response.json();

            // Check if download feature flag exists in stats
            // If the endpoint exists and doesn't return 404, feature is likely enabled
            // We'll try to fetch download jobs to verify
            try {
                const testResponse = await fetch('/api/download-jobs?limit=1', { credentials: 'include' });
                if (testResponse.ok || testResponse.status === 401) {
                    // Feature is enabled (200 or 401 means endpoint exists)
                    showDownloadButton();
                }
            } catch (e) {
                // Feature disabled or not available
                console.debug('Download feature not enabled');
            }
        }
    } catch (error) {
        console.debug('Could not check download feature status:', error);
    }
}

function showDownloadButton() {
    const downloadBtn = document.getElementById('downloadVideoBtn');
    const downloadBtnMobile = document.getElementById('downloadVideoBtnMobile');

    if (downloadBtn) {
        downloadBtn.style.display = '';
    }
    if (downloadBtnMobile) {
        downloadBtnMobile.style.display = '';
    }
}

// Check download feature on page load
document.addEventListener('DOMContentLoaded', function() {
    checkDownloadFeatureEnabled();
});
