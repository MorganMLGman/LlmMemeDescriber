// ======================== CSRF Token Management ========================

async function initializeCSRFToken() {
    // Initialize CSRF token on page load for authenticated requests.
    try {
        const response = await fetch('/api/csrf-token', { credentials: 'include' });
        if (response.ok) {
            const data = await response.json();
            csrfToken = data.csrf_token;
            console.log('CSRF token initialized successfully');
        } else {
            console.warn('Failed to initialize CSRF token (may not be authenticated)');
        }
    } catch (error) {
        console.warn('CSRF token initialization skipped:', error);
    }
}

function getSecurityHeaders() {
    // Return security headers including CSRF token if available.
    const headers = {
        'Content-Type': 'application/json'
    };
    if (csrfToken) {
        headers['X-CSRF-Token'] = csrfToken;
    }
    return headers;
}

// ======================== Meme Data Fetching ========================

async function fetchMoreFromAPI() {
    if (isLoading) return;
    
    isLoading = true;
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    
    try {
        console.log(`Fetching from API: offset=${apiOffset}, limit=2000`);
        
        const response = await fetch(`/memes?limit=2000&offset=${apiOffset}`, {
            signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (response.status === 429) {
            showRateLimitWarning();
            hasMoreMemes = false;
            throw new Error('Rate limit exceeded');
        }
        
        if (!response.ok) {
            throw new Error(`API error: ${response.status} ${response.statusText}`);
        }
        
        const data = await response.json();
        const newMemes = Array.isArray(data) ? data : (data.memes || []);
        
        console.log(`API returned ${newMemes.length} memes from offset ${apiOffset}`);
        
        if (newMemes.length > 0) {
            allMemes = allMemes.concat(newMemes);
            filteredMemes = allMemes;
            totalFetched = allMemes.length;
            apiOffset += newMemes.length;
            
            hasMoreMemes = newMemes.length === 2000;
        } else {
            hasMoreMemes = false;
        }
        
        console.log(`Total memes in memory: ${allMemes.length}, hasMore: ${hasMoreMemes}`);
    } catch (error) {
        console.error('Error fetching from API:', error);
        hasMoreMemes = false;
    } finally {
        isLoading = false;
    }
}

// ======================== Individual Meme Operations ========================

async function saveMeme() {
    if (!currentMemeId) return;
    
    const category = document.getElementById('memeCategory').value;
    const keywords = (window.currentKeywords || []).join(', ');
    const description = document.getElementById('memeDescription').value;
    
    try {
        const response = await fetch(`/memes/${encodeURIComponent(currentMemeId)}`, {
            method: 'PATCH',
            headers: getSecurityHeaders(),
            body: JSON.stringify({category, keywords, description})
        });
        
        if (!response.ok) throw new Error('Failed to save');
        
        showAlert('Meme updated!', 'success');
        bootstrap.Modal.getInstance(document.getElementById('memeModal')).hide();
        loadMemes();
    } catch (error) {
        console.error('Error saving meme:', error);
        showAlert('Failed to save meme', 'error');
    }
}

async function markRemoved() {
    if (!currentMemeId) return;
    try {
        const response = await fetch(`/memes/${encodeURIComponent(currentMemeId)}`, {
            method: 'DELETE'
        });
        if (response.ok) {
            showSuccess('Meme marked as removed');
            bootstrap.Modal.getInstance(document.getElementById('memeModal')).hide();
            loadMemes();
        } else {
            showError('Failed to mark meme as removed');
        }
    } catch (error) {
        console.error('Error removing meme:', error);
        showError('Failed to remove meme');
    }
}

// ======================== Phash Operations ========================

async function getPhashStatus() {
    try {
        const response = await fetch('/memes/phash-status');
        const data = await response.json();
        return data;
    } catch (error) {
        console.error('Error getting phash status:', error);
        return null;
    }
}

async function recalculatePhash(filename) {
    if (!confirm('Recalculate perceptual hash for this meme? This may take a moment.')) {
        return;
    }
    
    try {
        const response = await fetch(`/memes/${encodeURIComponent(filename)}/recalculate-phash`, {
            method: 'POST'
        });
        
        const data = await response.json();
        
        if (data.status === 'ok') {
            showSuccess(`Phash calculated: ${data.phash}`);
            loadMemes();
        } else {
            showError(`Failed: ${data.message} (data size: ${data.data_size})`);
        }
    } catch (error) {
        console.error('Error recalculating phash:', error);
        showError('Failed to recalculate phash');
    }
}

// ======================== Reprocessing Operations ========================

async function retryDescriptionGeneration() {
    if (!currentMemeId) return;
    
    if (!confirm('Force retry description generation for this meme?')) return;
    
    try {
        // Show loading spinner, hide preview
        const previewDiv = document.getElementById('memePreview');
        const spinnerDiv = document.getElementById('generationLoadingSpinner');
        const imageEl = document.getElementById('memeImage');
        const videoEl = document.getElementById('memeVideo');
        
        previewDiv.style.display = 'none';
        spinnerDiv.style.display = 'block';
        imageEl.style.display = 'none';
        videoEl.style.display = 'none';
        
        const response = await fetch(`/memes/${encodeURIComponent(currentMemeId)}/force-description`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'}
        });
        
        // Hide spinner
        spinnerDiv.style.display = 'none';
        
        if (response.status === 429) {
            previewDiv.style.display = 'block';
            showError('Rate limit reached. Processing will retry automatically on the next sync cycle.');
            return;
        }
        
        if (!response.ok) {
            previewDiv.style.display = 'block';
            const errorData = await response.json();
            showError(`Error (${response.status}): ${errorData.detail || 'Failed to generate description'}`);
            return;
        }
        
        const updatedMeme = await response.json();
        
        // If description was generated successfully, reload page
        if (updatedMeme.description && updatedMeme.status === 'filled') {
            showSuccess('Description generated successfully! Reloading...');
            await new Promise(resolve => setTimeout(resolve, 1000));
            location.reload();
        } else {
            previewDiv.style.display = 'block';
            showError('Description generation did not produce results. Please check the sync logs.');
        }
    } catch (error) {
        console.error('Error retrying description generation:', error);
        // Show preview again on error
        document.getElementById('generationLoadingSpinner').style.display = 'none';
        document.getElementById('memePreview').style.display = 'block';
        showError(`Error: ${error.message}`);
    }
}

async function reprocessMeme() {
    if (!currentMemeId) return;

    try {
        // Get CSRF token
        const csrfResponse = await fetch('/api/csrf-token');
        const csrfData = await csrfResponse.json();
        const csrfToken = csrfData.csrf_token;

        // Show loading spinner, hide preview
        const previewDiv = document.getElementById('memePreview');
        const spinnerDiv = document.getElementById('generationLoadingSpinner');
        const imageEl = document.getElementById('memeImage');
        const videoEl = document.getElementById('memeVideo');
        const reprocessBtn = document.getElementById('reprocessBtn');

        previewDiv.style.display = 'none';
        spinnerDiv.style.display = 'block';
        imageEl.style.display = 'none';
        videoEl.style.display = 'none';
        reprocessBtn.disabled = true;

        const response = await fetch(`/memes/${encodeURIComponent(currentMemeId)}/reprocess`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'X-CSRF-Token': csrfToken
            }
        });

        // Hide spinner
        spinnerDiv.style.display = 'none';
        reprocessBtn.disabled = false;

        if (response.status === 429) {
            previewDiv.style.display = 'block';
            showError('Rate limit reached. Processing will retry automatically on the next sync cycle.');
            return;
        }

        if (!response.ok) {
            previewDiv.style.display = 'block';
            const errorData = await response.json();
            showError(`Error (${response.status}): ${errorData.detail || 'Failed to reprocess meme'}`);
            return;
        }

        const updatedMeme = await response.json();

        // If reprocessing was successful, reload the meme data in the modal
        if (updatedMeme.status === 'filled') {
            showSuccess('Meme reprocessed successfully! Updating modal...');
            await new Promise(resolve => setTimeout(resolve, 500));
            // Refresh the modal with the updated data
            await viewMeme(currentMemeId);
        } else {
            previewDiv.style.display = 'block';
            showError('Reprocessing did not produce complete results. Please check the sync logs.');
        }
    } catch (error) {
        console.error('Error reprocessing meme:', error);
        // Show preview again on error
        document.getElementById('generationLoadingSpinner').style.display = 'none';
        document.getElementById('memePreview').style.display = 'block';
        document.getElementById('reprocessBtn').disabled = false;
        showError(`Error: ${error.message}`);
    }
}
