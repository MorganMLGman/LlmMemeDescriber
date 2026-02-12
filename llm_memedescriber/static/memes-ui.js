// ======================== Meme Loading and Initialization ========================

async function loadMemes() {
    try {
        console.log('=== Starting loadMemes ===');
        
        // Initialize CSRF token first
        await initializeCSRFToken();
        
        console.log('Testing API health...');
        const healthResponse = await fetch(`/health`, { timeout: 2000 });
        console.log('Health check response:', healthResponse.status);
        
        if (!healthResponse.ok) {
            throw new Error(`API not responding: ${healthResponse.status}`);
        }
        
        console.log('API is responsive, initializing memes list...');
        
        try {
            const statsResponse = await fetch('/api/stats', { credentials: 'include' });
            if (statsResponse.ok) {
                const statsData = await statsResponse.json();
                maxGenerationAttempts = statsData.max_generation_attempts || 3;
                console.log('Loaded max_generation_attempts from backend:', maxGenerationAttempts);
            } else {
                maxGenerationAttempts = 3;  // Fallback
            }
        } catch (e) {
            console.debug('Failed to load max_generation_attempts from backend, using default:', e);
            maxGenerationAttempts = 3;  // Fallback
        }
        
        allMemes = [];
        filteredMemes = [];
        displayedMemes = [];
        apiOffset = 0;
        totalFetched = 0;
        currentOffset = 0;
        hasMoreMemes = true;
        searchQuery = '';
        
        await fetchMoreFromAPI();
        
        const total = allMemes.length;
        const processed = allMemes.filter(m => m.processed === true).length;
        const pending = total - processed;
        updateStats({total_memes: total, processed_memes: processed, unprocessed_memes: pending});
        
        console.log('Calling renderInitial...');
        renderInitial();
        setupInfiniteScroll();
        console.log('=== Memes loaded successfully ===');
    } catch (error) {
        console.error('Error loading memes:', error);
        showAlert(`Failed to load memes: ${error.message}`, 'error');
    }
}

// ======================== Rendering Functions ========================

function renderInitial() {
    const container = document.getElementById('memesContainer');
    if (!container) {
        console.debug('renderInitial: #memesContainer not found, skipping render');
        return;
    }
    
    displayedMemes = [];
    currentOffset = 0;
    container.innerHTML = '';
    loadMoreMemes();
}

function loadMoreMemes() {
    if (isLoading) {
        console.log('loadMoreMemes skipped - already loading');
        return;
    }
    
    console.log(`loadMoreMemes: currentOffset=${currentOffset}, displayedMemes=${displayedMemes.length}, allMemes=${allMemes.length}, itemsPerPage=${itemsPerPage}`);
    
    if (currentOffset + itemsPerPage > allMemes.length && hasMoreMemes) {
        console.log('Need more data from API - fetching...');
        const loadingIndicator = document.getElementById('loadingIndicator');
        if (loadingIndicator) {
            loadingIndicator.style.display = 'block';
        }
        fetchMoreFromAPI().then(() => {
            loadMoreMemesFromCached();
        });
    } else {
        loadMoreMemesFromCached();
    }
}

function loadMoreMemesFromCached() {
    try {
        const nextBatch = filteredMemes.slice(currentOffset, currentOffset + itemsPerPage);
        
        if (nextBatch.length === 0) {
            showEndOfList(true);
            showLoadingIndicator(false);
            console.log('No more memes to display');
            return;
        }
        
        displayedMemes = displayedMemes.concat(nextBatch);
        currentOffset += itemsPerPage;
        
        if (currentOffset >= filteredMemes.length && !hasMoreMemes) {
            showEndOfList(true);
        }
        
        renderDisplayedMemes();
        
    } catch (error) {
        console.error('Error loading more memes:', error);
        showError('Error loading more memes');
    } finally {
        showLoadingIndicator(false);
    }
}

function renderDisplayedMemes() {
    const container = document.getElementById('memesContainer');
    if (!container) {
        console.debug('renderDisplayedMemes: #memesContainer not found, skipping render');
        return;
    }
    
    if (displayedMemes.length === 0) {
        container.innerHTML = `
            <div class="col-12 text-center py-5">
                <p class="text-muted">No memes found</p>
            </div>
        `;
        return;
    }

    try {
        container.innerHTML = displayedMemes.map((meme, index) => `
            <div class="col-md-6 col-lg-4">
                <div class="card meme-card h-100 position-relative">
                    ${(meme.duplicate_group_id && meme.is_false_positive !== true) ?
                        `<div class="position-absolute top-0 end-0 m-2">
                            <button class="btn btn-sm btn-warning" onclick="openDeduplicationPanel('${escapeHtml(meme.filename)}')"
                                    style="padding: 2px 6px; font-size: 12px;">⚠️ Similar</button>
                        </div>` : ''}
                    <img src="/memes/${encodeURIComponent(meme.filename)}/preview"
                         class="card-img-top cursor-pointer"
                         style="height: 300px; object-fit: contain; background: #f8f9fa; cursor: pointer;"
                         alt="${meme.filename}"
                         loading="${index < 12 ? 'eager' : 'lazy'}"
                         decoding="async"
                         data-bs-toggle="modal" data-bs-target="#memeModal" data-meme-filename="${meme.filename}"
                         onerror="this.src='/static/placeholder.png'">
                    <div class="card-body d-flex flex-column cursor-pointer" data-bs-toggle="modal" data-bs-target="#memeModal" data-meme-filename="${meme.filename}">
                        <h6 class="card-title text-truncate">${escapeHtml(meme.filename)}</h6>
                        <p class="card-text mb-2 small flex-grow-1">
                            ${escapeHtml((meme.description || '').substring(0, 100))}...
                        </p>
                        <small class="text-secondary">
                            ${meme.processed === true ? '<span class="badge bg-success">Processed</span>' : '<span class="badge bg-warning">Pending</span>'}
                        </small>
                    </div>
                </div>
            </div>
        `).join('');
    } catch (error) {
        console.error('Error rendering memes:', error);
        container.innerHTML = `<div class="col-12"><div class="alert alert-danger">Error rendering memes: ${error.message}</div></div>`;
    }
}

function setupInfiniteScroll() {
    const observer = new IntersectionObserver((entries) => {
        entries.forEach(entry => {
            if (entry.isIntersecting && !isLoading) {
                console.log('Sentinel element visible - loading more memes');
                const loadingIndicator = document.getElementById('loadingIndicator');
                if (loadingIndicator) {
                    loadingIndicator.style.display = 'block';
                }
                loadMoreMemes();
            }
        });
    }, { threshold: 0.1 });
    
    let sentinel = document.getElementById('scrollSentinel');
    if (!sentinel) {
        sentinel = document.createElement('div');
        sentinel.id = 'scrollSentinel';
        sentinel.style.height = '100px';
        document.getElementById('memesContainer').parentElement.appendChild(sentinel);
    }
    observer.observe(sentinel);
}

// ======================== Meme Modal Functions ========================

async function viewMeme(memeFilename) {
    currentMemeId = memeFilename;
    
    // Show loading state
    const memePreview = document.getElementById('memePreview');
    const generationSpinner = document.getElementById('generationLoadingSpinner');
    
    if (memePreview) memePreview.style.display = 'none';
    if (generationSpinner) generationSpinner.style.display = 'block';
    
    try {
        // Add timeout to fetch (10 seconds)
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        
        const response = await fetch(`/memes/${encodeURIComponent(memeFilename)}`, { signal: controller.signal });
        clearTimeout(timeout);
        
        if (!response.ok) throw new Error('Meme not found');
        
        const meme = await response.json();
        
        // Race condition check: if user closed modal or opened another meme, abort
        if (currentMemeId !== memeFilename) {
            console.log('viewMeme: aborted due to meme change');
            return;
        }
        
        const titleEl = document.getElementById('memeTitle');
        titleEl.textContent = escapeHtml(meme.filename);
        titleEl.setAttribute('title', meme.filename);

        const isVideo = /\.(mp4|webm|mov|mkv|avi|flv)$/i.test(meme.filename);
        const imageElement = document.getElementById('memeImage');
        const videoElement = document.getElementById('memeVideo');
        const videoSource = document.getElementById('memeVideoSource');

        if (isVideo) {
            imageElement.style.display = 'none';
            videoElement.style.display = 'block';

            videoSource.src = `/memes/${encodeURIComponent(memeFilename)}/download`;

            const ext = meme.filename.split('.').pop().toLowerCase();
            const mimeTypes = {
                'mp4': 'video/mp4',
                'webm': 'video/webm',
                'mkv': 'video/x-matroska',
                'avi': 'video/x-msvideo',
                'flv': 'video/x-flv'
            };
            videoSource.type = mimeTypes[ext] || 'video/mp4';
            videoElement.load();
            // Auto-play the video
            try {
                await videoElement.play();
            } catch (err) {
                console.log('Autoplay prevented by browser:', err);
            }
        } else {
            videoElement.style.display = 'none';
            imageElement.style.display = 'block';
            imageElement.src = `/memes/${encodeURIComponent(memeFilename)}/preview?size=600`;
        }
        
        // Show memePreview, hide loading spinner
        if (memePreview) memePreview.style.display = 'block';
        if (generationSpinner) generationSpinner.style.display = 'none';
        
        document.getElementById('memeCategory').value = meme.category || '';
        
        const keywordsList = (meme.keywords || '').split(',').map(k => k.trim()).filter(k => k);
        window.currentKeywords = keywordsList;
        renderKeywordBadges();
        
        const keywordInput = document.getElementById('memeKeywordsInput');
        keywordInput.value = '';
        
        // Remove old handler and add new one to prevent accumulation
        const newKeydownHandler = function(e) {
            if (e.key === 'Enter') {
                e.preventDefault();
                const newKeyword = this.value.trim();
                if (newKeyword && !window.currentKeywords.includes(newKeyword)) {
                    window.currentKeywords.push(newKeyword);
                    renderKeywordBadges();
                    this.value = '';
                }
            }
        };
        
        // Clone and replace to remove old listeners
        const newKeywordInput = keywordInput.cloneNode(true);
        keywordInput.parentNode.replaceChild(newKeywordInput, keywordInput);
        newKeywordInput.addEventListener('keydown', newKeydownHandler);
        
        document.getElementById('memeTextInImage').value = meme.text_in_image || '';
        document.getElementById('memeDescription').value = meme.description || '';
        
        const details = [
            `ID: ${meme.id}`,
            `Status: ${meme.processed === true ? 'Processed' : 'Pending'}`,
            meme.size ? `Size: ${(meme.size / 1024 / 1024).toFixed(2)} MB` : '',
            meme.attempts ? `Attempts: ${meme.attempts}` : ''
        ].filter(x => x).join(' | ');
        
        document.getElementById('memeDetails').textContent = details;
        
        const dedupeBtn = document.getElementById('dedupeBtn');
        const recalcBtn = document.getElementById('recalcPhashBtn');
        const retryBtn = document.getElementById('retryDescriptionBtn');
        
        if (!meme.phash) {
            recalcBtn.style.display = 'inline-block';
            dedupeBtn.style.display = 'none';
        } else if (meme.duplicate_group_id && !meme.is_false_positive) {
            dedupeBtn.style.display = 'inline-block';
            recalcBtn.style.display = 'none';
        } else {
            dedupeBtn.style.display = 'none';
            recalcBtn.style.display = 'none';
        }
        
        // Show retry button if attempts >= max_generation_attempts and status is not filled
        if ((meme.attempts || 0) >= maxGenerationAttempts && meme.status !== 'filled' && meme.status !== 'unsupported') {
            retryBtn.style.display = 'inline-block';
        } else {
            retryBtn.style.display = 'none';
        }
    } catch (error) {
        console.error('Error loading meme:', error);
        // Hide spinner and show preview (which now shows error state)
        if (generationSpinner) generationSpinner.style.display = 'none';
        if (memePreview) memePreview.style.display = 'block';
        showError('Failed to load meme: ' + (error.name === 'AbortError' ? 'Request timeout' : error.message));
    }
}

function renderKeywordBadges() {
    const container = document.getElementById('keywordsBadges');
    container.innerHTML = '';
    window.currentKeywords.forEach((keyword, idx) => {
        const badge = document.createElement('span');
        badge.className = 'badge bg-primary d-flex align-items-center gap-2';
        badge.style.padding = '6px 10px';
        badge.style.cursor = 'pointer';
        badge.title = 'Click to search';
        
        const textSpan = document.createElement('span');
        textSpan.textContent = keyword;
        textSpan.style.cursor = 'pointer';
        textSpan.onclick = (e) => {
            e.stopPropagation();
            searchByKeyword(keyword);
        };
        
        const closeBtn = document.createElement('button');
        closeBtn.type = 'button';
        closeBtn.className = 'btn-close btn-close-white';
        closeBtn.style.fontSize = '0.7rem';
        closeBtn.title = 'Remove';
        closeBtn.onclick = (e) => {
            e.stopPropagation();
            removeKeyword(idx);
        };
        
        badge.appendChild(textSpan);
        badge.appendChild(closeBtn);
        container.appendChild(badge);
    });
}

function removeKeyword(idx) {
    window.currentKeywords.splice(idx, 1);
    renderKeywordBadges();
}

function openMemeDetail(filename) {
    try {
        // Open modal and load meme data
        const modalElement = document.getElementById('memeModal');
        const modal = bootstrap.Modal.getOrCreateInstance(modalElement);
        modal.show();
        viewMeme(filename);
    } catch (e) {
        console.error('Unable to open meme detail for', filename, e);
        showError('Failed to open meme details');
    }
}

async function openMemeSimplePreview(filename) {
    try {
        const modalElement = document.getElementById('memeSimplePreviewModal');
        const modal = bootstrap.Modal.getOrCreateInstance(modalElement);
        
        document.getElementById('simpleMemeTitle').textContent = filename;
        
        const imgEl = document.getElementById('simpleMemeImage');
        const vidEl = document.getElementById('simpleMemeVideo');
        const vidSource = document.getElementById('simpleMemeVideoSource');
        const loadingEl = document.getElementById('simpleMemeLoading');
        
        // Reset state
        imgEl.style.display = 'none';
        vidEl.style.display = 'none';
        loadingEl.style.display = 'flex';
        
        modal.show();
        
        const isVideo = /\.(mp4|webm|mov|mkv|avi|flv)$/i.test(filename);
        
        if (isVideo) {
            vidSource.src = `/memes/${encodeURIComponent(filename)}/download`;
            const ext = filename.split('.').pop().toLowerCase();
            const mimeTypes = {
                'mp4': 'video/mp4', 'webm': 'video/webm', 'mkv': 'video/x-matroska',
                'avi': 'video/x-msvideo', 'flv': 'video/x-flv'
            };
            vidSource.type = mimeTypes[ext] || 'video/mp4';
            
            vidEl.oncanplay = () => {
                loadingEl.style.display = 'none';
                vidEl.style.display = 'block';
                vidEl.play().catch(e => console.log('Autoplay blocked', e));
            };
            vidEl.load();
        } else {
            imgEl.onload = () => {
                loadingEl.style.display = 'none';
                imgEl.style.display = 'block';
            };
            imgEl.onerror = () => {
                loadingEl.style.display = 'none';
                showError('Failed to load image preview');
            };
            // Use large preview
            imgEl.src = `/memes/${encodeURIComponent(filename)}/preview?size=800`;
        }
        
    } catch (e) {
        console.error('Unable to open simple preview for', filename, e);
        showError('Failed to open preview');
    }
}

// ======================== Page Initialization ========================

document.addEventListener('DOMContentLoaded', function() {
    console.log('Memes UI: DOM Content Loaded');
    
    // Initialize auth state (show/hide login buttons)
    initializeAuthState();
    
    if (document.getElementById('memesContainer')) {
        console.log('Calling loadMemes');
        loadMemes();
        checkDuplicatesButton();
    } else {
        console.log('memesContainer not present — skipping loadMemes');
    }
}, { once: true });
