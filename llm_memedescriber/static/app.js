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

async function loadMemes() {
    try {
        console.log('=== Starting loadMemes ===');
        
        console.log('Testing API health...');
        const healthResponse = await fetch(`/health`, { timeout: 2000 });
        console.log('Health check response:', healthResponse.status);
        
        if (!healthResponse.ok) {
            throw new Error(`API not responding: ${healthResponse.status}`);
        }
        
        console.log('API is responsive, initializing memes list...');
        
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
        showError(`Failed to load memes: ${error.message}`);
    }
}

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
    const loadingIndicator = document.getElementById('loadingIndicator');
    
    try {
        const nextBatch = filteredMemes.slice(currentOffset, currentOffset + itemsPerPage);
        
        if (nextBatch.length === 0) {
            const endMessage = document.getElementById('endOfListMessage');
            if (endMessage) {
                endMessage.style.display = 'block';
            }
            if (loadingIndicator) {
                loadingIndicator.style.display = 'none';
            }
            console.log('No more memes to display');
            return;
        }
        
        displayedMemes = displayedMemes.concat(nextBatch);
        currentOffset += itemsPerPage;
        
        if (currentOffset >= filteredMemes.length && !hasMoreMemes) {
            const endMessage = document.getElementById('endOfListMessage');
            if (endMessage) {
                endMessage.style.display = 'block';
            }
        }
        
        renderDisplayedMemes();
        
    } catch (error) {
        console.error('Error loading more memes:', error);
        showError('Error loading more memes');
    } finally {
        const loadingIndicator = document.getElementById('loadingIndicator');
        if (loadingIndicator) {
            loadingIndicator.style.display = 'none';
        }
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
        container.innerHTML = displayedMemes.map(meme => `
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
                         onclick="viewMeme('${meme.filename}')"
                         onerror="this.src='/static/placeholder.png'">
                    <div class="card-body d-flex flex-column cursor-pointer" onclick="viewMeme('${meme.filename}')">
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

function handleSearch() {
    const query = document.getElementById('searchInput').value.toLowerCase().trim();
    const clearBtn = document.getElementById('clearSearchBtn');
    
    if (query.length > 0) {
        clearBtn.style.display = 'block';
    } else {
        clearBtn.style.display = 'none';
    }
    
    searchQuery = query;
    
    if (!query) {
        filteredMemes = allMemes;
    } else {
        filteredMemes = allMemes.filter(meme => 
            meme.filename.toLowerCase().includes(query) ||
            (meme.description && meme.description.toLowerCase().includes(query)) ||
            (meme.category && meme.category.toLowerCase().includes(query)) ||
            (meme.keywords && meme.keywords.toLowerCase().includes(query)) ||
            (meme.text_in_image && meme.text_in_image.toLowerCase().includes(query))
        );
    }
    
    displayedMemes = [];
    currentOffset = 0;
    hasMoreMemes = filteredMemes.length > 0;
    
    const endMessage = document.getElementById('endOfListMessage');
    if (endMessage) {
        endMessage.style.display = 'none';
    }
    
    loadMoreMemes();
}

function clearSearch() {
    document.getElementById('searchInput').value = '';
    document.getElementById('clearSearchBtn').style.display = 'none';
    filteredMemes = allMemes;
    searchQuery = '';
    
    displayedMemes = [];
    currentOffset = 0;
    hasMoreMemes = filteredMemes.length > 0;
    
    const endMessage = document.getElementById('endOfListMessage');
    if (endMessage) {
        endMessage.style.display = 'none';
    }
    
    loadMoreMemes();
}

async function viewMeme(memeFilename) {
    currentMemeId = memeFilename;
    
    try {
        const response = await fetch(`/memes/${encodeURIComponent(memeFilename)}`);
        if (!response.ok) throw new Error('Meme not found');
        
        const meme = await response.json();
        
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
        } else {
            videoElement.style.display = 'none';
            imageElement.style.display = 'block';
            imageElement.src = `/memes/${encodeURIComponent(memeFilename)}/preview?size=600`;
        }
        
        document.getElementById('memeCategory').value = meme.category || '';
        
        const keywordsList = (meme.keywords || '').split(',').map(k => k.trim()).filter(k => k);
        window.currentKeywords = keywordsList;
        renderKeywordBadges();
        
        document.getElementById('memeKeywordsInput').value = '';
        document.getElementById('memeKeywordsInput').onkeydown = function(e) {
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
        
        document.getElementById('memeTextInImage').value = meme.text_in_image || '';
        document.getElementById('memeDescription').value = meme.description || '';
        
        const details = [
            `ID: ${meme.id}`,
            `Status: ${meme.processed === true ? 'Processed' : 'Pending'}`,
            meme.size ? `Size: ${(meme.size / 1024 / 1024).toFixed(2)} MB` : ''
        ].filter(x => x).join(' | ');
        
        document.getElementById('memeDetails').textContent = details;
        
        const dedupeBtn = document.getElementById('dedupeBtn');
        const recalcBtn = document.getElementById('recalcPhashBtn');
        
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
        
        const modal = new bootstrap.Modal(document.getElementById('memeModal'));
        modal.show();
    } catch (error) {
        console.error('Error loading meme:', error);
        showError('Failed to load meme');
    }
}

function searchByKeyword(keyword) {
    const modal = bootstrap.Modal.getInstance(document.getElementById('memeModal'));
    if (modal) {
        modal.hide();
    }
    
    document.getElementById('searchInput').value = keyword;
    handleSearch();
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
            viewMeme(filename);
        } catch (e) {
            console.error('Unable to open meme detail for', filename, e);
            showError('Failed to open meme details');
        }
    }

async function saveMeme() {
    if (!currentMemeId) return;
    
    const category = document.getElementById('memeCategory').value;
    const keywords = (window.currentKeywords || []).join(', ');
    const description = document.getElementById('memeDescription').value;
    
    try {
        const response = await fetch(`/memes/${encodeURIComponent(currentMemeId)}`, {
            method: 'PATCH',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({category, keywords, description})
        });
        
        if (!response.ok) throw new Error('Failed to save');
        
        showSuccess('Meme updated!');
        bootstrap.Modal.getInstance(document.getElementById('memeModal')).hide();
        loadMemes();
    } catch (error) {
        console.error('Error saving meme:', error);
        showError('Failed to save meme');
    }
}

function downloadMeme() {
    if (!currentMemeId) return;
    
    const downloadUrl = `/memes/${encodeURIComponent(currentMemeId)}/download`;
    const link = document.createElement('a');
    link.href = downloadUrl;
    link.download = currentMemeId;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
}

async function deleteMeme() {
    if (!currentMemeId || !confirm('Delete this meme?')) return;
    
    try {
        const response = await fetch(`/memes/${encodeURIComponent(currentMemeId)}`, {
            method: 'DELETE'
        });
        
        if (!response.ok) throw new Error('Failed to delete');
        
        showSuccess('Meme deleted!');
        bootstrap.Modal.getInstance(document.getElementById('memeModal')).hide();
        loadMemes();
        checkDuplicatesButton();
    } catch (error) {
        console.error('Error deleting meme:', error);
        showError('Failed to delete meme');
    }
}

function showError(message) {
    const alert = document.createElement('div');
    alert.className = 'alert alert-danger alert-dismissible fade show position-fixed top-0 start-50 translate-middle-x mt-3';
    alert.style.zIndex = '9999';
    alert.innerHTML = `
        ${escapeHtml(message)}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    document.body.appendChild(alert);
    setTimeout(() => alert.remove(), 5000);
}

function showSuccess(message) {
    const alert = document.createElement('div');
    alert.className = 'alert alert-success alert-dismissible fade show position-fixed top-0 start-50 translate-middle-x mt-3';
    alert.style.zIndex = '9999';
    alert.innerHTML = `
        ${escapeHtml(message)}
        <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
    `;
    document.body.appendChild(alert);
    setTimeout(() => alert.remove(), 3000);
}

function escapeHtml(text) {
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

async function openDeduplicationPanel(filename) {
    try {
        const response = await fetch(`/memes/${encodeURIComponent(filename)}/duplicates`);
        const data = await response.json();
        
        if (!data.duplicates || data.duplicates.length === 0) {
            showError('No duplicates found for this meme');
            return;
        }
        
        const modalContent = document.getElementById('deduplicationContent');

        const allMemes = [data.primary, ...data.duplicates];

        let html = '';
        html += '<div class="dedup-panel">';
        html += `<h6 class="mb-2">Found ${allMemes.length - 1} Similar Meme(s)</h6>`;
        html += '<p class="small text-muted mb-3">Select rows to operate on. Choose the primary to keep; checked rows will be deleted unless they are the primary.</p>';

        html += '<div class="table-responsive">';
        html += '<table class="table table-hover">';
        html += '<thead class="table-dark"><tr>';
        html += '<th style="width:48px;"></th>';
        html += '<th>File</th>';
        html += '<th style="width:160px;">Actions</th>';
        html += '</tr></thead><tbody>';

        allMemes.forEach((meme, idx) => {
            const isPrimary = idx === 0;
            const similarity = isPrimary ? 0 : (64 - (meme.similarity || 0));
            const similarityPercent = isPrimary ? 100 : Math.round((similarity / 64) * 100);

            html += '<tr>';
            html += `<td class="align-middle text-center">`;
            html += `<input class="form-check-input" type="radio" name="primaryMeme" value="${escapeHtml(meme.filename)}" ${isPrimary ? 'checked' : ''}>`;
            html += `</td>`;

            html += '<td class="align-middle">';
            html += `<div class="d-flex align-items-center gap-3">`;
            html += `<img src="${meme.preview_url}" style="height:60px; width:80px; object-fit:cover; border-radius:6px;" alt="preview">`;
            html += `<div class="flex-grow-1">`;
            html += `<a href="#" onclick="openMemeDetail('${escapeHtml(meme.filename)}'); return false;" class="fw-semibold">${escapeHtml(truncateFilename(meme.filename, 60))}</a>`;
            html += `<div class="small text-muted">${escapeHtml(meme.path || '')}</div>`;
            if (!isPrimary) {
                html += `<div class="mt-1 d-flex align-items-center gap-2">`;
                html += `<span class="badge bg-info">Match: ${similarityPercent}%</span>`;
                html += `<div class="form-check form-check-inline mb-0">`;
                html += `<input class="form-check-input" type="checkbox" name="includeMeta" value="${escapeHtml(meme.filename)}" id="meta${idx}" checked>`;
                html += `<label class="form-check-label small" for="meta${idx}">Include metadata</label>`;
                html += `</div>`;
                html += `</div>`;
            } else {
                html += `<div class="mt-1"><strong class="text-success">✓ Keep (Primary)</strong></div>`;
            }
            html += `</div></div>`;
            html += '</td>';

            html += '<td class="align-middle">';
            html += `<div class="d-flex gap-2 justify-content-end">`;
            html += `<button class="btn btn-sm btn-danger" onclick="deleteDuplicateRow('${escapeHtml(meme.filename)}')">Delete</button>`;
            html += `<button class="btn btn-sm btn-primary" onclick="mergeSingleDuplicate('${escapeHtml(meme.filename)}')">Merge</button>`;
            html += `</div>`;
            html += '</td>';
            html += '</tr>';
        });

        html += '</tbody></table></div>';

        html += '<div class="mt-3 d-flex gap-2 justify-content-start">';
        html += `<button class="btn btn-danger" onclick="confirmMergeDuplicates('${escapeHtml(filename)}')">Merge Selected</button>`;
        html += `<button class="btn btn-warning" onclick="markNotDuplicate('${escapeHtml(filename)}')">Mark as Not Duplicate</button>`;
        html += `<button class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>`;
        html += '</div>';

        html += '</div>';

        modalContent.innerHTML = html;
        new bootstrap.Modal(document.getElementById('deduplicationModal')).show();
    } catch (error) {
        console.error('Error loading duplicates:', error);
        showError('Failed to load duplicates');
    }
}

async function confirmMergeDuplicates(oldPrimaryFilename) {
    const selectedPrimary = document.querySelector('input[name="primaryMeme"]:checked')?.value;

    if (!selectedPrimary) {
        showError('Please select a file to keep as primary');
        return;
    }

    const checked = Array.from(document.querySelectorAll('input.select-dup:checked'))
        .map(cb => cb.value);

    const duplicateFilenames = checked.filter(f => f !== selectedPrimary);

    if (duplicateFilenames.length === 0) {
        const allRadios = document.querySelectorAll('input[name="primaryMeme"]');
        const allFilenames = Array.from(allRadios).map(rb => rb.value);
        const fallback = allFilenames.filter(f => f !== selectedPrimary);
        if (fallback.length === 0) {
            showError('Nothing to merge - only one meme in group');
            return;
        }
        if (!confirm('No rows selected — delete ALL ' + fallback.length + ' duplicate file(s)? This cannot be undone.')) {
            return;
        }
        duplicateFilenames.splice(0, duplicateFilenames.length, ...fallback);
    } else {
        if (!confirm('Delete ' + duplicateFilenames.length + ' selected duplicate file(s)? This cannot be undone.')) {
            return;
        }
    }

    const metadataSources = Array.from(document.querySelectorAll('input[name="includeMeta"]:checked'))
        .map(cb => cb.value)
        .filter(fn => fn !== selectedPrimary);

    await mergeDuplicates(selectedPrimary, duplicateFilenames, metadataSources);
}

async function mergeDuplicates(primaryFilename, duplicateFilenames, metadataSources) {
    try {
        const body = {
            primary_filename: primaryFilename,
            duplicate_filenames: duplicateFilenames,
            merge_metadata: true
        };
        if (Array.isArray(metadataSources) && metadataSources.length > 0) {
            body.metadata_sources = metadataSources;
        }

        const response = await fetch('/memes/merge-duplicates', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(body)
        });

        if (!response.ok) {
            const err = await response.json().catch(() => ({}));
            throw new Error(err.detail || 'Merge failed');
        }

        showSuccess('Duplicates merged successfully!');
        try {
            const dedupEl = document.getElementById('deduplicationModal');
            const dedupInstance = dedupEl && bootstrap && bootstrap.Modal ? bootstrap.Modal.getInstance(dedupEl) : null;
            if (dedupInstance && typeof dedupInstance.hide === 'function') dedupInstance.hide();

            const memeEl = document.getElementById('memeModal');
            const memeInstance = memeEl && bootstrap && bootstrap.Modal ? bootstrap.Modal.getInstance(memeEl) : null;
            if (memeInstance && typeof memeInstance.hide === 'function') memeInstance.hide();
        } catch (e) {
            console.debug('No modals to hide on this page');
        }
        
        const isDuplicatesPage = window.location.pathname.includes('/duplicates');
        if (isDuplicatesPage) {
            setTimeout(() => location.reload(), 400);
        } else {
            loadMemes();
            checkDuplicatesButton();
        }
    } catch (error) {
        console.error('Error merging duplicates:', error);
        showError('Failed to merge duplicates: ' + (error.message || ''));
    }
}

function deleteDuplicateRow(filename) {
    if (!confirm('Delete "' + filename + '" permanently?')) return;
    fetch(`/memes/${encodeURIComponent(filename)}`, { method: 'DELETE' })
        .then(resp => {
            if (!resp.ok) throw new Error('Delete failed');
            showSuccess('File deleted');
            const rows = Array.from(document.querySelectorAll('input.select-dup'));
            for (const cb of rows) {
                if (cb.value === filename) {
                    cb.closest('tr')?.remove();
                    break;
                }
            }
            loadMemes();
        })
        .catch(err => { console.error(err); showError('Failed to delete'); });
}

async function mergeSingleDuplicate(filename) {
    const selectedPrimary = document.querySelector('input[name="primaryMeme"]:checked')?.value;
    const primary = selectedPrimary || document.querySelector('input[name="primaryMeme"]')?.value;
    if (!primary) { showError('No primary selected'); return; }

    if (!confirm('Merge "' + filename + '" into "' + primary + '"?')) return;

    const metaCheckbox = Array.from(document.querySelectorAll('input[name="includeMeta"]')).find(cb => cb.value === filename);
    const metadataSources = metaCheckbox && metaCheckbox.checked ? [filename] : [];

    await mergeDuplicates(primary, [filename], metadataSources);
}

function cssEscape(s) {
    return s.replace(/"/g, '\\"').replace(/'/g, "\\'");
}

async function markNotDuplicate(filename) {
    if (!confirm('Mark this meme as not a duplicate? It will not appear in duplicate groups.')) {
        return;
    }
    
    try {
        const response = await fetch(`/memes/${encodeURIComponent(filename)}/mark-not-duplicate`, {
            method: 'POST'
        });
        
        if (!response.ok) throw new Error('Failed to mark');
        
        showSuccess('Meme marked as not a duplicate');
        bootstrap.Modal.getInstance(document.getElementById('deduplicationModal')).hide();
        loadMemes();
    } catch (error) {
        console.error('Error marking not duplicate:', error);
        showError('Failed to mark as not duplicate');
    }
}

async function showDuplicatesList() {
    const panel = document.getElementById('duplicatesListPanel');
    const content = document.getElementById('duplicatesListContent');
    
    panel.style.display = 'block';
    content.innerHTML = '<div class="spinner-border spinner-border-sm" role="status"><span class="visually-hidden">Loading...</span></div>';
    
    try {
        const response = await fetch('/memes/duplicates-by-group');
        const data = await response.json();
        
        if (!data.groups || data.groups.length === 0) {
            content.innerHTML = '<p class="text-muted">No duplicate groups found.</p>';
            return;
        }
        
        let html = `<p class="mb-3"><strong>Found ${data.total_groups} duplicate group(s)</strong></p>`;
        
        data.groups.forEach((group, groupIdx) => {
            html += `<div class="border rounded p-3 mb-3 bg-light">`;
            html += `<h6>Group ${group.group_id + 1}: ${group.count} meme(s)</h6>`;
            html += `<div class="row g-2">`;
            
            group.memes.forEach((meme) => {
                html += `<div class="col-md-6 col-lg-4">`;
                html += `<div class="border rounded p-2 bg-white text-center">`;
                html += `<img src="${meme.preview_url}" style="height: 100px; object-fit: contain; margin-bottom: 8px;" alt="Meme">`;
                html += `<p class="small text-truncate mb-1" title="${escapeHtml(meme.filename)}">`;
                html += `${escapeHtml(meme.filename.substring(0, 30))}${meme.filename.length > 30 ? '...' : ''}`;
                html += `</p>`;
                html += `<button class="btn btn-xs btn-sm btn-warning" onclick="openMemeDetail('${escapeHtml(meme.filename)}')">View</button>`;
                html += `</div></div>`;
            });
            
            html += `</div></div>`;
        });
        
        content.innerHTML = html;
    } catch (error) {
        console.error('Error loading duplicates:', error);
        content.innerHTML = `<p class="text-danger">Error loading duplicates: ${error.message}</p>`;
    }
}

async function checkDuplicatesButton() {
    try {
        const viewBtn = document.getElementById('viewDuplicatesBtn');
        if (!viewBtn) return;
        const resp = await fetch('/memes/duplicates-by-group');
        if (!resp.ok) return;
        const data = await resp.json();
        if (data && data.total_groups > 0) {
            viewBtn.style.display = 'inline-block';
        } else {
            viewBtn.style.display = 'none';
        }
    } catch (e) {
        console.debug('checkDuplicatesButton failed', e);
    }
}

function showDeduplicationModal() {
    openDeduplicationPanel(currentMemeId);
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

document.addEventListener('DOMContentLoaded', function() {
    console.log('DOM Content Loaded');
    
    // Set up video autoplay/stop listeners for the modal
    const memeModalEl = document.getElementById('memeModal');
    if (memeModalEl) {
        memeModalEl.addEventListener('show.bs.modal', () => {
            const video = document.getElementById('memeVideo');
            if (video && video.style.display === 'block') {
                // Use setTimeout to ensure video is fully ready
                setTimeout(() => {
                    video.play().catch(err => console.log('Autoplay failed:', err));
                }, 50);
            }
        });
        
        memeModalEl.addEventListener('hide.bs.modal', () => {
            const video = document.getElementById('memeVideo');
            if (video) {
                video.pause();
                video.currentTime = 0;
            }
        });
    }
    
    if (document.getElementById('memesContainer')) {
        console.log('Calling loadMemes');
        loadMemes();
        checkDuplicatesButton();
    } else {
        console.log('memesContainer not present — skipping loadMemes');
    }
});
