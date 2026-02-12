// ======================== Deduplication Panel ========================

async function openDeduplicationPanel(filename) {
    try {
        const response = await fetch(`/memes/${encodeURIComponent(filename)}/duplicates`);
        const data = await response.json();
        
        if (!data.duplicates || data.duplicates.length === 0) {
            showAlert('No duplicates found for this meme', 'error');
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
        
        // Get or create deduplication modal instance
        const dedupModalElement = document.getElementById('deduplicationModal');
        let dedupModal = bootstrap.Modal.getInstance(dedupModalElement);
        if (!dedupModal) {
            dedupModal = new bootstrap.Modal(dedupModalElement, {
                backdrop: true,
                keyboard: true,
                focus: true
            });
        }
        dedupModal.show();
    } catch (error) {
        console.error('Error loading duplicates:', error);
        showAlert('Failed to load duplicates', 'error');
    }
}

function showDeduplicationModal() {
    openDeduplicationPanel(currentMemeId);
}

// ======================== Merge Operations ========================

async function confirmMergeDuplicates(oldPrimaryFilename) {
    // Cache DOM queries
    const primaryRadios = document.querySelectorAll('input[name="primaryMeme"]');
    const selectedPrimary = document.querySelector('input[name="primaryMeme"]:checked')?.value;

    if (!selectedPrimary) {
        showAlert('Please select a file to keep as primary', 'error');
        return;
    }

    const checked = Array.from(document.querySelectorAll('input.select-dup:checked'))
        .map(cb => cb.value);

    const duplicateFilenames = checked.filter(f => f !== selectedPrimary);

    if (duplicateFilenames.length === 0) {
        const allFilenames = Array.from(primaryRadios).map(rb => rb.value);
        const fallback = allFilenames.filter(f => f !== selectedPrimary);
        if (fallback.length === 0) {
            showAlert('Nothing to merge - only one meme in group', 'error');
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

        showAlert('Duplicates merged successfully!', 'success');
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
        showAlert('Failed to merge duplicates: ' + (error.message || ''), 'error');
    }
}

function deleteDuplicateRow(filename) {
    if (!confirm('Delete "' + filename + '" permanently?')) return;
    fetch(`/memes/${encodeURIComponent(filename)}`, { 
        method: 'DELETE',
        headers: getSecurityHeaders()
    })
        .then(resp => {
            if (!resp.ok) throw new Error('Delete failed');
            showAlert('File deleted', 'success');
            const rows = Array.from(document.querySelectorAll('input.select-dup'));
            for (const cb of rows) {
                if (cb.value === filename) {
                    cb.closest('tr')?.remove();
                    break;
                }
            }
            loadMemes();
        })
        .catch(err => { console.error(err); showAlert('Failed to delete', 'error'); });
}

async function mergeSingleDuplicate(filename) {
    // Cache DOM queries
    const primaryRadios = document.querySelectorAll('input[name="primaryMeme"]');
    const selectedPrimary = document.querySelector('input[name="primaryMeme"]:checked')?.value;
    const primary = selectedPrimary || primaryRadios[0]?.value;
    
    if (!primary) { showAlert('No primary selected', 'error'); return; }

    if (!confirm('Merge "' + filename + '" into "' + primary + '"?')) return;

    const metaCheckbox = Array.from(document.querySelectorAll('input[name="includeMeta"]')).find(cb => cb.value === filename);
    const metadataSources = metaCheckbox && metaCheckbox.checked ? [filename] : [];

    await mergeDuplicates(primary, [filename], metadataSources);
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
        
        showAlert('Meme marked as not a duplicate', 'success');
        bootstrap.Modal.getInstance(document.getElementById('deduplicationModal')).hide();
        loadMemes();
    } catch (error) {
        console.error('Error marking not duplicate:', error);
        showAlert('Failed to mark as not duplicate', 'error');
    }
}

// ======================== Duplicates List ========================

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
    
    // Also check for pending memes
    try {
        const pendingBtn = document.getElementById('viewPendingBtn');
        if (!pendingBtn) return;
        const resp = await fetch('/api/pending-memes');
        if (!resp.ok) return;
        const data = await resp.json();
        if (data && data.length > 0) {
            pendingBtn.style.display = 'inline-block';
            pendingBtn.textContent = `⏳ Pending Memes (${data.length})`;
        } else {
            pendingBtn.style.display = 'none';
        }
    } catch (e) {
        console.debug('checkPendingButton failed', e);
    }
}
