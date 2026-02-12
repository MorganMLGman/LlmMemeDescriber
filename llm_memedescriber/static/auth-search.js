// ======================== Authentication Management ========================

function showLogoutButton() {
    fetch('/api/csrf-token')
        .then(response => {
            const logoutBtn = document.getElementById('logoutBtn');
            const tokensBtn = document.getElementById('tokensBtn');
            const logoutBtnMobile = document.getElementById('logoutBtnMobile');
            const tokensBtnMobile = document.getElementById('tokensBtnMobile');
            const logoutBtnDefault = document.getElementById('logoutBtnMobileDefault');
            const tokensBtnDefault = document.getElementById('tokensBtnMobileDefault');
            const isAuthenticated = response.status !== 401;

            if (logoutBtn) {
                logoutBtn.style.display = isAuthenticated ? 'inline-block' : 'none';
            }
            if (tokensBtn) {
                tokensBtn.style.display = isAuthenticated ? 'inline-block' : 'none';
            }
            if (logoutBtnMobile) {
                logoutBtnMobile.style.display = isAuthenticated ? 'block' : 'none';
            }
            if (tokensBtnMobile) {
                tokensBtnMobile.style.display = isAuthenticated ? 'block' : 'none';
            }
            if (logoutBtnDefault) {
                logoutBtnDefault.style.display = isAuthenticated ? 'block' : 'none';
            }
            if (tokensBtnDefault) {
                tokensBtnDefault.style.display = isAuthenticated ? 'block' : 'none';
            }

            // If authenticated, fetch and display username
            if (isAuthenticated) {
                fetch('/auth/user', { credentials: 'include' })
                    .then(userResponse => userResponse.json())
                    .then(userData => {
                        let username = userData.name || userData.user_id || 'User';
                        // If username is all lowercase, convert to uppercase
                        if (username === username.toLowerCase() && username !== username.toUpperCase()) {
                            username = username.toUpperCase();
                        }
                        if (logoutBtn) {
                            logoutBtn.textContent = username;
                            logoutBtn.setAttribute('data-logout-text', 'LOGOUT');
                            logoutBtn.addEventListener('mouseenter', function() {
                                this.textContent = this.getAttribute('data-logout-text');
                            });
                            logoutBtn.addEventListener('mouseleave', function() {
                                this.textContent = username;
                            });
                        }
                        if (logoutBtnMobile) {
                            logoutBtnMobile.textContent = username;
                            logoutBtnMobile.setAttribute('data-logout-text', 'LOGOUT');
                            logoutBtnMobile.addEventListener('mouseenter', function() {
                                this.textContent = this.getAttribute('data-logout-text');
                            });
                            logoutBtnMobile.addEventListener('mouseleave', function() {
                                this.textContent = username;
                            });
                        }
                    })
                    .catch(err => console.log('Could not fetch user info:', err));
            }
        })
        .catch(() => {
            const logoutBtn = document.getElementById('logoutBtn');
            const tokensBtn = document.getElementById('tokensBtn');
            const logoutBtnMobile = document.getElementById('logoutBtnMobile');
            const tokensBtnMobile = document.getElementById('tokensBtnMobile');
            const logoutBtnDefault = document.getElementById('logoutBtnMobileDefault');
            const tokensBtnDefault = document.getElementById('tokensBtnMobileDefault');
            if (logoutBtn) logoutBtn.style.display = 'none';
            if (tokensBtn) tokensBtn.style.display = 'none';
            if (logoutBtnMobile) logoutBtnMobile.style.display = 'none';
            if (tokensBtnMobile) tokensBtnMobile.style.display = 'none';
            if (logoutBtnDefault) logoutBtnDefault.style.display = 'none';
            if (tokensBtnDefault) tokensBtnDefault.style.display = 'none';
        });
}

function initializeAuthState() {
    // Initialize authentication state and toggle button visibility
    showLogoutButton();
}

async function logout() {
    if (confirm('Are you sure you want to logout?')) {
        try {
            const response = await fetch('/auth/logout', {
                method: 'POST',
                credentials: 'include',
                headers: getSecurityHeaders()
            });
            
            if (response.ok) {
                window.location.href = '/login';
            } else {
                console.error('Logout failed:', response.status);
                alert('Failed to logout');
            }
        } catch (error) {
            console.error('Logout error:', error);
            alert('Error during logout');
        }
    }
}

// ======================== Search Functionality ========================

function handleSearch() {
    const desktopInput = document.getElementById('searchInput');
    
    let query = desktopInput?.value?.toLowerCase().trim() || '';
    
    const clearBtn = document.getElementById('clearSearchBtn');
    
    if (query.length > 0) {
        if (clearBtn) clearBtn.style.display = 'block';
    } else {
        if (clearBtn) clearBtn.style.display = 'none';
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
    const clearBtn = document.getElementById('clearSearchBtn');
    if (clearBtn) clearBtn.style.display = 'none';
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

function searchByKeyword(keyword) {
    const modal = bootstrap.Modal.getInstance(document.getElementById('memeModal'));
    if (modal) {
        modal.hide();
    }
    
    document.getElementById('searchInput').value = keyword;
    handleSearch();
}

// ======================== Prompt Modal ========================

async function showPromptModal() {
    const promptModal = new bootstrap.Modal(document.getElementById('promptModal'));
    
    try {
        const response = await fetch('/api/prompt');
        const data = await response.json();
        
        document.getElementById('promptTextarea').value = data.prompt;
        document.getElementById('promptSource').textContent = data.source === 'custom' ? 'Custom Prompt' : 'Default Prompt';
        document.getElementById('promptSource').className = `badge ${data.source === 'custom' ? 'bg-warning' : 'bg-info'}`;
    } catch (error) {
        console.error('Error loading prompt:', error);
        alert('Failed to load prompt');
    }
    
    promptModal.show();
}

async function savePrompt() {
    const promptText = document.getElementById('promptTextarea').value.trim();
    
    if (!promptText) {
        alert('Prompt cannot be empty');
        return;
    }
    
    try {
        const response = await fetch('/api/prompt', {
            method: 'POST',
            headers: getSecurityHeaders(),
            body: JSON.stringify({ prompt: promptText })
        });
        
        if (!response.ok) {
            throw new Error('Failed to save prompt');
        }

        await response.json();
        alert('Prompt saved successfully!');
        document.getElementById('promptSource').textContent = 'Custom Prompt';
        document.getElementById('promptSource').className = 'badge bg-warning';
        
        const promptModal = bootstrap.Modal.getInstance(document.getElementById('promptModal'));
        promptModal.hide();
    } catch (error) {
        console.error('Error saving prompt:', error);
        alert('Failed to save prompt');
    }
}
