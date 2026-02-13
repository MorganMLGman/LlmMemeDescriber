// ======================== Meme Download Operations ========================

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

// ======================== Clipboard Operations ========================

async function copyMemeToClipboard() {
    if (!currentMemeId) return;

    const isVideo = /\.(mp4|webm|mov|mkv|avi|flv)$/i.test(currentMemeId);
    const isMobile = /iPhone|iPad|iPod|Android/i.test(navigator.userAgent);

    // Helper to get signed share link
    async function getShareLink() {
        try {
            const resp = await fetch(`/memes/${encodeURIComponent(currentMemeId)}/share-link`);
            if (!resp.ok) throw new Error('Failed to generate share link');
            const data = await resp.json();
            return data.url;
        } catch (e) {
            console.error('Error fetching share link:', e);
            return window.location.origin + `/memes/${encodeURIComponent(currentMemeId)}/download`;
        }
    }

    // If it's a video, we fetch a signed temporary link
    if (isVideo) {
        try {
            const shareUrl = await getShareLink();
            await navigator.clipboard.writeText(shareUrl);
            showAlert('Temporary share link copied! (Valid for 24h)', 'success');
        } catch (err) {
            console.error('Failed to copy video URL:', err);
            showError('Failed to copy video URL');
        }
        return;
    }

    // Mobile devices: Use Share API or copy link instead
    if (isMobile) {
        try {
            const downloadUrl = `/memes/${encodeURIComponent(currentMemeId)}/download`;

            // Try Web Share API if available (preferred on mobile)
            if (navigator.share) {
                try {
                    const response = await fetch(downloadUrl);
                    if (!response.ok) throw new Error('Failed to fetch image');
                    const blob = await response.blob();
                    const file = new File([blob], currentMemeId, { type: blob.type });

                    await navigator.share({
                        files: [file],
                        title: currentMemeId
                    });
                    showAlert('Image shared!', 'success');
                    return;
                } catch (shareError) {
                    console.log('Share API failed:', shareError);
                    // Fall through to link copy
                }
            }

            // Fallback: Copy share link for mobile
            const shareUrl = await getShareLink();
            await navigator.clipboard.writeText(shareUrl);
            showAlert('Share link copied! (Images can\'t be copied on mobile)', 'success');
        } catch (err) {
            console.error('Mobile copy failed:', err);
            showError('Copy failed. Try using the download button instead.');
        }
        return;
    }

    // Desktop: Try to copy image data
    try {
        const downloadUrl = `/memes/${encodeURIComponent(currentMemeId)}/download`;
        const response = await fetch(downloadUrl);
        if (!response.ok) throw new Error('Failed to fetch image data');
        const originalBlob = await response.blob();

        let blobToCopy = originalBlob;

        // WebP and PNG are supported by Clipboard API - copy as-is to preserve animation
        // Convert other formats (JPEG, GIF, etc) to PNG for compatibility
        if (originalBlob.type !== 'image/png' && originalBlob.type !== 'image/webp') {
            try {
                blobToCopy = await convertBlobToPng(originalBlob);
            } catch (conversionError) {
                console.warn('PNG conversion failed, trying original blob:', conversionError);
            }
        }

        try {
            const item = new ClipboardItem({ [blobToCopy.type]: blobToCopy });
            await navigator.clipboard.write([item]);
            showAlert('Image copied to clipboard!', 'success');
        } catch (writeError) {
            console.warn('Clipboard write failed:', writeError);
            showError('Browser rejected image data copy');
        }
    } catch (error) {
        console.error('Copy to clipboard error:', error);
        showError('Failed to copy image to clipboard');
    }
}

// Helper function to convert any image blob to a PNG blob
function convertBlobToPng(blob) {
    return new Promise((resolve, reject) => {
        const img = new Image();
        const url = URL.createObjectURL(blob);
        
        img.onload = () => {
            try {
                const canvas = document.createElement('canvas');
                canvas.width = img.width;
                canvas.height = img.height;
                
                const ctx = canvas.getContext('2d');
                ctx.drawImage(img, 0, 0);
                
                canvas.toBlob((pngBlob) => {
                    if (pngBlob) {
                        resolve(pngBlob);
                    } else {
                        reject(new Error('Canvas toBlob returned null'));
                    }
                    URL.revokeObjectURL(url);
                }, 'image/png');
            } catch (e) {
                URL.revokeObjectURL(url);
                reject(e);
            }
        };
        
        img.onerror = (e) => {
            URL.revokeObjectURL(url);
            reject(new Error('Failed to load image for conversion'));
        };
        
        img.src = url;
    });
}
