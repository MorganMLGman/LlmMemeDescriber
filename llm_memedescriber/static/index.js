// Main entry point for bundling
// This file imports all modules in the correct order

// Core must be loaded first (state and utilities)
import './core.js';

// API layer
import './api.js';

// Authentication and search
import './auth-search.js';

// UI components
import './memes-ui.js';

// Meme actions
import './meme-actions.js';

// Deduplication features
import './deduplication.js';

// Sync and download features
import './sync-download.js';

console.log('LlmMemeDescriber bundle loaded successfully');
