"""Whoosh full-text search indexing and querying for memes."""

import os
import shutil
import logging
from whoosh.filedb.filestore import FileStorage
from whoosh.fields import Schema, TEXT, ID, KEYWORD
from whoosh.qparser import QueryParser, OrGroup
from .db_helpers import session_scope
from .models import Meme
from .constants import INDEX_DIR
from sqlmodel import select

logger = logging.getLogger(__name__)

def get_schema() -> Schema:
    """Define Whoosh schema for memes."""
    return Schema(
        id=ID(stored=True),
        filename=TEXT(stored=True, field_boost=2.0),
        description=TEXT(stored=True),
        category=TEXT(stored=True, field_boost=1.5),
        keywords=TEXT(stored=True, field_boost=1.5),
        text_in_image=TEXT(stored=True),
        status=KEYWORD(stored=True),
        processed=KEYWORD(stored=True),
    )

def init_index() -> None:
    """Initialize or open the search index."""
    os.makedirs(INDEX_DIR, exist_ok=True)
    schema = get_schema()
    storage = FileStorage(INDEX_DIR)
    
    # Check if index already exists by trying to open it
    try:
        storage.open_index()
        logger.info("Opening existing Whoosh index at %s", INDEX_DIR)
    except:
        logger.info("Creating new Whoosh index at %s", INDEX_DIR)
        storage.create_index(schema)


def rebuild_index(engine) -> None:
    """Rebuild the search index from database."""
    logger.info("Rebuilding Whoosh search index...")
    
    os.makedirs(INDEX_DIR, exist_ok=True)
    schema = get_schema()
    
    if os.path.exists(INDEX_DIR):
        try:
            shutil.rmtree(INDEX_DIR)
            logger.debug("Removed old index directory")
        except Exception as e:
            logger.warning("Failed to remove old index directory: %s", e)
    
    os.makedirs(INDEX_DIR, exist_ok=True)
    storage = FileStorage(INDEX_DIR)
    
    ix = storage.create_index(schema)
    writer = ix.writer()
    
    try:
        with session_scope(engine) as session:
            memes = session.exec(select(Meme).where(Meme.status != 'removed')).all()

            for meme in memes:
                writer.add_document(
                    id=str(meme.id),
                    filename=meme.filename or '',
                    description=meme.description or '',
                    category=meme.category or '',
                    keywords=meme.keywords or '',
                    text_in_image=meme.text_in_image or '',
                    status=meme.status,
                    processed='true' if meme.status == 'filled' else 'false',
                )
            
            writer.commit()
            logger.info("Indexed %d memes", len(memes))
    except Exception as e:
        logger.exception("Failed to rebuild index: %s", e)
        writer.cancel()
        raise

def add_meme_to_index(meme: Meme) -> None:
    """Add or update a meme in the search index."""
    try:
        os.makedirs(INDEX_DIR, exist_ok=True)
        schema = get_schema()
        storage = FileStorage(INDEX_DIR)
        
        # Try to open existing index, create if doesn't exist
        try:
            ix = storage.open_index()
        except:
            ix = storage.create_index(schema)
        
        writer = ix.writer()
        writer.delete_by_term('id', str(meme.id))
        
        writer.add_document(
            id=str(meme.id),
            filename=meme.filename or '',
            description=meme.description or '',
            category=meme.category or '',
            keywords=meme.keywords or '',
            text_in_image=meme.text_in_image or '',
            status=meme.status,
            processed='true' if meme.status == 'filled' else 'false',
        )
        
        writer.commit()
        logger.debug("Added/updated meme %s in search index", meme.filename)
    except Exception as e:
        logger.warning("Failed to add meme to index: %s", e)

def remove_meme_from_index(meme_id: int) -> None:
    """Remove a meme from the search index."""
    try:
        storage = FileStorage(INDEX_DIR)
        # Try to open index, if it fails the index doesn't exist yet
        try:
            ix = storage.open_index()
            writer = ix.writer()
            writer.delete_by_term('id', str(meme_id))
            writer.commit()
            logger.debug("Removed meme %d from search index", meme_id)
        except:
            logger.debug("Search index not found, nothing to remove")
    except Exception as e:
        logger.warning("Failed to remove meme from index: %s", e)

def search_memes(query_text: str, limit: int = 50, offset: int = 0) -> List[dict]:
    """
    Search memes using Whoosh full-text search.
    
    Supports:
    - Phrase search: "text phrase"
    - Boolean operators: AND, OR, NOT
    - Prefix search: mem*
    - Fuzzy search: meme~
    
    Args:
        query_text: Search query string
        limit: Maximum results to return
        offset: Results offset for pagination
    
    Returns:
        List of matching meme documents
    """
    if not query_text or len(query_text.strip()) < 2:
        return []
    
    try:
        storage = FileStorage(INDEX_DIR)
        # Try to open index, if it fails return empty results
        try:
            ix = storage.open_index()
        except:
            logger.warning("Search index not found")
            return []
        
        ix = storage.open_index()
        searcher = ix.searcher()
        
        parser = QueryParser(
            "description",
            schema=ix.schema,
            group=OrGroup,
        )
        
        try:
            query = parser.parse(query_text)
        except Exception as e:
            logger.debug("Query parse error (fallback to simple search): %s", e)
            from whoosh.query import And, Term
            terms = [
                Term("filename", word) | Term("description", word) |
                Term("category", word) | Term("keywords", word) |
                Term("text_in_image", word)
                for word in query_text.split()
            ]
            query = And(terms) if terms else Term("description", query_text)
        
        results = searcher.search(query, limit=limit + offset)
        results.fragmenter.charlimit = None
        
        memes = []
        for i, result in enumerate(results[offset : offset + limit]):
            memes.append({
                'id': int(result['id']),
                'filename': result['filename'],
                'description': result.get('description', ''),
                'category': result.get('category', ''),
                'keywords': result.get('keywords', ''),
                'text_in_image': result.get('text_in_image', ''),
                'status': result.get('status', 'unknown'),
                'processed': result.get('processed', 'false') == 'true',
                'score': result.score,
            })
        
        searcher.close()
        logger.debug("Search for '%s' returned %d results", query_text, len(memes))
        return memes
        
    except Exception as e:
        logger.exception("Search failed: %s", e)
        return []
