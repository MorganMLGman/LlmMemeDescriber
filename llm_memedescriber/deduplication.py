"""Deduplication module for finding and merging duplicate memes using perceptual hashing."""

import logging
import datetime
from io import BytesIO
from typing import List, Dict, Optional, Set, FrozenSet

import imagehash
from PIL import Image
from sqlmodel import Session, select

from .constants import DUPLICATE_THRESHOLD
from .models import Meme, Duplicate, DuplicateGroup, MemeDuplicateGroup
from .storage import WebDavStorage

logger = logging.getLogger(__name__)


def calculate_phash(data: bytes) -> Optional[str]:
    """Calculate perceptual hash for image data.
    
    Args:
        data: Image bytes to hash
        
    Returns:
        Hex string of perceptual hash, or None on failure
    """
    try:
        if not data:
            logger.warning("Cannot calculate phash: empty data")
            return None
            
        if len(data) < 100:  # Suspiciously small, likely corrupted
            logger.warning("Cannot calculate phash: data too small (%d bytes)", len(data))
            return None
            
        img = Image.open(BytesIO(data))
        
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode in ('RGBA', 'LA'):
                background.paste(img, mask=img.split()[-1])
            else:
                background.paste(img)
            img = background
        elif img.mode != 'RGB':
            img = img.convert('RGB')
        
        phash = imagehash.phash(img)
        return str(phash)
    except Exception as e:
        logger.debug("Failed to calculate phash: %s", str(e))
        return None


def hamming_distance(hash1: str, hash2: str) -> int:
    """Calculate Hamming distance between two hash strings (hex format)."""

    if not hash1 or not hash2:
        return 999
    if not isinstance(hash1, str) or not isinstance(hash2, str):
        return 999

    if len(hash1) != 16 or len(hash2) != 16:
        return 999

    try:
        int(hash1, 16)
        int(hash2, 16)
    except ValueError:
        return 999

    try:
        h1 = imagehash.hex_to_hash(hash1)
        h2 = imagehash.hex_to_hash(hash2)
        return h1 - h2
    except Exception as e:
        logger.debug(f"Failed to calculate hamming distance for {hash1} and {hash2}: {e}")
        return 999


def find_duplicate_groups(session: Session) -> List[List[Meme]]:
    """Find groups of duplicate memes based on perceptual hash."""
    memes = session.exec(
        select(Meme).where(Meme.phash.isnot(None))
    ).all()
    
    # Load pairwise exceptions (duplicates table entries marked as false_positive)
    exceptions: Set[FrozenSet[str]] = set()
    try:
        dup_rows = session.exec(select(Duplicate).where(Duplicate.is_false_positive == True)).all()
        for d in dup_rows:
            if d.filename_a and d.filename_b:
                exceptions.add(frozenset({d.filename_a, d.filename_b}))
    except Exception:
        logger.debug("Failed to load Duplicate exceptions table")
    
    logger.debug(f"find_duplicate_groups: Loaded {len(memes)} memes with phash")
    
    if not memes:
        logger.warning("find_duplicate_groups: No memes with phash found")
        return []
    
    groups: Dict[int, List[Meme]] = {}
    assigned = set()
    group_counter = 0
    
    if memes:
        logger.debug(f"First meme phash: {memes[0].phash}, filename: {memes[0].filename}")
        if len(memes) > 1:
            logger.debug(f"Second meme phash: {memes[1].phash}, filename: {memes[1].filename}")
    
    for i, meme1 in enumerate(memes):
        if i in assigned:
            continue
        
        group = [meme1]
        assigned.add(i)
        
        for j, meme2 in enumerate(memes[i + 1:], start=i + 1):
            if j in assigned:
                continue
            pair_key = frozenset({meme1.filename, meme2.filename})
            pair_key = frozenset({meme1.filename, meme2.filename})
            if pair_key in exceptions:
                logger.debug(f"Skipping pair due to user exception: {meme1.filename} <-> {meme2.filename}")
                continue

            distance = hamming_distance(meme1.phash, meme2.phash)
            if distance <= DUPLICATE_THRESHOLD:
                logger.debug(f"Found duplicate: {meme1.filename} <-> {meme2.filename} (distance: {distance})")
                group.append(meme2)
                assigned.add(j)
        
        if len(group) > 1:
            logger.debug(f"Group {group_counter}: {len(group)} memes (distance threshold: {DUPLICATE_THRESHOLD})")
            for meme in group:
                logger.debug(f"  - {meme.filename}")
            groups[group_counter] = group
            group_counter += 1
    
    logger.debug(f"find_duplicate_groups: Found {len(groups)} groups total")
    return list(groups.values())


def mark_false_positive(session: Session, filename: str) -> bool:
    """Mark a meme as false positive (not a duplicate despite similar hash)."""
    meme = session.exec(select(Meme).where(Meme.filename == filename)).first()
    if not meme:
        return False

    meme.is_false_positive = True
    try:
        meme.updated_at = datetime.datetime.now(datetime.timezone.utc)
    except Exception:
        pass
    session.add(meme)
    try:
        links = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.filename == filename)).all()
        for l in links:
            session.delete(l)
    except Exception:
        logger.debug("Failed to remove meme-group links for false-positive marking")

    session.commit()
    logger.info("Marked %s as false positive and cleared group links", filename)
    return True


def add_pair_exception(session: Session, filename_a: str, filename_b: str) -> Duplicate:
    """Create or return a Duplicate record marking the pair as false positive."""
    # Normalize order to keep duplicates unique regardless of order
    a = filename_a
    b = filename_b
    # Check existing in either order
    existing = session.exec(
        select(Duplicate).where(
            ((Duplicate.filename_a == a) & (Duplicate.filename_b == b)) |
            ((Duplicate.filename_a == b) & (Duplicate.filename_b == a))
        )
    ).first()
    if existing:
        if not existing.is_false_positive:
            existing.is_false_positive = True
            session.add(existing)
            session.commit()
        return existing

    dup = Duplicate(filename_a=a, filename_b=b, is_false_positive=True)
    session.add(dup)
    session.commit()
    session.refresh(dup)
    return dup


def remove_pair_exception(session: Session, filename_a: str, filename_b: str) -> bool:
    a = filename_a
    b = filename_b
    existing = session.exec(
        select(Duplicate).where(
            ((Duplicate.filename_a == a) & (Duplicate.filename_b == b)) |
            ((Duplicate.filename_a == b) & (Duplicate.filename_b == a))
        )
    ).first()
    if not existing:
        return False
    session.delete(existing)
    session.commit()
    return True


def list_pair_exceptions(session: Session) -> List[Duplicate]:
    return session.exec(select(Duplicate)).all()


def merge_duplicates(
    session: Session,
    storage: WebDavStorage,
    primary_filename: str,
    duplicate_filenames: List[str],
    merge_metadata: bool = True,
    metadata_sources: Optional[List[str]] = None
) -> bool:
    """Merge duplicate memes, keeping primary and deleting others.
    
    Metadata from duplicate memes is merged into primary if merge_metadata=True.
    
    Args:
        session: Database session
        storage: WebDAV storage for deleting files
        primary_filename: The meme to keep
        duplicate_filenames: List of memes to delete
        merge_metadata: If True, merge keywords/description from duplicates
    
    Returns:
        True if merge succeeded
    """
    try:
        primary = session.exec(
            select(Meme).where(Meme.filename == primary_filename)
        ).first()
        
        if not primary:
            logger.error("Primary meme %s not found", primary_filename)
            return False
        
        duplicates = session.exec(
            select(Meme).where(Meme.filename.in_(duplicate_filenames))
        ).all()
        
        if not duplicates:
            logger.warning("No duplicate memes found for merge")
            return False
        
        if merge_metadata:
            all_keywords = set()
            all_descriptions = []

            if primary.keywords:
                all_keywords.update(k.strip() for k in primary.keywords.split(','))

            # If metadata_sources provided, only merge from those filenames
            sources_set = set(metadata_sources) if metadata_sources else None

            for dup in duplicates:
                # Skip merging metadata from duplicates not in metadata_sources when provided
                if sources_set is not None and dup.filename not in sources_set:
                    continue

                if dup.keywords:
                    all_keywords.update(k.strip() for k in dup.keywords.split(','))
                if dup.description and dup.description not in all_descriptions:
                    all_descriptions.append(dup.description)

            if all_keywords:
                primary.keywords = ', '.join(sorted(all_keywords))

            if all_descriptions:
                if primary.description:
                    primary.description = primary.description + '\n---\n' + '\n---\n'.join(all_descriptions)
                else:
                    primary.description = all_descriptions[0]
        
        session.add(primary)
        # Remove group links for duplicates
        try:
            links = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.filename.in_(duplicate_filenames))).all()
            for l in links:
                session.delete(l)
        except Exception:
            logger.debug("Failed to remove meme-group links for duplicates")

        # Delete duplicate Meme records and files
        for dup in duplicates:
            try:
                storage.delete_file(dup.filename)
                logger.info("Deleted file %s from storage", dup.filename)
            except Exception as e:
                logger.warning("Failed to delete %s from storage: %s", dup.filename, e)

            try:
                session.delete(dup)
            except Exception:
                logger.exception("Failed to delete meme record %s", dup.filename)

        session.commit()

        # Cleanup: remove any duplicate groups that now have <=1 members
        try:
            groups = session.exec(select(DuplicateGroup)).all()
            for g in groups:
                remaining = session.exec(select(MemeDuplicateGroup).where(MemeDuplicateGroup.group_id == g.id)).all()
                if len(remaining) <= 1:
                    # delete remaining links and the group
                    for r in remaining:
                        try:
                            session.delete(r)
                        except Exception:
                            pass
                    try:
                        session.delete(g)
                    except Exception:
                        pass
            session.commit()
        except Exception:
            logger.debug("Failed to cleanup duplicate groups after merge")
        logger.info("Merged %d duplicates into %s", len(duplicates), primary_filename)
        return True
        
    except Exception as e:
        logger.exception("Failed to merge duplicates: %s", e)
        session.rollback()
        return False
