import os

from llm_memedescriber.deduplication import calculate_phash, hamming_distance, find_duplicate_groups
from llm_memedescriber.models import Meme, Duplicate
from llm_memedescriber.constants import DUPLICATE_THRESHOLD
from sqlmodel import SQLModel, create_engine, Session



DATA_DIR = os.path.join(os.path.dirname(__file__), "data")


def _create_in_memory_session():
    engine = create_engine("sqlite:///:memory:", echo=False)
    SQLModel.metadata.create_all(engine)
    return Session(engine)


def _mask_ones_val(k: int) -> int:
    """Return integer with lower k bits set (0 <= k <= 64)."""
    if k <= 0:
        return 0
    return (1 << k) - 1


def _hex_from_val(val: int) -> str:
    return f"{val & ((1 << 64) - 1):016x}"


def _hex_ones(k: int, shift: int = 0) -> str:
    """Return hex phash with k ones shifted left by `shift` bits."""
    val = _mask_ones_val(k) << shift
    return _hex_from_val(val)


def _load_test_image(name: str) -> bytes:
    path = os.path.join(DATA_DIR, name)
    with open(path, "rb") as f:
        return f.read()


def test_calculate_phash_returns_hex_for_rgb_image():
    data = _load_test_image("rgb.png")
    phash = calculate_phash(data)
    assert phash is not None
    assert isinstance(phash, str)
    assert len(phash) == 16


def test_calculate_phash_handles_alpha_channel():
    data = _load_test_image("rgba.png")
    phash = calculate_phash(data)
    assert phash is not None


def test_calculate_phash_handles_paletted_images():
    phash = calculate_phash(_load_test_image("paletted.png"))
    assert phash is not None


def test_calculate_phash_is_same_for_identical_images():
    data1 = _load_test_image("rgb.png")
    data2 = _load_test_image("rgb.png")
    
    phash1 = calculate_phash(data1)
    phash2 = calculate_phash(data2)
    assert phash1 == phash2


def test_calculate_phash_returns_none_on_invalid_input():
    assert calculate_phash(b"") is None  # empty
    assert calculate_phash(b"12345") is None  # too small
    assert calculate_phash(b"not an image" * 20) is None  # corrupted


def test_calculate_phash_differs_for_different_images():
    data1 = _load_test_image("rgb.png")
    data2 = _load_test_image("rgb_variant2.png")

    phash1 = calculate_phash(data1)
    phash2 = calculate_phash(data2)
    assert phash1 != phash2


def test_calculate_phash_handles_grayscale_image():
    data = _load_test_image("grayscale.png")
    phash = calculate_phash(data)
    assert phash is not None


def test_calculate_phash_handles_image_formats():
    files = os.listdir(DATA_DIR)
    for fname in files:
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            Warning(f"Test image {fname} not found, skipping.")
            continue
        data = _load_test_image(fname)
        phash = calculate_phash(data)
        assert phash is not None


def test_hamming_distance_zero_for_identical_hashes():
    phash = calculate_phash(_load_test_image("rgb.png"))
    assert phash is not None
    assert hamming_distance(phash, phash) == 0


def test_hamming_distance_positive_for_different_images():
    phash1 = calculate_phash(_load_test_image("rgb.png"))
    phash2 = calculate_phash(_load_test_image("rgb_variant2.png"))
    assert phash1 is not None and phash2 is not None
    d = hamming_distance(phash1, phash2)
    assert d > 0 and d <= 64


def test_hamming_distance_returns_999_for_empty_or_none():
    assert hamming_distance("", "") == 999
    assert hamming_distance(None, "abc") == 999
    assert hamming_distance("abc", None) == 999


def test_hamming_distance_returns_999_on_invalid_hex():
    assert hamming_distance("zzzz", "abcd") == 999


def test_hamming_distance_is_symmetric():
    phash1 = calculate_phash(_load_test_image("rgb.png"))
    phash2 = calculate_phash(_load_test_image("rgb_variant2.png"))
    assert phash1 is not None and phash2 is not None
    d1 = hamming_distance(phash1, phash2)
    d2 = hamming_distance(phash2, phash1)
    assert d1 == d2


def test_hamming_distance_max_distance():
    phash1 = "0000000000000000"
    phash2 = "ffffffffffffffff"
    d = hamming_distance(phash1, phash2)
    assert d == 64


def test_hamming_distance_invalid_length():
    phash1 = "1234"
    phash2 = "abcd"
    d = hamming_distance(phash1, phash2)
    assert d == 999


def test_find_duplicate_groups_detects_similar_memes():
    T = DUPLICATE_THRESHOLD
    small_k = 1 if T >= 1 else 0
    far_k = min(64, T + 1)

    with _create_in_memory_session() as session:
        a = Meme(filename="a.png", phash=_hex_ones(0))
        b = Meme(filename="b.png", phash=_hex_ones(small_k))
        c = Meme(filename="c.png", phash=_hex_ones(64))
        session.add_all([a, b, c])
        session.commit()

        # sanity checks based on dynamic threshold
        assert hamming_distance(a.phash, b.phash) <= T
        assert hamming_distance(a.phash, c.phash) > T

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"a.png", "b.png"} in groups_sets
        assert not any("c.png" in s for s in groups_sets)


def test_find_duplicate_groups_transitive_not_merged():
    T = DUPLICATE_THRESHOLD
    with _create_in_memory_session() as session:
        a_ph = _hex_ones(0)
        b_ph = _hex_ones(1)  # one-bit diff
        c_ph = _hex_ones(T + 1)  # includes b's bit; d(a,c) = T+1 > T, d(b,c) = T <= T

        m1 = Meme(filename="ta.png", phash=a_ph)
        m2 = Meme(filename="tb.png", phash=b_ph)
        m3 = Meme(filename="tc.png", phash=c_ph)
        session.add_all([m1, m2, m3])
        session.commit()

        assert hamming_distance(a_ph, b_ph) <= T
        assert hamming_distance(b_ph, c_ph) <= T
        assert hamming_distance(a_ph, c_ph) > T

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"ta.png", "tb.png"} in groups_sets
        assert not any("tc.png" in s for s in groups_sets)


def test_find_duplicate_groups_multiple_groups():
    T = DUPLICATE_THRESHOLD
    with _create_in_memory_session() as session:
        # group1: low bits
        a = Meme(filename="g1_a.png", phash=_hex_ones(0))
        b = Meme(filename="g1_b.png", phash=_hex_ones(1))
        # group2: high bits (shifted) with T+1 ones -> far from group1
        shift = 64 - (T + 1)
        c = Meme(filename="g2_c.png", phash=_hex_ones(T + 1, shift=shift))
        d_val = (int(c.phash, 16) ^ 1)  # flip one low bit of c -> distance 1
        d = Meme(filename="g2_d.png", phash=_hex_from_val(d_val))

        session.add_all([a, b, c, d])
        session.commit()

        assert hamming_distance(a.phash, b.phash) <= T
        assert hamming_distance(c.phash, d.phash) <= T
        assert hamming_distance(a.phash, c.phash) > T

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"g1_a.png", "g1_b.png"} in groups_sets
        assert {"g2_c.png", "g2_d.png"} in groups_sets


def test_find_duplicate_groups_pair_exception_with_alternate_partner():
    T = DUPLICATE_THRESHOLD
    with _create_in_memory_session() as session:
        a = Meme(filename="ex_a.png", phash=_hex_ones(0))
        b = Meme(filename="ex_b.png", phash=_hex_ones(1))
        c = Meme(filename="ex_c.png", phash=_hex_ones(2))
        session.add_all([a, b, c])
        session.commit()

        # sanity
        assert hamming_distance(a.phash, b.phash) <= T
        assert hamming_distance(a.phash, c.phash) <= T

        # add pairwise exception between a and b
        dup = Duplicate(filename_a="ex_a.png", filename_b="ex_b.png", is_false_positive=True)
        session.add(dup)
        session.commit()

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"ex_a.png", "ex_c.png"} in groups_sets
        assert not any("ex_b.png" in s for s in groups_sets)


def test_find_duplicate_groups_includes_false_positive_flagged_memes():
    with _create_in_memory_session() as session:
        m1 = Meme(filename="fp_a.png", phash="0000000000000000", is_false_positive=True)
        m2 = Meme(filename="fp_b.png", phash="0000000000000001")
        session.add_all([m1, m2])
        session.commit()

        groups = find_duplicate_groups(session)
        groups_sets = [set(m.filename for m in g) for g in groups]
        assert {"fp_a.png", "fp_b.png"} in groups_sets