"""Two retained Phase 2 tests; explicit invariants with production debug=False."""

import pytest

from source_helpers import consistent, emit, load_uri_cache

URICache = load_uri_cache()


def test_multiple_evictions_ignore_actual_return():
    calls = []

    def delete(uri, logger):
        calls.append(uri)
        return 100  # Controlled callback: disk content grew from recorded size 3.

    cache = URICache(delete, max_total_size_bytes=9, debug_mode=False)
    for uri in ["a", "b", "c"]:
        cache.add(uri, 3)
        cache.mark_unused(uri)
    consistent(cache, {"a": 3, "b": 3, "c": 3})
    cache.add("big", 10)
    consistent(cache, {"big": 10})
    assert set(calls) == {"a", "b", "c"}
    cache.mark_unused("big")
    consistent(cache, {})


def test_add_eviction_failure_mid_sequence_and_resume():
    calls = []
    callback_totals = []
    error = OSError("second deletion failed")

    def delete(uri, logger):
        calls.append(uri)
        callback_totals.append(cache.get_total_size_bytes())
        if len(calls) == 2:
            raise error  # Controlled callback fault on the second eviction.
        return 10 if uri in {"big", "next"} else 3

    cache = URICache(delete, max_total_size_bytes=9, debug_mode=False)
    for uri in ["a", "b", "c"]:
        cache.add(uri, 3)
        cache.mark_unused(uri)
    with pytest.raises(OSError) as caught:
        cache.add("big", 10)
    assert caught.value is error
    assert len(calls) == 2
    remaining = ({"a", "b", "c"} - set(calls)).pop()
    emit("mid_eviction", total=cache.get_total_size_bytes(), expected_total=13)
    consistent(cache, {"big": 10, remaining: 3})
    assert cache._used_uris == {"big"} and cache._unused_uris == {remaining}
    assert callback_totals[1] == 13
    # An ordinary later operation resumes pending eviction; the failed URI's
    # deletion is not retried, because it is already absent from the cache.
    failed = calls[1]
    cache.mark_unused(failed)
    consistent(cache, {"big": 10})
    assert len(calls) == 3
    cache.mark_unused("big")
    consistent(cache, {})
    cache.add(failed, 3)
    cache.mark_unused(failed)
    consistent(cache, {failed: 3})
    # Complete the original reinsertion case by evicting that entry again.
    cache.add("next", 10)
    consistent(cache, {"next": 10})
    assert calls.count(failed) == 2
    cache.mark_unused("next")
    consistent(cache, {})
