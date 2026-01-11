import logging
import types

from llm_memedescriber import genai_client


def test_get_client_returns_none_without_api_key():
    genai_client.clear_client()
    assert genai_client.get_client(None) is None
    assert genai_client.get_client("") is None


def test_get_client_creates_singleton_and_ignores_api_key_mismatch(monkeypatch, caplog):
    genai_client.clear_client()
    created = []

    class FakeClient:
        def __init__(self, api_key=None):
            self.api_key = api_key
            created.append(self)

    monkeypatch.setattr(genai_client, "_genai", types.SimpleNamespace(Client=FakeClient), raising=False)

    caplog.set_level(logging.DEBUG)
    c1 = genai_client.get_client("key1")
    assert isinstance(c1, FakeClient)
    assert c1.api_key == "key1"
    assert len(created) == 1
    assert "Created GenAI client singleton" in "\n".join(r.getMessage() for r in caplog.records)

    c2 = genai_client.get_client("other")
    assert c2 is c1


def test_get_client_handles_creation_exception_and_logs(monkeypatch, caplog):
    genai_client.clear_client()

    class BadClient:
        def __init__(self, api_key=None):
            raise RuntimeError("nope")

    monkeypatch.setattr(genai_client, "_genai", types.SimpleNamespace(Client=BadClient), raising=False)

    caplog.set_level(logging.ERROR)
    res = genai_client.get_client("key")
    assert res is None
    assert any("Failed to create GenAI client" in r.getMessage() for r in caplog.records)


def test_clear_client_logs(caplog):
    caplog.set_level(logging.DEBUG)
    genai_client.clear_client()
    assert any("Cleared GenAI client singleton" in r.getMessage() for r in caplog.records)
