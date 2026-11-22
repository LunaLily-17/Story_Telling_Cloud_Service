import asyncio
import json
import os
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi.testclient import TestClient

from agents.mock import generate
from agents.prompts import PROMPTS
from agents.provider import GroqProvider, OpenAIProvider, configured_provider
from agents.service import create_app
from app.schemas import AgentRequest

REQUEST = AgentRequest(
    story_id="story", version=0, state={"hero": "Alex", "facts": []},
    instruction="Search the tunnel",
)
PLOT = generate("plot", REQUEST)
CHARACTER = generate("character", REQUEST)
DIALOGUE = generate("dialogue", REQUEST.model_copy(update={
    "contributions": {"plot": PLOT, "character": CHARACTER},
}))
PRIOR = {
    "plot": {},
    "character": {},
    "dialogue": {"plot": PLOT, "character": CHARACTER},
    "refinement": {"plot": PLOT, "character": CHARACTER, "dialogue": DIALOGUE},
}


def completed(proposal):
    return httpx.Response(200, json={
        "status": "completed",
        "output": [{"type": "message", "content": [
            {"type": "output_text", "text": json.dumps(proposal)},
        ]}],
    })


def provider_app(role, handler, *, timeout=0.5, attempts=2,
                 provider=OpenAIProvider, model="configured-model"):
    @asynccontextmanager
    async def provider_factory():
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
            yield provider(http, model=model, timeout=timeout, attempts=attempts)

    return create_app(role, provider_factory)


def post(client, role="plot", **overrides):
    return client.post("/contribute", json={
        **REQUEST.model_dump(), "contributions": PRIOR[role], **overrides,
    })


@pytest.mark.parametrize("role", tuple(PRIOR))
def test_role_prompt_and_structured_provider_round_trip(role):
    def reply(request):
        assert request.url == "https://api.openai.com/v1/responses"
        payload = json.loads(request.content)
        assert payload["model"] == "configured-model"
        assert payload["text"]["format"]["type"] == "json_object"
        assert payload["store"] is False
        assert PROMPTS[role] in payload["instructions"]
        instruction, context_json = payload["input"].split("\n", 1)
        assert "JSON object" in instruction
        context = json.loads(context_json)
        assert "story_id" not in context
        assert context["base_version"] == REQUEST.version
        assert context["contributions"] == PRIOR[role]
        return completed(generate(role, REQUEST.model_copy(update={"contributions": PRIOR[role]})))

    with TestClient(provider_app(role, reply)) as client:
        response = post(client, role)
    assert response.status_code == 200
    assert response.json()["agent"] == role


def test_default_mock_is_deterministic(monkeypatch):
    monkeypatch.delenv("LLM_PROVIDER", raising=False)
    with TestClient(create_app("plot")) as client:
        first, second = post(client), post(client)
    assert first.status_code == 200
    assert first.json() == second.json()


@pytest.mark.parametrize(
    ("status", "body", "expected_status", "code", "calls"),
    [
        (429, {}, 429, "provider_rate_limited", 2),
        (429, {"error": {"code": "insufficient_quota"}}, 424, "provider_quota_exhausted", 1),
        (401, {"error": {"message": "private details"}}, 424, "provider_request_rejected", 1),
        (503, {}, 503, "provider_unavailable", 2),
        (200, {"status": "incomplete"}, 422, "provider_incomplete", 1),
        (200, {"status": "completed", "output": []}, 422, "provider_invalid_output", 1),
        (200, {"status": "completed", "output": [{"type": "message", "content": [
            {"type": "refusal", "refusal": "private details"},
        ]}]}, 422, "provider_refused", 1),
    ],
)
def test_provider_errors_are_sanitized_and_retries_are_bounded(status, body, expected_status, code, calls):
    count = 0

    def reply(_):
        nonlocal count
        count += 1
        return httpx.Response(status, json=body)

    with TestClient(provider_app("plot", reply)) as client:
        response = post(client)
    assert response.status_code == expected_status
    assert response.json() == {"detail": {"code": code}}
    assert count == calls


def test_rate_limit_retry_can_recover():
    calls = 0

    def reply(_):
        nonlocal calls
        calls += 1
        return httpx.Response(429, json={}, headers={"Retry-After": "0"}) if calls == 1 else completed(PLOT)

    with TestClient(provider_app("plot", reply)) as client:
        assert post(client).status_code == 200
    assert calls == 2


def test_wall_clock_timeout_is_bounded():
    calls = 0

    async def reply(_):
        nonlocal calls
        calls += 1
        await asyncio.sleep(5)

    with TestClient(provider_app("plot", reply, timeout=.02, attempts=1)) as client:
        response = post(client)
    assert response.status_code == 504
    assert response.json()["detail"]["code"] == "provider_timeout"
    assert calls == 1


@pytest.mark.parametrize(("role", "proposal"), [
    ("plot", {**PLOT, "chapter_number": 4}),
    ("character", {**CHARACTER, "hero": "Jo"}),
    ("dialogue", {"lines": ["Alex: Hello", "Jo: Hello"]}),
    ("refinement", {"text": "Alex omits the new fact.", "checks": {
        "chapter_number_matches": True, "hero_preserved": True,
        "clue_is_new": True, "dialogue_uses_current_clue": True,
    }}),
])
def test_narrative_guardrails_for_each_role(role, proposal):
    with TestClient(provider_app(role, lambda _: completed(proposal))) as client:
        response = post(client, role)
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "narrative_invalid"


def test_malformed_types_and_oversized_input_are_rejected():
    with TestClient(provider_app("plot", lambda _: completed({**PLOT, "chapter_number": "1"}))) as client:
        assert post(client).json()["detail"]["code"] == "provider_invalid_output"
        assert post(client, state={"setting": "x" * 128_000}).status_code == 413
        assert post(client, version=-1).status_code == 422
        assert post(client, story_id="bad id").status_code == 422


def test_live_openai_plot():
    if os.getenv("RUN_LIVE_LLM") != "1":
        pytest.skip("Set RUN_LIVE_LLM=1 with explicit OpenAI configuration to make a billable request")
    if os.getenv("LLM_PROVIDER") != "openai":
        pytest.skip("OpenAI mode is not selected")
    assert os.getenv("OPENAI_API_KEY") and os.getenv("LLM_MODEL")
    with TestClient(create_app("plot", configured_provider)) as client:
        response = post(client)
    assert response.status_code == 200, response.json().get("detail")
    assert response.json()["contribution"]["chapter_number"] == 1


def test_long_rate_limit_cooldown_is_delegated_to_worker():
    calls = 0

    def reply(_):
        nonlocal calls
        calls += 1
        return httpx.Response(429, json={}, headers={"Retry-After": "180"})

    with TestClient(provider_app("plot", reply)) as client:
        response = post(client)
    assert response.status_code == 429
    assert response.headers["Retry-After"] == "60"
    assert calls == 1


@pytest.mark.parametrize("role", tuple(PRIOR))
def test_groq_chat_request_and_response(role):
    def reply(request):
        assert request.url == "https://api.groq.com/openai/v1/chat/completions"
        payload = json.loads(request.content)
        assert payload["model"] == "openai/gpt-oss-120b"
        assert payload["reasoning_effort"] == "low"
        fmt = payload["response_format"]
        assert fmt["type"] == "json_schema"
        spec = fmt["json_schema"]
        assert spec["strict"] is True and spec["name"] == f"story_{role}"
        schema = spec["schema"]
        assert schema["additionalProperties"] is False
        assert set(schema["required"]) == set(schema["properties"])
        if role == "dialogue":
            assert schema["properties"]["lines"] == {"type": "array", "items": {"type": "string"}}
        if role == "refinement":
            checks = schema["properties"]["checks"]
            assert checks["additionalProperties"] is False
            assert set(checks["required"]) == set(checks["properties"])
            assert checks["properties"]["semantic_coherent"] == {"type": "boolean"}
        assert "store" not in payload and "text" not in payload
        system, user = payload["messages"]
        assert system["role"] == "system" and PROMPTS[role] in system["content"]
        assert user["role"] == "user"
        prefix, context = user["content"].split("\n", 1)
        assert "JSON" in prefix
        assert json.loads(context)["contributions"] == PRIOR[role]
        data = generate(role, REQUEST.model_copy(update={"contributions": PRIOR[role]}))
        return httpx.Response(200, json={"choices": [{
            "finish_reason": "stop", "message": {"content": json.dumps(data)},
        }]})

    with TestClient(provider_app(role, reply, provider=GroqProvider, model="openai/gpt-oss-120b")) as client:
        response = post(client, role)
    assert response.status_code == 200


@pytest.mark.parametrize(("reply", "status", "code"), [
    (httpx.Response(429, json={}, headers={"Retry-After": "10"}), 429, "provider_rate_limited"),
    (httpx.Response(200, json={"choices": [{"finish_reason": "length", "message": {}}]}),
     422, "provider_incomplete"),
    (httpx.Response(200, json={"choices": [{"finish_reason": "stop", "message": {"content": "{}"}}]}),
     422, "provider_invalid_output"),
])
def test_groq_rejects_failed_or_invalid_replies(reply, status, code):
    with TestClient(provider_app("plot", lambda _: reply, provider=GroqProvider)) as client:
        response = post(client)
    assert response.status_code == status
    assert response.json()["detail"]["code"] == code


def test_configured_groq_uses_its_own_key(monkeypatch):
    monkeypatch.setenv("LLM_PROVIDER", "groq")
    monkeypatch.setenv("LLM_MODEL", "openai/gpt-oss-120b")
    monkeypatch.setenv("GROQ_API_KEY", "test-groq")
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai")

    async def configure():
        async with configured_provider() as provider:
            assert isinstance(provider, GroqProvider)
            assert provider.client.headers["Authorization"] == "Bearer test-groq"

    asyncio.run(configure())


def test_live_groq_plot():
    if os.getenv("RUN_LIVE_LLM") != "1" or os.getenv("LLM_PROVIDER") != "groq":
        pytest.skip("Set RUN_LIVE_LLM=1 with explicit Groq configuration to make a live request")
    assert os.getenv("GROQ_API_KEY") and os.getenv("LLM_MODEL")
    with TestClient(create_app("plot")) as client:
        response = post(client)
    assert response.status_code == 200, response.json().get("detail")
    assert response.json()["contribution"]["chapter_number"] == 1
