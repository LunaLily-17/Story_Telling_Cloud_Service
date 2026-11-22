import asyncio
import json

import httpx
import pytest

from agents.mock import generate
from app.graph import WorkflowFailed, run_workflow
from app.schemas import AgentRequest

INPUT = {
    "story_id": "map",
    "base_version": 3,
    "story_snapshot": {"hero": "Alex", "facts": []},
    "user_instruction": "Search the tunnel carefully",
}


def agent_response(request):
    role = request.url.host
    body = AgentRequest.model_validate_json(request.content)
    return {
        "agent": role,
        "story_id": body.story_id,
        "base_version": body.version,
        "contribution": generate(role, body),
    }


def test_parallel_fan_out_and_ordered_http_handoffs():
    async def scenario():
        started = set()
        both_started = asyncio.Event()
        returned = {}

        async def handler(request):
            role = request.url.host
            body = json.loads(request.content)
            assert request.method == "POST" and request.url.path == "/contribute"
            assert body["story_id"] == INPUT["story_id"]
            assert body["version"] == INPUT["base_version"]
            assert body["state"] == INPUT["story_snapshot"]
            assert body["instruction"] == INPUT["user_instruction"]
            if role in {"plot", "character"}:
                assert not body["contributions"]
                started.add(role)
                if len(started) == 2:
                    both_started.set()
                await both_started.wait()
            elif role == "dialogue":
                assert set(returned) == {"plot", "character"}
                assert body["contributions"] == returned
            else:
                assert set(returned) == {"plot", "character", "dialogue"}
                assert body["contributions"] == returned
            data = agent_response(request)
            returned[role] = data["contribution"]
            return httpx.Response(200, json=data)

        result = await run_workflow(INPUT, transport=httpx.MockTransport(handler), timeout=1)
        assert result["validation_result"] == {"valid": True, "failed_checks": []}
        assert result["errors"] == []
        assert {role: result[role] for role in returned} == returned
        assert "Alex" in result["refinement"]["text"]

    asyncio.run(scenario())


@pytest.fixture(autouse=True)
def local_agent_urls(monkeypatch):
    monkeypatch.setattr("app.graph.URLS", {
        role: f"http://{role}:8000" for role in ("plot", "character", "dialogue", "refinement")
    })


def test_parallel_failures_are_combined_and_block_dialogue():
    called = set()

    async def handler(request):
        role = request.url.host
        called.add(role)
        if role == "plot":
            return httpx.Response(503, text="private provider details")
        return httpx.Response(422, text="invalid request")

    with pytest.raises(WorkflowFailed) as failed:
        asyncio.run(run_workflow(INPUT, transport=httpx.MockTransport(handler)))
    assert called == {"plot", "character"}
    assert not failed.value.retryable
    assert {error["stage"]: error["http_status"] for error in failed.value.errors} == {
        "plot": 503, "character": 422,
    }
    assert "private provider details" not in str(failed.value)


def test_whole_request_timeout_is_retryable_and_stops_dependents():
    called = set()

    async def handler(request):
        role = request.url.host
        called.add(role)
        if role == "plot":
            await asyncio.sleep(10)
        return httpx.Response(200, json=agent_response(request))

    with pytest.raises(WorkflowFailed) as failed:
        asyncio.run(run_workflow(INPUT, transport=httpx.MockTransport(handler), timeout=0.05))
    assert called == {"plot", "character"}
    assert failed.value.retryable
    assert failed.value.errors == [{"stage": "plot", "code": "timeout", "retryable": True}]


@pytest.mark.parametrize(
    ("change", "code"),
    [
        (lambda data: data.update(agent="plot"), "response_mismatch"),
        (lambda data: data.update(story_id="other"), "response_mismatch"),
        (lambda data: data.update(base_version=1), "response_mismatch"),
        (lambda data: data.update(contribution={"lines": "a string"}), "invalid_response"),
    ],
)
def test_bad_dialogue_response_stops_refinement(change, code):
    called = set()

    async def handler(request):
        role = request.url.host
        called.add(role)
        data = agent_response(request)
        if role == "dialogue":
            change(data)
        return httpx.Response(200, json=data)

    with pytest.raises(WorkflowFailed) as failed:
        asyncio.run(run_workflow(INPUT, transport=httpx.MockTransport(handler)))
    assert called == {"plot", "character", "dialogue"}
    assert failed.value.errors == [{"stage": "dialogue", "code": code, "retryable": False}]


@pytest.mark.parametrize(("status", "retryable"), [(429, True), (503, True), (404, False)])
def test_http_failure_classification(status, retryable):
    async def handler(request):
        if request.url.host == "refinement":
            return httpx.Response(status)
        return httpx.Response(200, json=agent_response(request))

    with pytest.raises(WorkflowFailed) as failed:
        asyncio.run(run_workflow(INPUT, transport=httpx.MockTransport(handler)))
    assert failed.value.retryable is retryable
    assert failed.value.errors == [{
        "stage": "refinement", "code": "http_status", "http_status": status, "retryable": retryable,
    }]


def test_deterministic_validation_rejects_false_refinement_claims():
    async def handler(request):
        role = request.url.host
        data = agent_response(request)
        if role == "plot":
            data["contribution"]["chapter_number"] = 900
        if role == "refinement":
            data["contribution"]["checks"]["chapter_number_matches"] = True
        return httpx.Response(200, json=data)

    with pytest.raises(WorkflowFailed) as failed:
        asyncio.run(run_workflow(INPUT, transport=httpx.MockTransport(handler)))
    assert failed.value.errors == [{
        "stage": "validation", "code": "narrative_invalid", "retryable": False,
    }]


def test_dependency_failure_surfaces_sanitized_provider_reason():
    async def handler(request):
        if request.url.host == "plot":
            return httpx.Response(424, json={"detail": {
                "code": "provider_quota_exhausted", "message": "private details",
            }})
        return httpx.Response(200, json=agent_response(request))

    with pytest.raises(WorkflowFailed) as failed:
        asyncio.run(run_workflow(INPUT, transport=httpx.MockTransport(handler)))
    assert str(failed.value) == "plot/http_status (424): provider_quota_exhausted"
    assert not failed.value.retryable


def test_refinement_rejection_reports_safe_reason_without_retry():
    async def handler(request):
        if request.url.host == "refinement":
            return httpx.Response(422, json={"detail": {
                "code": "narrative_invalid", "message": "private prose",
            }})
        return httpx.Response(200, json=agent_response(request))

    with pytest.raises(WorkflowFailed) as failed:
        asyncio.run(run_workflow(INPUT, transport=httpx.MockTransport(handler)))
    assert str(failed.value) == "refinement/http_status (422): narrative_invalid"
    assert not failed.value.retryable
