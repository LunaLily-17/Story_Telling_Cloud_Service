"""Coordinate stateless storytelling agents over HTTP and validate their proposals."""
import asyncio
import math
import operator
import os
from dataclasses import dataclass
from typing import Annotated, Literal, NotRequired, TypedDict

import httpx
from langgraph.graph import END, START, StateGraph
from langgraph.runtime import Runtime

from app.narrative import chapter_contains_hero, fact_is_new
from app.schemas import (
    CONTRIBUTION_MODELS, AgentRequest, AgentResponse, CharacterContribution, DialogueContribution,
    PlotContribution, RefinementContribution,
)

AgentRole = Literal["plot", "character", "dialogue", "refinement"]
ROLES = ("plot", "character", "dialogue", "refinement")
URLS = {role: os.getenv(f"{role.upper()}_URL", f"http://{role}:8000") for role in ROLES}
AGENT_TIMEOUT = float(os.getenv("AGENT_TIMEOUT_SECONDS", "30"))
if not math.isfinite(AGENT_TIMEOUT) or AGENT_TIMEOUT <= 0:
    raise ValueError("AGENT_TIMEOUT_SECONDS must be finite and positive")

class GraphInput(TypedDict):
    story_id: str
    base_version: int
    story_snapshot: dict[str, object]
    user_instruction: str


class WorkflowError(TypedDict):
    stage: AgentRole | Literal["validation"]
    code: Literal["timeout", "transport", "http_status", "invalid_response",
                  "response_mismatch", "narrative_invalid"]
    retryable: bool
    http_status: NotRequired[int]
    detail_code: NotRequired[str]
    retry_after: NotRequired[int]


class ValidationResult(TypedDict):
    valid: bool
    failed_checks: list[str]


class GraphState(GraphInput):
    plot: NotRequired[PlotContribution]
    character: NotRequired[CharacterContribution]
    dialogue: NotRequired[DialogueContribution]
    refinement: NotRequired[RefinementContribution]
    validation_result: NotRequired[ValidationResult]
    errors: Annotated[list[WorkflowError], operator.add]


@dataclass(frozen=True)
class AgentContext:
    client: httpx.AsyncClient
    timeout: float


class WorkflowFailed(Exception):
    def __init__(self, errors: list[WorkflowError]):
        self.errors = errors
        self.retryable = all(error["retryable"] for error in errors)
        self.retry_after = max((error.get("retry_after", 0) for error in errors), default=0)
        message = ", ".join(
            f'{error["stage"]}/{error["code"]}'
            + (f' ({error["http_status"]})' if "http_status" in error else "")
            + (f': {error["detail_code"]}' if "detail_code" in error else "")
            for error in sorted(errors, key=lambda error: error["stage"])
        )
        super().__init__(message)


def agent_node(role: AgentRole, dependencies: tuple[AgentRole, ...] = ()):
    async def contribute(state: GraphState, runtime: Runtime[AgentContext]):
        request = AgentRequest(
            story_id=state["story_id"], version=state["base_version"],
            state=state["story_snapshot"], instruction=state["user_instruction"],
            contributions={dependency: state[dependency] for dependency in dependencies},
        )
        context = runtime.context
        error: WorkflowError
        try:
            # HTTPX bounds individual network operations. This also bounds the whole call.
            async with asyncio.timeout(context.timeout):
                response = await context.client.post(
                    f'{URLS[role].rstrip("/")}/contribute', json=request.model_dump(),
                )
                response.raise_for_status()
                result = AgentResponse.model_validate_json(response.content, strict=True)
                if (result.agent, result.story_id, result.base_version) != (
                    role, request.story_id, request.version,
                ):
                    error = {"stage": role, "code": "response_mismatch", "retryable": False}
                else:
                    return {role: CONTRIBUTION_MODELS[role].model_validate(result.contribution, strict=True).root}
        except (TimeoutError, httpx.TimeoutException):
            error = {"stage": role, "code": "timeout", "retryable": True}
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            error = {
                "stage": role, "code": "http_status", "http_status": status,
                "retryable": status in (408, 429) or 500 <= status < 600,
            }
            if status in (413, 422, 424):
                try:
                    detail_code = exc.response.json()["detail"]["code"]
                except (ValueError, TypeError, KeyError):
                    detail_code = None
                if detail_code in (
                    "context_too_large", "invalid_dependencies", "invalid_proposal",
                    "narrative_invalid", "provider_incomplete", "provider_invalid_output",
                    "provider_output_too_large", "provider_refused",
                    "provider_quota_exhausted", "provider_request_rejected",
                ):
                    error["detail_code"] = detail_code
            retry_after = exc.response.headers.get("Retry-After", "")
            if status == 429 and retry_after.isascii() and retry_after.isdigit() and len(retry_after) <= 4:
                error["retry_after"] = min(60, int(retry_after))
        except httpx.RequestError:
            error = {"stage": role, "code": "transport", "retryable": True}
        except ValueError:
            error = {"stage": role, "code": "invalid_response", "retryable": False}
        return {"errors": [error]}

    return contribute


def validate_narrative(state: GraphState):
    plot = state["plot"]
    hero = state["story_snapshot"].get("hero", "Alex")
    facts = state["story_snapshot"].get("facts", [])
    checks = {
        **state["refinement"]["checks"],
        "plot_version_matches": plot["chapter_number"] == state["base_version"] + 1,
        "character_identity_matches": state["character"]["hero"] == hero,
        "plot_fact_is_new": fact_is_new(plot["new_fact"], facts),
        "chapter_contains_new_fact": plot["new_fact"] in state["refinement"]["text"],
        "chapter_contains_hero": chapter_contains_hero(state["refinement"]["text"], hero),
        "dialogue_references_plot": any(plot["clue"] in line for line in state["dialogue"]["lines"]),
    }
    failed = [name for name, passed in checks.items() if not passed]
    result: ValidationResult = {"valid": not failed, "failed_checks": failed}
    if failed:
        error: WorkflowError = {"stage": "validation", "code": "narrative_invalid", "retryable": False}
        return {"validation_result": result, "errors": [error]}
    return {"validation_result": result}


def next_if_success(node: str):
    def route(state: GraphState):
        return END if state["errors"] else node

    return route


builder = StateGraph(GraphState, input_schema=GraphInput, context_schema=AgentContext)
builder.add_node("plot", agent_node("plot"))
builder.add_node("character", agent_node("character"))
builder.add_node("join", lambda _: {})
builder.add_node("dialogue", agent_node("dialogue", ("plot", "character")))
builder.add_node("refinement", agent_node("refinement", ("plot", "character", "dialogue")))
builder.add_node("validate", validate_narrative)
builder.add_edge(START, "plot")
builder.add_edge(START, "character")
builder.add_edge(["plot", "character"], "join")
for source, target in (("join", "dialogue"), ("dialogue", "refinement"), ("refinement", "validate")):
    builder.add_conditional_edges(source, next_if_success(target), {target: target, END: END})
builder.add_edge("validate", END)
graph = builder.compile()


async def run_workflow(
    state: GraphInput, *, transport: httpx.AsyncBaseTransport | None = None,
    timeout: float = AGENT_TIMEOUT,
) -> GraphState:
    async with httpx.AsyncClient(timeout=timeout, transport=transport, trust_env=False) as client:
        result = await graph.ainvoke(state, context=AgentContext(client, timeout))
    if result["errors"]:
        raise WorkflowFailed(result["errors"])
    return result
