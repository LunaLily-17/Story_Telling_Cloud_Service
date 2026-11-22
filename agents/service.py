"""Independently deployed storytelling proposal services."""
import os
import socket
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from agents.provider import MAX_CONTEXT_BYTES, Provider, ProviderError, configured_provider
from app.narrative import chapter_contains_hero, fact_is_new
from app.schemas import CONTRIBUTION_MODELS, AgentRequest, AgentResponse

DEPENDENCIES = {
    "plot": (),
    "character": (),
    "dialogue": ("plot", "character"),
    "refinement": ("plot", "character", "dialogue"),
}


def valid_proposal(role: str, req: AgentRequest, data: dict) -> bool:
    hero = req.state.get("hero", "Alex")
    if role == "plot":
        return data["chapter_number"] == req.version + 1 and fact_is_new(
            data["new_fact"], req.state.get("facts", []),
        )
    if role == "character":
        return data["hero"] == hero
    plot = req.contributions["plot"]
    if role == "dialogue":
        return any(plot["clue"] in line for line in data["lines"])
    return (
        all(data["checks"].values())
        and plot["new_fact"] in data["text"]
        and chapter_contains_hero(data["text"], hero)
    )


def create_app(role: str, provider_factory=configured_provider):
    if role not in DEPENDENCIES:
        raise RuntimeError("Invalid AGENT_ROLE")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async with provider_factory() as provider:
            app.state.provider = provider
            yield

    app = FastAPI(title=f"{role.title()} Agent", lifespan=lifespan)

    @app.get("/health")
    def health():
        return {"status": "ok", "agent": role, "replica": socket.gethostname()}

    @app.get("/ready")
    def ready():
        return {"ready": True, "role": role}

    @app.post("/contribute", response_model=AgentResponse)
    async def contribute(req: AgentRequest):
        if len(req.model_dump_json().encode()) > MAX_CONTEXT_BYTES:
            raise HTTPException(413, detail={"code": "context_too_large"})
        if set(req.contributions) != set(DEPENDENCIES[role]):
            raise HTTPException(422, detail={"code": "invalid_dependencies"})
        try:
            prior = {
                name: CONTRIBUTION_MODELS[name].model_validate(value, strict=True).root
                for name, value in req.contributions.items()
            }
        except ValidationError:
            raise HTTPException(422, detail={"code": "invalid_dependencies"}) from None
        req = req.model_copy(update={"contributions": prior})
        provider: Provider = app.state.provider
        try:
            proposal = await provider.propose(role, req)
            data = CONTRIBUTION_MODELS[role].model_validate(proposal, strict=True).root
        except ProviderError as exc:
            headers = {"Retry-After": str(exc.retry_after)} if exc.retry_after is not None else None
            raise HTTPException(exc.status, detail={"code": str(exc)}, headers=headers) from None
        except ValidationError:
            raise HTTPException(422, detail={"code": "invalid_proposal"}) from None
        if not valid_proposal(role, req, data):
            raise HTTPException(422, detail={"code": "narrative_invalid"})
        return AgentResponse(
            agent=role, story_id=req.story_id, base_version=req.version, contribution=data,
        )

    return app


app = create_app(os.getenv("AGENT_ROLE", "plot"))
