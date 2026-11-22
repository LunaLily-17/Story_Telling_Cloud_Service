"""Pluggable rule-based and hosted contribution providers."""
import asyncio
import json
import math
import os
from contextlib import asynccontextmanager
from typing import Protocol

import httpx

from agents.mock import generate
from agents.prompts import COMMON, PROMPTS
from app.schemas import CONTRIBUTION_MODELS, AgentRequest

MAX_RESPONSE_CHARS = 32_000
MAX_CONTEXT_BYTES = 128_000
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
GROQ_CHAT_URL = "https://api.groq.com/openai/v1/chat/completions"


class ProviderError(Exception):
    def __init__(self, code: str, status: int, retry_after: int | None = None):
        super().__init__(code)
        self.status = status
        self.retry_after = retry_after


class Provider(Protocol):
    async def propose(self, role: str, request: AgentRequest) -> dict: ...


class MockProvider:
    async def propose(self, role: str, request: AgentRequest) -> dict:
        return generate(role, request)


class HTTPProvider:
    url: str

    def __init__(self, client: httpx.AsyncClient, *, model: str, timeout: float, attempts: int):
        self.client = client
        self.model = model
        self.timeout = timeout
        self.attempts = attempts

    async def propose(self, role: str, request: AgentRequest) -> dict:
        context = request.model_dump(exclude={"story_id"})
        context["base_version"] = context.pop("version")
        instructions = f"{COMMON}\n\n{PROMPTS[role]}"
        context_text = "Return a JSON object for this story context.\n" + json.dumps(
            context, ensure_ascii=False,
        )
        payload = self.payload(role, instructions, context_text, 2400 if role == "refinement" else 1200)
        for attempt in range(self.attempts):
            delay = 0.25 * 2**attempt
            try:
                async with asyncio.timeout(self.timeout):
                    response = await self.client.post(self.url, json=payload)
                    response.raise_for_status()
                    return self.parse(role, response)
            except (TimeoutError, httpx.TimeoutException):
                error = ProviderError("provider_timeout", 504)
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status == 429:
                    try:
                        if exc.response.json().get("error", {}).get("code") == "insufficient_quota":
                            raise ProviderError("provider_quota_exhausted", 424) from None
                    except (ValueError, AttributeError):
                        pass
                    try:
                        retry_after = float(exc.response.headers.get("Retry-After", "0"))
                    except ValueError:
                        retry_after = 0
                    cooldown = math.ceil(min(60, retry_after)) if retry_after > 0 else None
                    error = ProviderError("provider_rate_limited", 429, cooldown)
                    # Delegate longer waits to the durable queue.
                    if retry_after > 2:
                        raise error from None
                    delay = max(delay, retry_after)
                elif status == 408 or 500 <= status < 600:
                    error = ProviderError("provider_unavailable", 503)
                else:
                    raise ProviderError("provider_request_rejected", 424) from None
            except httpx.RequestError:
                error = ProviderError("provider_unavailable", 503)
            if attempt + 1 == self.attempts:
                raise error
            await asyncio.sleep(delay)
        raise AssertionError("Provider requires at least one attempt")


class OpenAIProvider(HTTPProvider):
    url = OPENAI_RESPONSES_URL

    def payload(self, role, instructions, context, output_tokens):
        return {
            "model": self.model,
            "instructions": instructions,
            "input": context,
            "text": {"format": {"type": "json_object"}},
            "max_output_tokens": output_tokens,
            "store": False,
        }

    def parse(self, role, response):
        return parse_response(role, response)


class GroqProvider(HTTPProvider):
    url = GROQ_CHAT_URL

    def payload(self, role, instructions, context, output_tokens):
        payload = {
            "model": self.model,
            "messages": [{"role": "system", "content": instructions},
                         {"role": "user", "content": context}],
            "response_format": {"type": "json_object"},
            "max_completion_tokens": output_tokens,
        }
        if self.model in {"openai/gpt-oss-20b", "openai/gpt-oss-120b"}:
            payload["reasoning_effort"] = "low"
        if self.model in {
            "openai/gpt-oss-20b", "openai/gpt-oss-120b",
            "qwen/qwen3.8-27b", "meta-models/muse-spark-open",
        }:
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": f"story_{role}",
                    "strict": True,
                    "schema": groq_schema(CONTRIBUTION_MODELS[role].model_json_schema()),
                },
            }
        return payload

    def parse(self, role, response):
        try:
            choice = response.json()["choices"][0]
            message = choice["message"]
            if message.get("refusal") or choice["finish_reason"] == "content_filter":
                raise ProviderError("provider_refused", 422)
            if choice["finish_reason"] != "stop":
                raise ProviderError("provider_incomplete", 422)
            return validate_output(role, message["content"])
        except (AttributeError, IndexError, KeyError, TypeError, ValueError):
            raise ProviderError("provider_invalid_output", 422) from None


def groq_schema(schema):
    # Inline the role models' references and enforce shape at the provider boundary.
    definitions = schema.get("$defs", {})

    def convert(node):
        if "$ref" in node:
            node = definitions[node["$ref"].removeprefix("#/$defs/")]
        result = {"type": node["type"]}
        if "properties" in node:
            result["properties"] = {
                name: convert(value) for name, value in node["properties"].items()
            }
            result["required"] = list(result["properties"])
            result["additionalProperties"] = False
        if "items" in node:
            result["items"] = convert(node["items"])
        return result

    return convert(schema)


def validate_output(role, content):
    if not isinstance(content, str):
        raise ProviderError("provider_invalid_output", 422)
    if len(content) > MAX_RESPONSE_CHARS:
        raise ProviderError("provider_output_too_large", 422)
    return CONTRIBUTION_MODELS[role].model_validate_json(content, strict=True).root


def parse_response(role: str, response: httpx.Response) -> dict:
    try:
        data = response.json()
        if data["status"] != "completed":
            raise ProviderError("provider_incomplete", 422)
        parts = [part for item in data["output"] if item["type"] == "message"
                 for part in item["content"]]
        if any(part["type"] == "refusal" for part in parts):
            raise ProviderError("provider_refused", 422)
        result = "".join(part["text"] for part in parts if part["type"] == "output_text")
        return validate_output(role, result)
    except (KeyError, TypeError, ValueError):
        raise ProviderError("provider_invalid_output", 422) from None


@asynccontextmanager
async def configured_provider():
    mode = os.getenv("LLM_PROVIDER", "mock").strip().lower()
    if mode == "mock":
        yield MockProvider()
        return
    providers = {"openai": (OpenAIProvider, "OPENAI_API_KEY"),
                 "groq": (GroqProvider, "GROQ_API_KEY")}
    if mode not in providers:
        raise RuntimeError("LLM_PROVIDER must be mock, openai, or groq")
    provider_type, key_name = providers[mode]
    key = os.getenv(key_name, "").strip()
    model = os.getenv("LLM_MODEL", "").strip()
    if not key or not model:
        raise RuntimeError(f"{mode} mode requires {key_name} and LLM_MODEL")
    timeout = float(os.getenv("LLM_TIMEOUT_SECONDS", "8"))
    attempts = int(os.getenv("LLM_MAX_ATTEMPTS", "2"))
    if not math.isfinite(timeout) or not 0 < timeout <= 60 or not 1 <= attempts <= 3:
        raise RuntimeError("LLM_TIMEOUT_SECONDS must be in (0, 60] and LLM_MAX_ATTEMPTS in [1, 3]")
    async with httpx.AsyncClient(
        timeout=timeout, headers={"Authorization": f"Bearer {key}"}
    ) as client:
        yield provider_type(client, model=model, timeout=timeout, attempts=attempts)
