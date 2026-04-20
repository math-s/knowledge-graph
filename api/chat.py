"""Streaming /chat endpoint with Claude tool-use loop."""

from __future__ import annotations

import asyncio
import json
import os
from typing import Any, AsyncIterator, Literal

import anthropic
from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from api.tools import TOOLS, dispatch_to_content

router = APIRouter()

SYSTEM_PROMPT = """You are a knowledgeable assistant specializing in Catholic theology, doctrine, and history.

You have access to several tools that let you query a local knowledge graph containing:
- The Catechism of the Catholic Church (CCC) — ~2,800 paragraphs
- The Catholic Encyclopedia (1907–1913) — ~11,600 articles
- Church Fathers texts (Augustine, Aquinas, Chrysostom, and more)
- Bible verses

When answering questions, use your tools to retrieve precise, sourced information. Always cite your sources
(CCC paragraph numbers, encyclopedia article titles, Church Father works, etc.).

Be thorough but concise. Prefer primary sources retrieved from the tools over general knowledge."""


class Message(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class ChatRequest(BaseModel):
    messages: list[Message]
    stream: bool = True


_client: anthropic.AsyncAnthropic | None = None


def _get_client() -> anthropic.AsyncAnthropic:
    global _client
    if _client is None:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY environment variable not set")
        _client = anthropic.AsyncAnthropic(api_key=api_key)
    return _client


async def _run_agent(messages: list[dict]) -> AsyncIterator[dict]:
    """Run the agentic tool-use loop, yielding structured events.

    Event shapes:
      {"type": "text", "text": str}                         -- streamed text delta
      {"type": "tool_call", "name": str, "input": dict}     -- model is calling a tool
      {"type": "tool_result", "name": str, "output": Any}   -- tool returned
      {"type": "done"}                                      -- end of conversation
    """
    client = _get_client()
    history = list(messages)

    while True:
        response_text = ""
        tool_uses: list[dict] = []

        try:
            async with client.messages.stream(
                model="claude-sonnet-4-6",
                max_tokens=4096,
                system=SYSTEM_PROMPT,
                tools=TOOLS,
                messages=history,
            ) as stream:
                async for event in stream:
                    if not hasattr(event, "type"):
                        continue
                    if event.type == "content_block_delta" and hasattr(event.delta, "text"):
                        chunk = event.delta.text
                        response_text += chunk
                        yield {"type": "text", "text": chunk}

                final = await stream.get_final_message()
                stop_reason = final.stop_reason

                tool_uses = [
                    {"id": b.id, "name": b.name, "input": b.input}
                    for b in final.content
                    if b.type == "tool_use"
                ]
        except anthropic.APIError as e:
            raise HTTPException(status_code=502, detail=str(e))

        assistant_content: list[dict] = []
        if response_text:
            assistant_content.append({"type": "text", "text": response_text})
        for tu in tool_uses:
            assistant_content.append({
                "type": "tool_use",
                "id": tu["id"],
                "name": tu["name"],
                "input": tu["input"],
            })
        history.append({"role": "assistant", "content": assistant_content})

        if stop_reason != "tool_use" or not tool_uses:
            yield {"type": "done"}
            return

        tool_results = []
        for tu in tool_uses:
            yield {"type": "tool_call", "name": tu["name"], "input": tu["input"]}
            result = await asyncio.to_thread(dispatch_to_content, tu["name"], tu["input"])
            yield {"type": "tool_result", "name": tu["name"]}
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu["id"],
                "content": result,
            })

        history.append({"role": "user", "content": tool_results})


def _sse(event: str, data: str) -> str:
    return f"event: {event}\ndata: {data}\n\n"


async def _sse_stream(messages: list[dict]) -> AsyncIterator[str]:
    async for ev in _run_agent(messages):
        kind = ev["type"]
        if kind == "text":
            yield _sse("text", ev["text"])
        elif kind == "tool_call":
            yield _sse("tool_call", json.dumps({"name": ev["name"], "input": ev["input"]}))
        elif kind == "tool_result":
            yield _sse("tool_result", json.dumps({"name": ev["name"]}))
        elif kind == "done":
            yield _sse("done", "")


async def _collect(messages: list[dict]) -> dict[str, Any]:
    text_parts: list[str] = []
    tool_calls: list[dict] = []
    async for ev in _run_agent(messages):
        if ev["type"] == "text":
            text_parts.append(ev["text"])
        elif ev["type"] == "tool_call":
            tool_calls.append({"name": ev["name"], "input": ev["input"]})
    return {"response": "".join(text_parts), "tool_calls": tool_calls}


@router.post("/chat")
async def chat(req: ChatRequest):
    messages = [{"role": m.role, "content": m.content} for m in req.messages]

    if not req.stream:
        return await _collect(messages)

    return StreamingResponse(
        _sse_stream(messages),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
