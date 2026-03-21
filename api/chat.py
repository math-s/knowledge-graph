"""Streaming /chat endpoint with Claude tool-use loop."""

from __future__ import annotations

import json
import os
from typing import AsyncIterator

import anthropic
from fastapi import APIRouter
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
    role: str  # "user" or "assistant"
    content: str


class ChatRequest(BaseModel):
    messages: list[Message]
    stream: bool = True


def _make_client() -> anthropic.Anthropic:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY environment variable not set")
    return anthropic.Anthropic(api_key=api_key)


async def _stream_chat(messages: list[dict]) -> AsyncIterator[str]:
    """Run the agentic tool-use loop, yielding SSE-formatted chunks."""
    client = _make_client()
    history = list(messages)

    while True:
        # Collect a full response (streaming per token but buffering tool calls)
        response_text = ""
        tool_uses: list[dict] = []
        stop_reason = None

        with client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=SYSTEM_PROMPT,
            tools=TOOLS,
            messages=history,
        ) as stream:
            for event in stream:
                if hasattr(event, "type"):
                    if event.type == "content_block_delta":
                        if hasattr(event.delta, "text"):
                            chunk = event.delta.text
                            response_text += chunk
                            yield _sse("text", chunk)
                    elif event.type == "content_block_start":
                        if hasattr(event.content_block, "type") and event.content_block.type == "tool_use":
                            tool_uses.append({
                                "id": event.content_block.id,
                                "name": event.content_block.name,
                                "input": "",
                            })
                    elif event.type == "content_block_stop":
                        pass

            # Get final message for stop_reason and complete tool inputs
            final = stream.get_final_message()
            stop_reason = final.stop_reason

            # Rebuild tool_uses with complete input from final message
            tool_uses = []
            for block in final.content:
                if block.type == "tool_use":
                    tool_uses.append({
                        "id": block.id,
                        "name": block.name,
                        "input": block.input,
                    })

        # Build assistant message for history
        assistant_content = []
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

        # If no tool calls or stop_reason is end_turn, we're done
        if stop_reason != "tool_use" or not tool_uses:
            yield _sse("done", "")
            return

        # Execute tools and add results
        tool_results = []
        for tu in tool_uses:
            yield _sse("tool_call", json.dumps({"name": tu["name"], "input": tu["input"]}))
            result = dispatch_to_content(tu["name"], tu["input"])
            yield _sse("tool_result", json.dumps({"name": tu["name"]}))
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tu["id"],
                "content": result,
            })

        history.append({"role": "user", "content": tool_results})
        # Loop back for Claude's next response


def _sse(event: str, data: str) -> str:
    return f"event: {event}\ndata: {data}\n\n"


@router.post("/chat")
async def chat(req: ChatRequest):
    messages = [{"role": m.role, "content": m.content} for m in req.messages]

    if not req.stream:
        # Non-streaming: collect everything and return
        chunks = []
        async for chunk in _stream_chat(messages):
            chunks.append(chunk)
        return {"response": "".join(chunks)}

    return StreamingResponse(
        _stream_chat(messages),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
