"use client";

import { useState, useRef, useEffect, FormEvent } from "react";
import MessageContent from "./MessageContent";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

interface Message {
  role: "user" | "assistant";
  content: string;
  error?: boolean;
}

interface SSEEvent {
  event: string;
  data: string;
}

function parseSSE(raw: string): SSEEvent[] {
  const events: SSEEvent[] = [];
  const blocks = raw.split("\n\n");
  for (const block of blocks) {
    const lines = block.split("\n");
    let event = "message";
    let data = "";
    for (const line of lines) {
      if (line.startsWith("event: ")) event = line.slice(7);
      if (line.startsWith("data: ")) data = line.slice(6);
    }
    if (data) events.push({ event, data });
  }
  return events;
}

const TOOL_LABELS: Record<string, string> = {
  search_ccc: "Searching Catechism",
  get_paragraph: "Fetching CCC paragraph",
  search_encyclopedia: "Searching Encyclopedia",
  get_encyclopedia_article: "Fetching article",
  search_patristic: "Searching Church Fathers",
  search_bible: "Searching Bible",
  get_citations: "Fetching citations",
};

function describeToolCall(name: string, input: Record<string, unknown>): string {
  const label = TOOL_LABELS[name] || name.replace(/_/g, " ");
  const query = input.query ?? input.id ?? input.article_id ?? input.paragraph_id;
  if (query !== undefined) return `${label}: "${String(query)}"`;
  return `${label}…`;
}

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [statusTrail, setStatusTrail] = useState<string[]>([]);
  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, statusTrail]);

  async function submit(e: FormEvent) {
    e.preventDefault();
    const text = input.trim();
    if (!text || loading) return;

    const userMessage: Message = { role: "user", content: text };
    const newMessages = [...messages, userMessage];
    setMessages(newMessages);
    setInput("");
    setLoading(true);
    setStatusTrail([]);

    let assistantText = "";
    setMessages((prev) => [...prev, { role: "assistant", content: "" }]);

    try {
      const res = await fetch(`${API_URL}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: newMessages }),
      });

      if (!res.ok || !res.body) {
        throw new Error(`HTTP ${res.status}`);
      }

      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });

        const events = parseSSE(buffer);
        const lastDouble = buffer.lastIndexOf("\n\n");
        if (lastDouble !== -1) buffer = buffer.slice(lastDouble + 2);

        for (const ev of events) {
          if (ev.event === "text") {
            assistantText += ev.data;
            setMessages((prev) => {
              const updated = [...prev];
              updated[updated.length - 1] = { role: "assistant", content: assistantText };
              return updated;
            });
          } else if (ev.event === "tool_call") {
            try {
              const tc = JSON.parse(ev.data);
              const line = describeToolCall(tc.name, tc.input || {});
              setStatusTrail((prev) => [...prev, line]);
            } catch {}
          } else if (ev.event === "done") {
            setStatusTrail([]);
          }
        }
      }
    } catch {
      setMessages((prev) => {
        const updated = [...prev];
        updated[updated.length - 1] = {
          role: "assistant",
          content:
            "Could not reach the API. Make sure the backend is running, then try again.",
          error: true,
        };
        return updated;
      });
      setInput(text);
    } finally {
      setLoading(false);
      setStatusTrail([]);
    }
  }

  return (
    <div className="flex h-screen flex-col bg-zinc-50 dark:bg-zinc-950">
      {/* Header */}
      <header className="border-b border-zinc-200 bg-white px-6 py-4 dark:border-zinc-800 dark:bg-zinc-900">
        <div className="mx-auto flex max-w-3xl items-center justify-between">
          <div>
            <h1 className="text-lg font-semibold text-zinc-900 dark:text-white">
              Catholic Knowledge Graph
            </h1>
            <p className="text-sm text-zinc-500 dark:text-zinc-400">
              CCC · Catholic Encyclopedia · Church Fathers · Bible
            </p>
          </div>
          <a
            href="/"
            className="text-sm text-zinc-500 hover:text-zinc-900 dark:hover:text-white"
          >
            ← Back
          </a>
        </div>
      </header>

      {/* Messages */}
      <main className="flex-1 overflow-y-auto px-4 py-6">
        <div className="mx-auto max-w-3xl space-y-6">
          {messages.length === 0 && (
            <div className="py-16 text-center text-zinc-400 dark:text-zinc-500">
              <p className="mb-2 text-xl">Ask anything about Catholic doctrine</p>
              <p className="text-sm">
                What does the CCC say about grace? Tell me about transubstantiation.
                Who was Augustine of Hippo?
              </p>
            </div>
          )}

          {messages.map((msg, i) => (
            <div
              key={i}
              className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
            >
              <div
                className={`max-w-[85%] rounded-2xl px-4 py-3 text-sm leading-relaxed ${
                  msg.role === "user"
                    ? "bg-zinc-900 text-white dark:bg-white dark:text-zinc-900"
                    : msg.error
                      ? "bg-red-50 text-red-900 ring-1 ring-red-200 dark:bg-red-950 dark:text-red-200 dark:ring-red-900"
                      : "bg-white text-zinc-800 shadow-sm ring-1 ring-zinc-200 dark:bg-zinc-800 dark:text-zinc-100 dark:ring-zinc-700"
                }`}
              >
                {msg.role === "user" ? (
                  <div className="whitespace-pre-wrap">{msg.content}</div>
                ) : msg.content ? (
                  <MessageContent content={msg.content} />
                ) : null}
              </div>
            </div>
          ))}

          {statusTrail.length > 0 && (
            <div className="flex justify-start">
              <div className="rounded-2xl bg-zinc-100 px-4 py-3 text-xs text-zinc-500 dark:bg-zinc-800 dark:text-zinc-400">
                {statusTrail.map((line, i) => {
                  const isLast = i === statusTrail.length - 1;
                  return (
                    <div key={i} className="flex items-center gap-2">
                      <span className={isLast ? "animate-pulse" : "opacity-60"}>●</span>
                      <span className={isLast ? "" : "opacity-60 line-through"}>{line}</span>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

          {loading && statusTrail.length === 0 && messages.at(-1)?.role === "assistant" && !messages.at(-1)?.content && (
            <div className="flex justify-start">
              <div className="flex items-center gap-2 rounded-2xl bg-zinc-100 px-4 py-3 text-xs text-zinc-500 dark:bg-zinc-800 dark:text-zinc-400">
                <span className="animate-pulse">●</span>
                Thinking…
              </div>
            </div>
          )}

          <div ref={bottomRef} />
        </div>
      </main>

      {/* Input */}
      <footer className="border-t border-zinc-200 bg-white px-4 py-4 dark:border-zinc-800 dark:bg-zinc-900">
        <form onSubmit={submit} className="mx-auto flex max-w-3xl gap-3">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask about Catholic doctrine, history, theology…"
            disabled={loading}
            className="flex-1 rounded-xl border border-zinc-200 bg-zinc-50 px-4 py-3 text-sm text-zinc-900 placeholder-zinc-400 focus:outline-none focus:ring-2 focus:ring-zinc-900 disabled:opacity-50 dark:border-zinc-700 dark:bg-zinc-800 dark:text-white dark:focus:ring-white"
          />
          <button
            type="submit"
            disabled={loading || !input.trim()}
            className="rounded-xl bg-zinc-900 px-5 py-3 text-sm font-medium text-white transition hover:bg-zinc-700 disabled:opacity-40 dark:bg-white dark:text-zinc-900 dark:hover:bg-zinc-200"
          >
            Send
          </button>
        </form>
      </footer>
    </div>
  );
}
