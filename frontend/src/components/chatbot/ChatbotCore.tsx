"use client";

import { useEffect, useRef, useState } from "react";
import ReactMarkdown from "react-markdown";
import { Message, useChatbotStore } from "@/lib/store/chatbotStore";
import WindowSendButton from "@/components/chatbot/WindowSendButton";
import './style.css';
import { Copy, Share2, ThumbsDown, ThumbsUp } from "lucide-react";
import { Fragment } from "react";




type ParsedWeek = {
  week: string
  actions: string[]
}


type ParsedDetail = {
  label: string
  value: string
}

type ParsedAI = {
  title: string
  period?: string
  details: ParsedDetail[]
  weeks: ParsedWeek[]
}

export default function ChatbotCore() {
  const scrollRef = useRef<HTMLDivElement>(null);

  const [actionMessage, setActionMessage] = useState<{ id: string; text: string } | null>(null);

  const {
    messages,
    loading,
    sendMessage,
    clearChat,
    loadFromStorage,
    reactToMessage,
    sendFeedback,
  } = useChatbotStore();

  

 

  useEffect(() => {
    scrollRef.current?.scrollTo({
      top: scrollRef.current.scrollHeight,
      behavior: "smooth",
    });
  }, [messages, loading]);

  const flash = (id: string, text: string) => {
    setActionMessage({ id, text });
    setTimeout(() => setActionMessage(null), 1200);
  };

  const handleCopy = async (msg: Message) => {
    try {
      await navigator.clipboard.writeText(msg.text || "");
      flash(msg.id, "Copied!");
    } catch {
      flash(msg.id, "Copy failed");
    }
  };

  const handleShare = async (msg: Message) => {
    try {
      if (navigator.share) {
        await navigator.share({ text: msg.text || "" });
        flash(msg.id, "Shared!");
      } else {
        await navigator.clipboard.writeText(msg.text || "");
        flash(msg.id, "Copied to share");
      }
    } catch {
      // user cancelled share or it failed
    }
  };

  const cleanMarkdown = (s = '') =>
  s
    .replace(/(^|\n)\s*[-*•]\s+/g, '$1')
    .replace(/\*\*(.*?)\*\*/g, '$1')
    .replace(/__(.*?)__/g, '$1')
    .replace(/\*(.*?)\*/g, '$1')
    .replace(/_(.*?)_/g, '$1')
    .replace(/`(.*?)`/g, '$1')
    .replace(/~~(.*?)~~/g, '$1')
    .replace(/\*{1,3}/g, '')
    .trim()

// ---- Timestamp helpers (WhatsApp-style) ----
const pad2 = (n: number) => String(n).padStart(2, "0");
const formatTime = (ts?: number) => {
  const d = new Date(ts || Date.now());
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
};
const dayKey = (ts?: number) => {
  const d = new Date(ts || Date.now());
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
};
const formatDayLabel = (ts?: number) => {
  const d = new Date(ts || Date.now());
  const now = new Date();
  const todayKey = dayKey(now.getTime());
  const y = new Date(now);
  y.setDate(now.getDate() - 1);
  const yKey = dayKey(y.getTime());
  const k = dayKey(d.getTime());
  if (k === todayKey) return "Today";
  if (k === yKey) return "Yesterday";
  return d.toLocaleDateString(undefined, { day: "2-digit", month: "short", year: "numeric" });
};

function parseAIResponse(rawText: string): ParsedAI {
  const result: ParsedAI = { title: '', details: [], weeks: [] }
  if (!rawText) return result

const text = rawText || ''
  const lines = text
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean)

  // Find an explicit markdown title if present: "### Title: ..."
  let explicitTitle = ''
  for (const line of lines) {
    const m = line.match(/^#{1,6}\s*Title:\s*(.+)$/i)
    if (m) {
      explicitTitle = m[1].trim()
      break
    }
  }

  // Fallback title: the first non "Key: Value" line
  let fallbackTitle = ''
  for (const line of lines) {
    if (!/^([^:]+):\s*(.+)$/.test(line) && !/^#{1,6}\s*Title:/i.test(line)) {
      fallbackTitle = line
      break
    }
  }

  result.title = explicitTitle || fallbackTitle || ''

  // Optional: try to detect a period range if present in the title line
  const periodMatch = result.title.match(/\(([^)]+)\)/)
  if (periodMatch) {
    result.period = periodMatch[1]
  }

  // Extract "Key: Value" pairs from bullet or normal lines
  for (const line of lines) {
    // Skip the explicit or fallback title line to avoid duplication
    if (
      result.title &&
      (line === result.title || line.replace(/^Title:\s*/i, '').trim() === result.title)
    ) {
      continue
    }
    const kv = line.match(/^\s*(?:[-*•]\s*)?([^:]+):\s*(.+)\s*$/)
    if (kv) {
      const label = kv[1].trim()
      const value = kv[2].trim()
      result.details.push({ label, value })
    }
  }

  // Optional: parse "Week N ..." sections
  const weekRegex = /(Week\s+\d+[^\n]*)([\s\S]*?)(?=Week\s+\d+|$)/gi
  let wk: RegExpExecArray | null
  while ((wk = weekRegex.exec(text)) !== null) {
    const weekTitle = wk[1].trim()
    const actions = wk[2]
      .split(/\r?\n/)
      .map((l) => cleanMarkdown(l).trim())
      .filter(Boolean)
    if (actions.length) result.weeks.push({ week: weekTitle, actions })
  }

  return result
}

  function convertPlainTextToMarkdown(text: string): string {
  const lines = text.split('\n');
  let out: string[] = [];

  let productIndex = 0;
  let collectingProductPoints = false;
  let productPoints: string[] = [];

  let inConsolidatedActions = false;
  let consolidatedPoints: string[] = [];

  const flushProductPoints = () => {
    if (productPoints.length > 0) {
      productPoints.forEach(p => out.push(`1. ${p}`)); // ordered list
      productPoints = [];
    }
  };

  for (let raw of lines) {
    const line = raw.trim();
    if (!line) continue;

    // ================= SUMMARY =================
    if (/^SUMMARY$/i.test(line)) {
      flushProductPoints();
      out.push('## **SUMMARY**');
      collectingProductPoints = false;
      inConsolidatedActions = false;
      continue;
    }

    // ================= ACTIONS =================
    if (/^ACTIONS$/i.test(line)) {
      flushProductPoints();
      out.push('\n## **ACTIONS**');
      collectingProductPoints = false;
      inConsolidatedActions = false;
      continue;
    }

    // ========== CONSOLIDATED ACTIONS ==========
    if (/^CONSOLIDATED ACTIONS$/i.test(line)) {
      flushProductPoints();
      out.push('\n## **CONSOLIDATED ACTIONS**');
      collectingProductPoints = false;
      inConsolidatedActions = true;
      continue;
    }

    // ================= PRODUCT NAME =================
    const productMatch = line.match(/^Product name\s*-\s*(.+)$/i);
    if (productMatch) {
      flushProductPoints();
      productIndex++;
      out.push(`\n### ${productIndex}. **Product name – ${productMatch[1].trim()}**`);
      collectingProductPoints = true;
      inConsolidatedActions = false;
      continue;
    }

    // ================= ACTION LINE =================
    if (/^(Review|Check)\b/i.test(line)) {
      flushProductPoints();
      out.push(`\n**Action: ${line}**\n`);
      collectingProductPoints = false;
      inConsolidatedActions = false;
      continue;
    }

    // ================= PRODUCT POINTS =================
    if (collectingProductPoints) {
      const sentences = line
        .split(/(?<=[.])\s+/)
        .map(s => s.trim())
        .filter(Boolean);

      productPoints.push(...sentences);
      continue;
    }

    // ========== CONSOLIDATED ACTION POINTS ==========
    if (inConsolidatedActions) {
      consolidatedPoints.push(line);
      continue;
    }

    // ================= SUMMARY BULLETS =================
    out.push(`- ${line}`);
  }

  // Flush remaining product points
  flushProductPoints();

  // Flush consolidated actions as ordered list
  if (consolidatedPoints.length > 0) {
    consolidatedPoints.forEach(p => out.push(`1. ${p}`));
  }

  return out.join('\n');
}

  return (
<div className="flex flex-col h-full overflow-hidden">
      {/* Messages */}
      <div
        ref={scrollRef}
        className="flex-1 min-h-0 overflow-y-auto px-3 py-4 space-y-3"
      >
        {messages.length === 0 && (
          <p className="text-center text-gray-400 text-sm mt-16">
            Start a conversation 💬
          </p>
        )}

        {Array.from(
  new Map(
    messages
      .filter(
        (msg): msg is Message =>
          !!msg && typeof msg === "object" && typeof msg.sender === "string"
      )
      .map((m) => [m.id, m])
  ).values()
).map((msg, idx, arr) => (
 <Fragment key={msg.id}>
    {(idx === 0 || dayKey(msg.timestamp) !== dayKey(arr[idx - 1]?.timestamp)) && (
      <div className="flex justify-center my-3">
  <span className="px-3 py-1 text-[11px] font-medium text-gray-500 bg-gray-200 rounded-full">
    {formatDayLabel(msg.timestamp)}
  </span>
</div>
      <div className="flex justify-center my-3">
  <span className="px-3 py-1 text-[11px] font-medium text-gray-500 bg-gray-200 rounded-full">
    {formatDayLabel(msg.timestamp)}
  </span>
</div>
    )}
    <div
    className={`flex ${
      msg.sender === "user" ? "justify-end" : "justify-start"
    }`}
  >
            <div className="flex flex-col max-w-[80%]">
              <div
             className={`px-4 py-2 rounded-2xl text-sm leading-snug
  ${
    msg.sender === "user"
      ? "bg-[#5EA68E] text-white rounded-br-md"
      : "bg-[#F1F1F1] text-gray-800 rounded-bl-md"
  }`}
            >
             {msg.sender !== "user" && msg.text ? (
  (() => {
    const parsed = parseAIResponse(msg.text)
    const isStructured =
      parsed.weeks.length > 0 ||
      /Title:/i.test(msg.text) ||
      /Week\s+\d+/i.test(msg.text)

    if (isStructured && (parsed.title || parsed.details.length > 0)) {
      return (
        <div className="space-y-1 markdown-body">
          {parsed.title && (
            <h3 className="font-semibold text-gray-800">{parsed.title}</h3>
          )}

          {parsed.weeks.length > 0 && (
            <div className="mt-2 space-y-2">
              {parsed.weeks.map((w, idx) => (
                <div key={idx}>
                  <h4 className="font-semibold text-sm text-gray-800">
                    {w.week}
                  </h4>
                  <ul className="list-disc pl-5 text-xs text-gray-700 space-y-1">
                    {w.actions.map((a, i) => (
                      <li key={i}>{a}</li>
                    ))}
                  </ul>
                </div>
              ))}
            </div>
          )}

          <ul className="space-y-2">
            {parsed.details.map((d, i) => (
              <li key={i} className="flex gap-2">
                <span className="font-bold">{d.label}:</span>
                <span>{d.value}</span>
              </li>
            ))}
          </ul>
        </div>
      )
    }

  

    return (
      <div className="markdown-body">
        <ReactMarkdown>
         {convertPlainTextToMarkdown(msg.text)}
        </ReactMarkdown>
      </div>
    )
  })()
) : (
  msg.text
)}

             <div
  className={`mt-1 text-[10px] leading-none ${
    msg.sender === "user"
      ? "text-[#F8EDCE] text-right"
      : "text-gray-500 text-right"
  }`}
>
  {formatTime(msg.timestamp)}
</div>
              </div>

              {msg.sender !== "user" && (
                <div className="flex items-center gap-1 mt-1 pl-1">
                  <button
                    type="button"
                    className={`chat-action-btn ${msg.liked === "like" ? "is-active" : ""}`}
                    onClick={() => {
                      const next = msg.liked === "like" ? undefined : "like";
                      reactToMessage(msg.id, next);
                      if (next) sendFeedback(msg.id, "like");
                    }}
                    aria-label="Like"
                    title="Like"
                  >
                    <ThumbsUp size={14} />
                  </button>

                  <button
                    type="button"
                    className={`chat-action-btn ${msg.liked === "dislike" ? "is-active" : ""}`}
                    onClick={() => {
                      const next = msg.liked === "dislike" ? undefined : "dislike";
                      reactToMessage(msg.id, next);
                      if (next) sendFeedback(msg.id, "dislike");
                    }}
                    aria-label="Dislike"
                    title="Dislike"
                  >
                    <ThumbsDown size={14} />
                  </button>

                  <button
                    type="button"
                   className="p-1 rounded-full text-gray-500 hover:bg-gray-200 hover:text-gray-700 transition"
                    onClick={() => handleCopy(msg)}
                    aria-label="Copy"
                    title="Copy"
                  >
                    <Copy size={14} />
                  </button>

                  <button
                    type="button"
className="p-1 rounded-full text-gray-500 hover:bg-gray-200 hover:text-gray-700 transition"
                    onClick={() => handleShare(msg)}
                    aria-label="Share"
                    title="Share"
                  >
                    <Share2 size={14} />
                  </button>

                  {actionMessage?.id === msg.id && (
                    <span className="ml-2 text-[10px] text-gray-500">
  {actionMessage.text}
</span>
                  )}
                </div>
              )}
            </div>
          </div>
  </Fragment>
        ))}

        {loading && (
          <p className="text-xs text-gray-400">Bot typing...</p>
        )}
      </div>

      {/* Input */}
      <div className="px-2 border-t border-gray-300 py-2 shrink-0">
        <div className="flex-1 flex items-center bg-[#D9D9D9] rounded-full px-3 py-2">
        <input
          placeholder="Ask me anything..."
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              sendMessage((e.target as HTMLInputElement).value);
              (e.target as HTMLInputElement).value = "";
            }
          }}
          className="flex-1 bg-transparent text-black caret-black outline-none text-xs sm:text-sm md:text-[0.75rem] lg:text-[0.875rem] h-full cursor-text"
        />
       <WindowSendButton
  onClick={() => {
    const input = document.querySelector<HTMLInputElement>(
      'input[placeholder="Ask me anything..."]'
    );
    if (input?.value) {
      sendMessage(input.value);
      input.value = "";
    }
  }}
  disabled={loading}
  
/>
        </div>
        
      </div>

      <p className="text-[10px] text-center text-gray-400 pb-2 shrink-0">
        AI-generated responses. Verify critical info.
      </p>
    </div>
  );
}
