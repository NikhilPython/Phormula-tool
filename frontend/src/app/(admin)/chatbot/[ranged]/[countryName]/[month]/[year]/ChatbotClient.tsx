'use client'

import React, { useEffect, useLayoutEffect, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { useParams } from 'next/navigation'
import './style.css'
import Delete from '@/components/chatbot/Delete'
import RightArrow from '@/components/chatbot/RightArrow'
import { Copy, Share2, ThumbsDown, ThumbsUp, Dot } from 'lucide-react'
import { useChatbotStore } from "@/lib/store/chatbotStore";

// ---------- Types ----------

type Sender = 'user' | 'bot'

type Message = {
  id: number
  sender: Sender
  text: string
  timestamp: number
  liked?: 'like' | 'dislike'
  serverId?: number
  promptText?: string
  error?: boolean
}

type ParsedDetail = {
  label: string
  value: string
}

type ParsedWeek = {
  week: string
  actions: string[]
}

type ParsedAI = {
  title: string
  period?: string
  details: ParsedDetail[]
  weeks: ParsedWeek[]
}

// ---------- Helpers ----------



const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://localhost:5000'
const getAuthToken = () => (typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null)

// Strip common Markdown artifacts (bullets, bold/italics/code/strikethrough)
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
const pad2 = (n: number) => String(n).padStart(2, '0')
const formatTime = (ts?: number) => {
  const d = new Date(ts || Date.now())
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}`
}
const dayKey = (ts?: number) => {
  const d = new Date(ts || Date.now())
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`
}
const formatDayLabel = (ts?: number) => {
  const d = new Date(ts || Date.now())
  const now = new Date()
  const todayKey = dayKey(now.getTime())
  const y = new Date(now)
  y.setDate(now.getDate() - 1)
  const yKey = dayKey(y.getTime())
  const k = dayKey(d.getTime())
  if (k === todayKey) return 'Today'
  if (k === yKey) return 'Yesterday'
  return d.toLocaleDateString(undefined, { day: '2-digit', month: 'short', year: 'numeric' })
}

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

// ---------- Page component ----------

export default function ChatbotPage() {
  // Read dynamic segments from the URL: /Chatbot/[ranged]/[countryName]/[month]/[year]
  const params = useParams<{
    ranged: string
    countryName: string
    month: string
    year: string
  }>()

  const {
  messages,
  loading: isLoading,
  sendMessage,
  clearChat,
  loadFromStorage,
  reactToMessage,
  sendFeedback,
} = useChatbotStore();

  const scrollRef = useRef<HTMLDivElement | null>(null)
  
  const [inputValue, setInputValue] = useState('')
  const [userData, setUserData] = useState<any>(null)
  const [likeInProgress, setLikeInProgress] = useState<number | null>(null)
  const [dislikeInProgress, setDislikeInProgress] = useState<number | null>(null)
  const [actionMessage, setActionMessage] =
  useState<{ id: string; text: string } | null>(null)

  useEffect(() => {
  loadFromStorage();
}, []);
  

  useEffect(() => {
  const fetchUserData = async () => {
    const token = localStorage.getItem('jwtToken')
    if (!token) return
    try {
      const res = await fetch(`${API_BASE_URL.replace(/\/$/, '')}/get_user_data`, {
        headers: { Authorization: `Bearer ${token}` },
      })
      const data = await res.json()
      setUserData(data)   // yahan data set ho jayega
    } catch (e) {
      console.error('Error fetching user data', e)
    }
  }

  fetchUserData()
}, [])

  // Load user data from localStorage
  useEffect(() => {
    if (typeof window === 'undefined') return
    const storedUserData = localStorage.getItem('userdata')
    if (storedUserData) {
      try {
        setUserData(JSON.parse(storedUserData))
      } catch {}
    }
  }, [])

  // Scroll to bottom whenever messages change
  const scrollToBottom = (smooth = false) => {
    if (!scrollRef.current) return
    if (smooth) {
      scrollRef.current.scrollTo({ top: scrollRef.current.scrollHeight, behavior: 'smooth' })
    } else {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight
    }
  }

  useLayoutEffect(() => {
  scrollToBottom()
}, [messages])

  const flash = (id: string, text: string) => {
    setActionMessage({ id, text })
    setTimeout(() => setActionMessage(null), 1200)
  }

  const handleCopy = async (msg: any) => {
    try {
      await navigator.clipboard.writeText(msg.text || '')
      flash(msg.id, 'Copied!')
    } catch {
      flash(msg.id, 'Copy failed')
    }
  }

  const handleShare = async (msg: any) => {
    try {
      if (navigator.share) {
        await navigator.share({ text: msg.text || '' })
        flash(msg.id, 'Shared!')
      } else {
        await navigator.clipboard.writeText(msg.text || '')
        flash(msg.id, 'Copied to share')
      }
    } catch {
      // user cancelled share or it failed
    }
  }

  // Create message objects

 

  

  // NOTE: Dislike should stay simple (no extra textbox/modal). We just send feedback.



  const getValidMessages = () =>
    messages.filter((msg) => msg && typeof msg === 'object' && msg.text && msg.sender)
  const validMessages = getValidMessages()

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





  if (!userData) {
    return <div className="p-4">Loading...</div>
  }

  return (
    <div className="flex flex-col bg-white font-[Lato] chatbot-container h-full ">
      <div className="text-white bg-gradient-to-r from-[#5ea68e] to-[#37455f] rounded-t-xl message-header py-[2vw] px-[2vw] md:py-[2vw] md:px-[3.5vw] lg:py-[1vw] lg:px-[1.25vw]">
        <h1 className="2xl:text-2xl text-[18px] font-bold">
          Hi <i>{userData?.company_name?.split(' ')[0] || 'User'}!</i>
        </h1>
        <p style={{ fontFamily: "Lato, sans-serif" }} className="2xl:text-sm text-xs  mt-1 ">
        I&apos;m your Analytics Assistant, here to help you understand your business data, generate insights, and make informed decisions. What would you like to explore today?
        </p>
      </div>

      <div className="flex-1 border border-black/25 rounded-b-xl chat-container flex flex-col">
        {/* Chat messages container */}
        <div ref={scrollRef} className="w-full mx-auto h-[63vh] sm:h-[65vh] lg:h-[60vh] 2xl:h-[65vh] overflow-y-auto 2xl:p-3 p-1">
          {/* Bottom-anchoring wrapper: keeps content at the bottom until it overflows */}
          <div className="min-h-full flex flex-col justify-end space-y-3">
            {validMessages.length > 0 ? (
              <>
                {Array.from(
  new Map(validMessages.map((m) => [m.id, m])).values()
).map((msg, idx, arr) => (
  <React.Fragment key={msg.id}>
    {(idx === 0 || dayKey(msg.timestamp) !== dayKey(arr[idx - 1]?.timestamp)) && (
      <div className="chat-date-separator"><span>{formatDayLabel(msg.timestamp)}</span></div>
    )}
    <div
    key={msg.id}
    className={`flex ${msg.sender === 'user' ? 'justify-end' : 'justify-start'}`}
  >
                    <div className="flex flex-col mx-4">
                      <div
                        className={`px-4 2xl:py-2 py-1 rounded-2xl text-xs sm:text-sm md:text-[0.75] lg:text-[0.875rem] break-words max-w-full sm:max-w-[50vw] md:max-w-[50vw] lg:max-w-full ${msg.sender === 'user' ? 'bg-[#5EA68E] text-[#F8EDCE] mb-2' : 'bg-[#D9D9D9] text-gray-800 mb-1'}`}
                      >
                        {msg.sender !== 'user' && msg.text ? (
                          (() => {
                            const parsed = parseAIResponse(msg.text)
                            const isStructured =
                              parsed.weeks.length > 0 ||
                              /Title:/i.test(msg.text) ||
                              /Week\s+\d+/i.test(msg.text)

                            if (isStructured && (parsed.title || parsed.details.length > 0)) {
                              return (
                                <div className="space-y-1">
                                  {parsed.title && (
                                    <h3 className="font-semibold text-gray-800">{parsed.title}</h3>
                                  )}
                                  {parsed.period && (
                                    <p className="text-sm text-gray-500">{parsed.period}</p>
                                  )}
                                  {parsed.weeks.length > 0 && (
                                    <div className="mt-2 space-y-2">
                                      {parsed.weeks.map((w, idx) => (
                                        <div key={idx} className="mt-2">
                                          <h4 className="font-semibold text-sm text-gray-800">
                                            {w.week}
                                          </h4>
                                          <ul className="list-disc pl-5 text-xs sm:text-sm text-gray-700 space-y-1">
                                            {w.actions.map((action, i) => (
                                              <li key={i}>{action}</li>
                                            ))}
                                          </ul>
                                        </div>
                                      ))}
                                    </div>
                                  )}

                                  <ul className="space-y-2">
                                    {parsed.details.map((d, i) => (
                                      <li
                                        key={i}
                                        className="flex items-start gap-2 leading-relaxed"
                                      >
                                        <Dot className="mt-1 shrink-0" size={18} />
                                        <div className="text-xs sm:text-sm">
                                          <span className="font-bold text-gray-900">
                                            {d.label}:
                                          </span>{' '}
                                          {d.value.split('\n').map((line, idx) => (
                                            <span key={idx}>
                                              {idx > 0 && <br />}
                                              <span className="text-gray-700">{line}</span>
                                            </span>
                                          ))}
                                        </div>
                                      </li>
                                    ))}
                                  </ul>
                                </div>
                              )
                            } else {
                              // Default: render as Markdown


return (
  <div className="markdown-body">
   <div className="markdown-body">
  <ReactMarkdown>
    {convertPlainTextToMarkdown(msg.text)}
  </ReactMarkdown>
</div>
  </div>
);

                            }
                          })()
                        ) : (
                          msg.text
                        )}
                      <div className={`chat-msg-meta ${msg.sender === 'user' ? 'chat-msg-meta-user' : 'chat-msg-meta-bot'}`}>{formatTime(msg.timestamp)}</div>
                      </div>

                      {msg.sender !== 'user' && (
                        <div className="chat-msg-actions ml-2 mb-2">
                          <button
                            type="button"
                            className={`chat-action-btn ${msg.liked === 'like' ? 'is-active' : ''}`}
                            onClick={() => {
                              const next = msg.liked === 'like' ? undefined : 'like'
                              reactToMessage(msg.id, next)
                              if (next) {
                                sendFeedback(msg.id, 'like')
                                flash(msg.id, 'Liked!')
                              }
                            }}
                            aria-label="Like"
                            title="Like"
                          >
                            <ThumbsUp size={16} />
                          </button>

                          <button
                            type="button"
                            className={`chat-action-btn ${msg.liked === 'dislike' ? 'is-active' : ''}`}
                            onClick={() => {
                              const next = msg.liked === 'dislike' ? undefined : 'dislike'
                              reactToMessage(msg.id, next)
                              if (next) {
                                sendFeedback(msg.id, 'dislike')
                                flash(msg.id, 'Disliked!')
                              }
                            }}
                            aria-label="Dislike"
                            title="Dislike"
                          >
                            <ThumbsDown size={16} />
                          </button>

                          <button
                            type="button"
                            className="chat-action-btn"
                            onClick={() => handleCopy(msg)}
                            aria-label="Copy"
                            title="Copy"
                          >
                            <Copy size={16} />
                          </button>

                          <button
                            type="button"
                            className="chat-action-btn"
                            onClick={() => handleShare(msg)}
                            aria-label="Share"
                            title="Share"
                          >
                            <Share2 size={16} />
                          </button>

                          {actionMessage?.id === msg.id && (
                            <span className="chat-action-toast">{actionMessage.text}</span>
                          )}
                        </div>
                      )}
                    </div>
                  </div>
  </React.Fragment>
                ))}

                {isLoading && (
                  <div className="flex justify-start">
                    <div className="flex items-center px-4 py-2 rounded-2xl text-[#D9D9D9] rounded-bl-none">
                      {[0, 1, 2, 3, 4].map((i) => (
                        <span
                          key={i}
                          className="inline-block w-1 h-1 sm:w-1.5 sm:h-1.5 md:w-2 md:h-2 rounded-full bg-gray-400 mr-1 last:mr-0 animate-pulse"
                          style={{ animationDelay: `${i * 0.2}s` }}
                        />
                      ))}
                    </div>
                  </div>
                )}
              </>
            ) : (
              <div className="flex flex-1 justify-center items-center text-gray-500 text-center text-xs sm:text-base md:text-lg lg:text-xl px-2 sm:px-4 md:px-6 lg:px-8">
                Start a new conversation 💬
              </div>
            )}
          </div>
        </div>

        {/* Input area */}

        <div className="border-t border-gray-200 p-2 sm:p-2 2xl:p-4 flex flex-col gap-2">
          <div className="flex items-center gap-4">
            <div className="flex-1 flex items-center bg-[#D9D9D9] rounded-full px-3 2xl:py-3 py-2">
              <input
                type="text"
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                onKeyDown={(e) => {
  if (e.key === 'Enter' && !e.shiftKey) {
    e.preventDefault()
    sendMessage(inputValue)
    setInputValue('')
  }
}}
                disabled={isLoading}
                autoFocus
                className="flex-1 bg-transparent text-black caret-black outline-none text-xs sm:text-sm md:text-[0.75rem] lg:text-[0.875rem] h-full cursor-text"
              />
              <RightArrow
                 onClick={() => {
    sendMessage(inputValue)
    setInputValue('')
  }}
                disabled={isLoading || !inputValue.trim()}
                className="cursor-pointer"
              />
            </div>
            <Delete className="cursor-pointer mr-2 mt-1" onClick={clearChat} />
           
          </div>
         <p className='2xl:text-xs text-[10px] flex justify-center items-center text-center text-gray-400'>Responses are AI-generated and may contain inaccuracies.Please verify critical information before use.</p>
        </div>
      </div>
    </div>
  )
}
