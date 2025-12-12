'use client'

import React, { useEffect, useLayoutEffect, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { useParams } from 'next/navigation'
import './style.css'
import Delete from '@/components/chatbot/Delete'
import RightArrow from '@/components/chatbot/RightArrow'
import { Dot } from 'lucide-react'

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

const HISTORY_KEY = 'chatbot_history'
const CHAT_CLEARED_FLAG = 'chatbot_cleared_flag'

const loadCache = (): Message[] => {
  if (typeof window === 'undefined') return []
  try {
    const clearedAt = localStorage.getItem(CHAT_CLEARED_FLAG)
    const history = localStorage.getItem(HISTORY_KEY)
    if (!history) return []
    const parsed: Message[] = JSON.parse(history)
    if (clearedAt) {
      const clearedTs = Number(clearedAt)
      return parsed.filter((msg) => msg.timestamp > clearedTs)
    }
    return parsed
  } catch {
    return []
  }
}

const saveCache = (msgs: Message[]) => {
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(msgs))
    localStorage.removeItem(CHAT_CLEARED_FLAG)
  } catch {}
}

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


function analyticsTextToMarkdown(raw = ''): string {
  const text = raw.replace(/\r\n/g, '\n').trim()
  if (!text) return ''

  // Normalize spaces but keep newlines
  const normalized = text
    .replace(/[ \t]+/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim()

  // --- Split SUMMARY / ACTIONS even if they're in same line ---
  const upper = normalized.toUpperCase()
  const sIdx = upper.indexOf('SUMMARY')
  const aIdx = upper.indexOf('ACTIONS')

  const summaryPart =
    sIdx >= 0 && aIdx > sIdx ? normalized.slice(sIdx + 'SUMMARY'.length, aIdx).trim() : ''
  const actionsPart =
    aIdx >= 0 ? normalized.slice(aIdx + 'ACTIONS'.length).trim() : normalized.trim()

  const splitSentences = (s: string) =>
    s
      .replace(/\s+/g, ' ')
      .split(/(?<=[.?!])\s+/)
      .map((x) => x.trim())
      .filter(Boolean)

  let md = ''

  // --- SUMMARY ---
  if (sIdx >= 0) {
    md += `## **SUMMARY**\n\n`
    const bullets = splitSentences(summaryPart)
    bullets.forEach((b) => {
      md += `- ${b}\n`
    })
    md += `\n---\n\n`
  }

  // --- ACTIONS ---
  md += `## **ACTIONS**\n\n`

  // Handle both:
  // 1) "Product name - Classic ..." (single line)
  // 2) Multi-line blocks
  const productChunks = actionsPart
    .split(/Product\s*name\s*[-:]\s*/i)
    .map((x) => x.trim())
    .filter(Boolean)

  // If "Product name -" not present, fallback to old newline-based logic (optional)
  if (productChunks.length === 0) return md.trim()

  let p = 0

  for (const chunk of productChunks) {
    p++

    // Product name = text before first known sentence starter
    const nameMatch = chunk.match(
      /^(.*?)(?=\s+(There is|The increase|The ASP|Sales mix|The sales mix)\b)/i
    )
    const productName = (nameMatch?.[1] || chunk.split('. ')[0] || chunk).trim()

    // Body = rest after productName
    const body = chunk.slice(productName.length).trim()
    const sentences = splitSentences(body)

    const metric1 = sentences[0] || ''
    const metric2 = sentences[1] || ''

    // Remaining sentences -> action (often "Review..." / "Check...")
    const rest = sentences.slice(2).join(' ').trim()
    const actionText = rest.replace(/^Actions?\s*[:\-]\s*/i, '').trim()

    // Outer numbered product
    md += `${p}. **${productName}**\n`

    // Inner points (2 only)
    if (metric1) md += `    1. ${metric1}\n`
    if (metric2) md += `    2. ${metric2}\n`

    // Actions bold (no numeral)
    if (actionText) md += `\n    **Actions: ${actionText}**\n\n`

    md += `\n`
  }

  return md.trim()
}





function parseAIResponse(rawText: string): ParsedAI {
  const result: ParsedAI = { title: '', details: [], weeks: [] }
  if (!rawText) return result

  const text = cleanMarkdown(rawText)
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

  const scrollRef = useRef<HTMLDivElement | null>(null)
  const [messages, setMessages] = useState<Message[]>([])
  const [inputValue, setInputValue] = useState('')
  const [isLoading, setIsLoading] = useState(false)
  const [isTyping, setIsTyping] = useState(false)
  const [userData, setUserData] = useState<any>(null)
  const [aliasOfInput, setAliasOfInput] = useState('')
  const [likeInProgress, setLikeInProgress] = useState<number | null>(null)
  const [dislikeInProgress, setDislikeInProgress] = useState<number | null>(null)
  const [dislikeInputFor, setDislikeInputFor] = useState<number | null>(null)
  const [actionMessage, setActionMessage] = useState<{ id: number; text: string } | null>(null)
  

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

  // Load chat history from localStorage
  useEffect(() => {
    const cached = loadCache()
    if (cached.length > 0) {
      setMessages(cached)
    }
  }, [])

  // Keep chat history cached
  useEffect(() => {
    if (messages.length > 0) {
      saveCache(messages)
    }
  }, [messages])

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
  }, [messages.length])

  // Create message objects
  const createMessage = (sender: Sender, text: string, extra?: Partial<Message>): Message => ({
    id: Date.now() + Math.random(),
    sender,
    text,
    timestamp: Date.now(),
    ...extra,
  })

  const userMsg = (text: string, extra?: Partial<Message>) => createMessage('user', text, extra)
  const botMsg = (text: string, extra?: Partial<Message>) => createMessage('bot', text, extra)

  const addMessage = (msg: Message) => {
    setMessages((prev) => [...prev, msg])
  }

  const clearChatLocally = () => {
    setMessages([])
    if (typeof window !== 'undefined') {
      try {
        localStorage.setItem(CHAT_CLEARED_FLAG, String(Date.now()))
        localStorage.removeItem(HISTORY_KEY)
      } catch {}
    }
  }

  const sendMessage = async (content?: string) => {
    const value = typeof content === 'string' ? content : inputValue.trim()
    if (!value || isLoading) return

    const ranged = params?.ranged || ''
    const countryName = params?.countryName || ''
    const month = params?.month || ''
    const year = params?.year || ''

    const paramsInfo = {
      range: ranged,
      countryName,
      month,
      year,
      user_company_id: userData?.company_id,
    }

    const userMessage = value
    const staffAlias = aliasOfInput.trim() || userData?.company_name || 'User'
    const contentWithAlias = `${userMessage} (Alias: ${staffAlias})`

    addMessage(userMsg(userMessage))
    setInputValue('')
    setIsLoading(true)
    setIsTyping(true)
    scrollToBottom(true)

    try {
      const resp = await fetch(`${API_BASE_URL}/chatbot`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${getAuthToken()}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ action: 'chat', query: contentWithAlias, params: paramsInfo }),
      })
      const data = await resp.json()
      if (resp.ok && (data?.success as boolean)) {
        addMessage(
          botMsg(String(data.response ?? ''), {
            serverId: data.message_id,
            promptText: contentWithAlias,
          })
        )
      } else {
        addMessage(botMsg('Error processing request', { error: true }))
      }
    } catch {
      addMessage(botMsg('Network error', { error: true }))
    } finally {
      setIsLoading(false)
      setIsTyping(false)
      scrollToBottom(true)
    }
  }

  const handleLike = async (msgObj: Message) => {
    if (!msgObj.serverId) return
    setLikeInProgress(msgObj.id)
    setActionMessage(null)

    try {
      const resp = await fetch(`${API_BASE_URL}/chatbot/feedback`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${getAuthToken()}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message_id: msgObj.serverId,
          feedback: 'like',
          original_prompt: msgObj.promptText,
        }),
      })

      if (!resp.ok) {
        setMessages((prev) => prev.map((m) => (m.id === msgObj.id ? { ...m, liked: 'like' } : m)))
        setActionMessage({ id: msgObj.id, text: 'Liked!' })
        setTimeout(() => setActionMessage(null), 1500)
      } else {
        setMessages((prev) => prev.map((m) => (m.id === msgObj.id ? { ...m, liked: 'like' } : m)))
        setActionMessage({ id: msgObj.id, text: 'Liked!' })
        setTimeout(() => setActionMessage(null), 1500)
      }
    } catch {
      setMessages((prev) => prev.map((m) => (m.id === msgObj.id ? { ...m, liked: 'like' } : m)))
      setActionMessage({ id: msgObj.id, text: 'Liked!' })
      setTimeout(() => setActionMessage(null), 1500)
    } finally {
      setLikeInProgress(null)
    }
  }

  const handleDislike = async (msgObj: Message) => {
    if (!msgObj.serverId) return
    setDislikeInProgress(msgObj.id)
    setActionMessage(null)
    setDislikeInputFor(msgObj.id)
  }

  const handleSaveDislike = async (feedbackText: string) => {
    const msgObj = messages.find((m) => m.id === dislikeInputFor)
    if (!msgObj || !msgObj.serverId) return

    try {
      const resp = await fetch(`${API_BASE_URL}/chatbot/feedback`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${getAuthToken()}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          message_id: msgObj.serverId,
          feedback: 'dislike',
          original_prompt: msgObj.promptText,
          additional_feedback: feedbackText,
        }),
      })

      if (!resp.ok) {
        setMessages((prev) => prev.map((m) => (m.id === msgObj.id ? { ...m, liked: 'dislike' } : m)))
        setActionMessage({ id: msgObj.id, text: 'Disliked!' })
        setTimeout(() => setActionMessage(null), 1500)
        setDislikeInputFor(null)
        setAliasOfInput('')
      } else {
        setMessages((prev) => prev.map((m) => (m.id === msgObj.id ? { ...m, liked: 'dislike' } : m)))
        setActionMessage({ id: msgObj.id, text: 'Disliked!' })
        setTimeout(() => setActionMessage(null), 1500)
        setDislikeInputFor(null)
        setAliasOfInput('')
      }
    } catch {
      setMessages((prev) => prev.map((m) => (m.id === msgObj.id ? { ...m, liked: 'dislike' } : m)))
      setActionMessage({ id: msgObj.id, text: 'Disliked!' })
      setTimeout(() => setActionMessage(null), 1500)
      setDislikeInputFor(null)
      setAliasOfInput('')
    }
  }

  const handleCancelDislike = () => {
    handleSaveDislike('User provided negative feedback without additional comments')
  }

  const clearChat = () => {
    clearChatLocally()
  }

  const getValidMessages = () =>
    messages.filter((msg) => msg && typeof msg === 'object' && msg.text && msg.sender)
  const validMessages = getValidMessages()

  if (!userData) {
    return <div className="p-4">Loading...</div>
  }

  return (
    <div className="flex flex-col bg-white font-[Lato] chatbot-container h-full ">
      <div className="text-white bg-gradient-to-r from-[#5ea68e] to-[#37455f] rounded-t-xl message-header py-[2vw] px-[2vw] md:py-[2vw] md:px-[3.5vw] lg:py-[1vw] lg:px-[1.25vw]">
        <h1 className="text-base sm:text-lg md:text-xl lg:text-[1.625rem] font-bold">
          Hi <i>{userData?.company_name?.split(' ')[0] || 'User'}!</i>
        </h1>
        <p className="text-xs sm:text-sm md:text-base lg:text-[1rem] mt-1">
        I'm your Analytics Assistant, here to help you understand your business data, generate insights, and make informed decisions. What would you like to explore today?
        </p>
      </div>

      <div className="flex-1 border border-black/25 rounded-b-xl chat-container flex flex-col">
        {/* Chat messages container */}
        <div ref={scrollRef} className="w-full mx-auto h-[63vh] sm:h-[65vh] lg:h-[60vh] 2xl:h-[65vh] overflow-y-auto p-3">
          {/* Bottom-anchoring wrapper: keeps content at the bottom until it overflows */}
          <div className="min-h-full flex flex-col justify-end space-y-3">
            {validMessages.length > 0 ? (
              <>
                {validMessages.map((msg) => (
                  <div
                    key={msg.id}
                    className={`flex ${msg.sender === 'user' ? 'justify-end' : 'justify-start'}`}
                  >
                    <div className="flex flex-col mx-4">
                      <div
                        className={`px-4 py-2 rounded-2xl text-xs sm:text-sm md:text-[0.75] lg:text-[0.875rem] break-words max-w-full sm:max-w-[50vw] md:max-w-[50vw] lg:max-w-full ${msg.sender === 'user' ? 'bg-[#5EA68E] text-[#F8EDCE] mb-2' : 'bg-[#D9D9D9] text-gray-800 mb-1'}`}
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
const formatted = analyticsTextToMarkdown(msg.text);

return (
  <div className="markdown-body">
    <ReactMarkdown>{formatted || msg.text}</ReactMarkdown>
  </div>
);

                            }
                          })()
                        ) : (
                          msg.text
                        )}
                      </div>

                      {msg.sender !== 'user' && (
                        <div className="flex flex-col ml-2 mb-2">
                          {/* action buttons placeholder */}
                        </div>
                      )}
                    </div>
                  </div>
                ))}

                {isTyping && (
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

        <div className="border-t border-gray-200 p-2 sm:p-3 md:p-4 flex flex-col gap-2">
          {dislikeInputFor && (
            <div className="mb-2 p-2 border border-red-300 rounded-md bg-red-50">
              <p className="text-xs sm:text-sm text-red-700 mb-1">
                Please share what went wrong so we can improve:
              </p>
              <textarea
                value={aliasOfInput}
                onChange={(e) => setAliasOfInput(e.target.value)}
                className="w-full border border-red-300 rounded-md p-1 text-xs sm:text-sm"
                rows={2}
              />
              <div className="flex justify-end gap-2 mt-1">
                <button
                  onClick={() => handleSaveDislike(aliasOfInput)}
                  className="px-2 py-1 text-xs sm:text-sm bg-red-600 text-white rounded-md"
                >
                  Submit
                </button>
                <button
                  onClick={handleCancelDislike}
                  className="px-2 py-1 text-xs sm:text-sm border border-gray-300 rounded-md"
                >
                  Skip
                </button>
              </div>
            </div>
          )}

          <div className="flex items-center gap-4">
            <div className="flex-1 flex items-center bg-[#D9D9D9] rounded-full px-3 py-3">
              <input
                type="text"
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault()
                    sendMessage()
                  }
                }}
                disabled={isLoading}
                autoFocus
                className="flex-1 bg-transparent text-black caret-black outline-none text-xs sm:text-sm md:text-[0.75rem] lg:text-[0.875rem] h-full cursor-text"
              />
              <RightArrow
                onClick={() => sendMessage()}
                disabled={isLoading || !inputValue.trim()}
                className="cursor-pointer"
              />
            </div>
            <Delete className="cursor-pointer mr-2 mt-1" onClick={clearChat} />
           
          </div>
         <p className='md:text-xs text-[10px] flex justify-center items-center text-center text-gray-400'>Responses are AI-generated and may contain inaccuracies.Please verify critical information before use.</p>
        </div>
      </div>
    </div>
  )
}
