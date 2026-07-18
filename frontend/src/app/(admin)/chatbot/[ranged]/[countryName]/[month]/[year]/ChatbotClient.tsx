'use client'

import React, { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { useParams } from 'next/navigation'
import './style.css'
import Delete from '@/components/chatbot/Delete'
import RightArrow from '@/components/chatbot/RightArrow'
import { Copy, Share2, ThumbsDown, ThumbsUp } from 'lucide-react'
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
  suggestedQuestions?: string[]
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

type AssistantBlock =
  | { type: 'heading'; text: string; level: 2 | 3 }
  | { type: 'paragraph'; text: string }
  | { type: 'list'; ordered: boolean; items: string[] }
  | { type: 'table'; headers: string[]; rows: string[][] }

type ChargeGroupRow = {
  group: string
  amount: string
  rows: string
  dates: string
}

type DatedRawRow = {
  date: string
  description: string
  amount: string
}

// ---------- Helpers ----------



const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'http://127.0.0.1:5000'
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

const stripOuterMarkdown = (value: string) =>
  value
    .replace(/^#{1,6}\s*/, '')
    .replace(/^\*\*(.+)\*\*$/, '$1')
    .replace(/^__(.+)__$/, '$1')
    .trim()

const isTableSeparatorLine = (line: string) =>
  /^\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$/.test(line.trim())

const parsePipeRow = (line: string) =>
  line
    .trim()
    .replace(/^\|/, '')
    .replace(/\|$/, '')
    .split('|')
    .map((cell) => stripOuterMarkdown(cell.trim()))

const isPipeTableLine = (line: string) => {
  const trimmed = line.trim()
  return trimmed.includes('|') && trimmed.replace(/[^|]/g, '').length >= 2 && parsePipeRow(trimmed).length >= 2
}

const headingFromLine = (line: string): AssistantBlock | null => {
  const trimmed = line.trim()
  const markdownHeading = trimmed.match(/^(#{2,6})\s+(.+)$/)
  if (markdownHeading) {
    return {
      type: 'heading',
      level: markdownHeading[1].length <= 2 ? 2 : 3,
      text: stripOuterMarkdown(markdownHeading[2]),
    }
  }

  const boldOnly = trimmed.match(/^\*\*([^*]+?)\*\*:?$/)
  if (boldOnly) {
    return { type: 'heading', level: 3, text: stripOuterMarkdown(boldOnly[1].replace(/:$/, '')) }
  }

  const knownSection = trimmed.match(/^(Summary|Summary Table|Breakdown|Actions|Consolidated Actions|Recommendations?|Next Steps?|Key Metric to Improve|Data Limitations|Bottom Line|Analysis|Insights?):?$/i)
  if (knownSection) {
    return { type: 'heading', level: 3, text: stripOuterMarkdown(knownSection[1]) }
  }

  const shortLabel = trimmed.match(/^([A-Za-z][A-Za-z0-9 /&().,+-]{2,64}):$/)
  if (shortLabel) {
    return { type: 'heading', level: 3, text: stripOuterMarkdown(shortLabel[1]) }
  }

  return null
}

const listItemFromLine = (line: string): { ordered: boolean; text: string } | null => {
  const trimmed = line.trim()
  const bullet = trimmed.match(/^(?:[-*]|\u2022)\s+(.+)$/)
  if (bullet) return { ordered: false, text: bullet[1].trim() }

  const ordered = trimmed.match(/^(?:\d+[\.)]|[ivxlcdm]+[\.)])\s+(.+)$/i)
  if (ordered) return { ordered: true, text: ordered[1].trim() }

  return null
}

function parseAssistantBlocks(text: string): AssistantBlock[] {
  const lines = text.replace(/\r\n/g, '\n').split('\n')
  const blocks: AssistantBlock[] = []
  let i = 0

  while (i < lines.length) {
    const line = lines[i].trim()
    if (!line || line.startsWith('```')) {
      i += 1
      continue
    }

    if (isPipeTableLine(line)) {
      const tableLines: string[] = []
      let j = i
      while (j < lines.length && isPipeTableLine(lines[j])) {
        tableLines.push(lines[j].trim())
        j += 1
      }

      const hasSeparator = tableLines.some((tableLine) => isTableSeparatorLine(tableLine))
      const rows = tableLines
        .filter((tableLine) => !isTableSeparatorLine(tableLine))
        .map(parsePipeRow)
        .filter((row) => row.some(Boolean))

      if (rows.length > 0 && (hasSeparator || rows.length > 1)) {
        blocks.push({ type: 'table', headers: rows[0], rows: rows.slice(1) })
        i = j
        continue
      }
    }

    const heading = headingFromLine(line)
    if (heading) {
      blocks.push(heading)
      i += 1
      continue
    }

    const listItem = listItemFromLine(line)
    if (listItem) {
      const items: string[] = []
      const ordered = listItem.ordered
      let j = i
      while (j < lines.length) {
        const nextItem = listItemFromLine(lines[j])
        if (!nextItem || nextItem.ordered !== ordered) break
        items.push(nextItem.text)
        j += 1
      }
      blocks.push({ type: 'list', ordered, items })
      i = j
      continue
    }

    blocks.push({ type: 'paragraph', text: line })
    i += 1
  }

  return blocks
}

function renderInlineText(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*|__[^_]+__|`[^`]+`)/g).filter(Boolean)

  return parts.map((part, idx) => {
    if ((part.startsWith('**') && part.endsWith('**')) || (part.startsWith('__') && part.endsWith('__'))) {
      return <strong key={idx}>{part.slice(2, -2)}</strong>
    }
    if (part.startsWith('`') && part.endsWith('`')) {
      return <code key={idx}>{part.slice(1, -1)}</code>
    }
    return <React.Fragment key={idx}>{part}</React.Fragment>
  })
}

function renderParagraphText(text: string): React.ReactNode {
  const labelValue = text.match(/^(\*\*)?([A-Za-z][A-Za-z0-9 /&().,+-]{2,64}):(\*\*)?\s+(.+)$/)
  if (labelValue) {
    const label = stripOuterMarkdown(labelValue[2])
    if (/^Direct answer$/i.test(label)) {
      return (
        <span className="assistant-direct-answer">
          {renderInlineText(labelValue[4])}
        </span>
      )
    }

    if (/^Business impact$/i.test(label)) {
      return (
        <span className="assistant-impact-note">
          <strong>{label}:</strong> {renderInlineText(labelValue[4])}
        </span>
      )
    }

    return (
      <>
        <strong>{label}:</strong> {renderInlineText(labelValue[4])}
      </>
    )
  }

  return renderInlineText(text)
}

const stripInlineMarkdown = (value: string) =>
  value
    .replace(/\*\*(.*?)\*\*/g, '$1')
    .replace(/__(.*?)__/g, '$1')
    .replace(/`(.*?)`/g, '$1')
    .trim()

const amountPattern = String.raw`([+-]?(?:[$£€])?\d[\d,]*(?:\.\d+)?|[+-]?\d[\d,]*(?:\.\d+)?\s*(?:GBP|USD|EUR))`
const chargeGroupRegex = new RegExp(`^(.+?):\\s+${amountPattern}\\s+\\((\\d+)\\s+rows?,\\s+(.+)\\)$`, 'i')
const datedRawRowRegex = new RegExp(`^(\\d{4}-\\d{2}-\\d{2}):\\s+(.+?)\\s+${amountPattern}$`, 'i')

const parseChargeGroupRow = (item: string): ChargeGroupRow | null => {
  const cleaned = stripInlineMarkdown(item)
  const match = cleaned.match(chargeGroupRegex)
  if (!match) return null

  return {
    group: match[1].trim(),
    amount: match[2].trim(),
    rows: match[3].trim(),
    dates: match[4].trim(),
  }
}

const parseDatedRawRow = (item: string): DatedRawRow | null => {
  const cleaned = stripInlineMarkdown(item)
  const match = cleaned.match(datedRawRowRegex)
  if (!match) return null

  return {
    date: match[1].trim(),
    description: match[2].trim(),
    amount: match[3].trim(),
  }
}

const amountToneClass = (amount: string) =>
  amount.trim().startsWith('-') ? 'assistant-amount-negative' : 'assistant-amount-positive'

function renderSpecializedList(items: string[], key: React.Key): React.ReactNode | null {
  const chargeRows = items.map(parseChargeGroupRow)
  if (chargeRows.length > 0 && chargeRows.every(Boolean)) {
    return (
      <div key={key} className="assistant-table-wrap assistant-breakdown-wrap" role="region" aria-label="Charge group breakdown">
        <table className="assistant-table assistant-breakdown-table">
          <thead>
            <tr>
              <th>Charge group</th>
              <th>Amount</th>
              <th>Rows</th>
              <th>Date</th>
            </tr>
          </thead>
          <tbody>
            {chargeRows.map((row, rowIdx) => {
              const safeRow = row as ChargeGroupRow
              return (
                <tr key={rowIdx}>
                  <td className="assistant-description-cell">{renderInlineText(safeRow.group)}</td>
                  <td className={`assistant-number-cell ${amountToneClass(safeRow.amount)}`}>{safeRow.amount}</td>
                  <td className="assistant-rows-cell">{safeRow.rows}</td>
                  <td className="assistant-date-cell">{safeRow.dates}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    )
  }

  const datedRows = items.map(parseDatedRawRow)
  if (datedRows.length > 0 && datedRows.every(Boolean)) {
    return (
      <div key={key} className="assistant-table-wrap assistant-breakdown-wrap" role="region" aria-label="Largest dated rows">
        <table className="assistant-table assistant-breakdown-table">
          <thead>
            <tr>
              <th>Date</th>
              <th>Description</th>
              <th>Amount</th>
            </tr>
          </thead>
          <tbody>
            {datedRows.map((row, rowIdx) => {
              const safeRow = row as DatedRawRow
              return (
                <tr key={rowIdx}>
                  <td className="assistant-date-cell">{safeRow.date}</td>
                  <td className="assistant-description-cell">{renderInlineText(safeRow.description)}</td>
                  <td className={`assistant-number-cell ${amountToneClass(safeRow.amount)}`}>{safeRow.amount}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    )
  }

  return null
}

function renderAssistantResponse(text: string) {
  const blocks = parseAssistantBlocks(text)
  if (!blocks.length) return <span>{text}</span>

  const visibleBlocks = blocks.filter((block, idx) => {
    const previous = blocks[idx - 1]
    if (block.type === 'heading' && /^Largest dated rows$/i.test(block.text)) {
      return false
    }
    if (
      previous?.type === 'heading' &&
      /^Largest dated rows$/i.test(previous.text) &&
      block.type === 'list'
    ) {
      return false
    }
    return true
  })

  return (
    <div className="assistant-output">
      {visibleBlocks.map((block, idx) => {
        if (block.type === 'heading') {
          const Tag: 'h2' | 'h3' = block.level === 2 ? 'h2' : 'h3'
          return <Tag key={idx}>{renderInlineText(block.text)}</Tag>
        }

        if (block.type === 'list') {
          const specializedList = renderSpecializedList(block.items, idx)
          if (specializedList) return specializedList

          const ListTag: 'ol' | 'ul' = block.ordered ? 'ol' : 'ul'
          return (
            <ListTag key={idx}>
              {block.items.map((item, itemIdx) => (
                <li key={itemIdx}>{renderParagraphText(item)}</li>
              ))}
            </ListTag>
          )
        }

        if (block.type === 'table') {
          return (
            <div key={idx} className="assistant-table-wrap" role="region" aria-label="Response table">
              <table className="assistant-table">
                <thead>
                  <tr>
                    {block.headers.map((header, headerIdx) => (
                      <th key={headerIdx}>{renderInlineText(header)}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {block.rows.map((row, rowIdx) => (
                    <tr key={rowIdx}>
                      {block.headers.map((_, cellIdx) => (
                        <td key={cellIdx}>{renderInlineText(row[cellIdx] || '')}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )
        }

        return <p key={idx}>{renderParagraphText(block.text)}</p>
      })}
    </div>
  )
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
  const activeCountry = String(params?.countryName || '').trim().toLowerCase()

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
      } catch { }
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

  const handleSuggestedQuestion = (question: string) => {
    const cleaned = question.trim()
    if (!cleaned || isLoading) return
    setInputValue('')
    sendMessage(cleaned, { country: activeCountry, source: 'suggested' })
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

  function humanizeMetric(metric: string) {
    if (!metric) return ""

    const replacements: Record<string, string> = {

      // -------- COMMON --------
      acos: "ACOS",
      asp: "ASP",
      roi: "ROI",
      cpc: "CPC",
      ctr: "CTR",
      cvr: "CVR",
      roas: "ROAS",

      // -------- PROFITS --------
      cm2_profit: "CM2 Profit",
      cm2_profit_percentage: "CM2 Profit %",
      cm2_margins: "CM2 Margins",
      profit_percentage: "Profit %",
      unit_wise_profitability: "Unit-wise Profitability",

      // -------- TYPO FIXES --------
      rembursement_fee: "Reimbursement Fee",
      rembursment_vs_cm2_margins: "Reimbursement vs CM2 Margins",
      dealsvouchar_ads: "Deals Voucher Ads",
      tex_and_credits: "Tax and Credits",
      platformfeenew: "Platform Fee",

      // -------- ADS --------
      visible_ads: "Visible Ads",
      advertising_total: "Advertising Total",

      // -------- SALES --------
      net_sales: "Net Sales",
      gross_sales: "Gross Sales",
      refund_sales: "Refund Sales",
      product_sales: "Product Sales",

      // -------- FEES --------
      platform_fee_inventory_storage: "Platform Fee Inventory Storage",
      other_transaction_fees: "Other Transaction Fees",

      // -------- INVENTORY --------
      units_shipped_t30: "Units Shipped (30 Days)",
      units_shipped_t60: "Units Shipped (60 Days)",
      units_shipped_t90: "Units Shipped (90 Days)",
      days_of_supply: "Days of Supply",

      // -------- MISC --------
      sales_mix: "Sales Mix",
      profit_mix: "Profit Mix",
    }

    const lower = metric.toLowerCase()

    if (replacements[lower]) {
      return replacements[lower]
    }

    return lower
      .replace(/_/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase())
  }

  function renderJsonResponse(data: any) {

    // -------- GROWTH --------
    if (data.type === "growth_summary" || data.type === "trend") {
      return (
        <div className="space-y-2">
          <h3 className="font-semibold">{data.metric}</h3>

          {data.current && (
            <div>
              <b>Current:</b> {data.current.formatted}
            </div>
          )}

          {data.change && (
            <div>
              <b>Change:</b> {data.change.formatted}
            </div>
          )}

          {data.mom && (
            <div>
              <b>Month-on-Month:</b>
              <ul className="list-disc pl-4">
                {data.mom.map((m: any, i: number) => (
                  <li key={i}>
                    {m.period}: {m.formatted}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {data.top_growth?.length > 0 && (
            <div>
              <b>Top Growing:</b>
              <ul className="list-disc pl-4">
                {data.top_growth.map((p: any, i: number) => (
                  <li key={i}>
                    {p.product}: {p.formatted}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {data.top_decline?.length > 0 && (
            <div>
              <b>Top Declining:</b>
              <ul className="list-disc pl-4">
                {data.top_decline.map((p: any, i: number) => (
                  <li key={i}>
                    {p.product}: {p.formatted}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )
    }

    // -------- RANKING --------
    if (data.type === "ranking") {
      return (
        <div>
          <h3 className="font-semibold">{data.metric}</h3>
          <ol className="list-decimal pl-4">
            {data.items.map((item: any) => (
              <li key={item.rank}>
                {item.product} — {item.formatted}
              </li>
            ))}
          </ol>
        </div>
      )
    }

    // -------- SUMMARY --------
    if (data.type === "summary") {
      return (
        <div className="space-y-2">
          <h3 className="font-semibold">Summary</h3>

          {Object.entries(data.metrics || {}).map(([k, v]: any) => (
            <div key={k}>
              <b>{humanizeMetric(k)}:</b>{" "}
              {typeof v === "number"
                ? v.toLocaleString(undefined, {
                  maximumFractionDigits: 2,
                })
                : v}
            </div>
          ))}

          {data.top_products?.length > 0 && (
            <div>
              <b>Top Products:</b>
              <ul className="list-disc pl-4">
                {data.top_products.map((p: any, i: number) => (
                  <li key={i}>{p.product_name}</li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )
    }

    // -------- SINGLE VALUE --------
    if (data.type === "single_value") {
      return (
        <div>
          <b>{data.metric}:</b> {data.formatted}
        </div>
      )
    }

    // -------- FALLBACK --------
    return <div>{JSON.stringify(data)}</div>
  }


  return (
    <div className="flex flex-col bg-white chatbot-container chatbot-page-shell overflow-hidden">
      <div className="text-white bg-gradient-to-r from-[#5ea68e] to-[#37455f] rounded-t-xl message-header px-4 py-3 sm:px-5 lg:px-6 lg:py-3">
        <h1 className="text-base sm:text-lg font-bold leading-tight">
          Hi <i>{userData?.name || 'User'}!</i>
        </h1>
        <p className="text-[11px] sm:text-xs mt-1 leading-snug">
          I&apos;m your Analytics Assistant, here to help you understand your business data, generate insights, and make informed decisions. What would you like to explore today?
        </p>
      </div>

      <div className="flex-1 min-h-0 border border-black/25 rounded-b-xl chat-container flex flex-col overflow-hidden">
        {/* Chat messages container */}
        <div
          ref={scrollRef}
          className="w-full mx-auto flex-1 min-h-0 overflow-y-auto overscroll-contain p-1.5 lg:p-2"
        >
          {/* Bottom-anchoring wrapper: keeps content at the bottom until it overflows */}
          <div className="min-h-full flex flex-col justify-end space-y-2">
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
                      <div className={`flex flex-col mx-2 sm:mx-3 ${msg.sender === 'user' ? 'items-end' : 'items-start'}`}>
                        <div
                          className={`px-3 py-1.5 rounded-xl text-xs break-words ${
                            msg.sender === 'user'
                              ? 'bg-[#5EA68E] text-[#F8EDCE] mb-2 max-w-full sm:max-w-[50vw] md:max-w-[50vw]'
                              : 'assistant-bot-bubble bg-[#D9D9D9] text-gray-800 mb-1'
                          }`}
                        >
                          {msg.sender !== 'user' && msg.text ? (
                            (() => {
                              let parsedJson: any = null

                              try {
                                parsedJson = JSON.parse(msg.text)
                              } catch {
                                parsedJson = null
                              }

                              // -------- JSON MODE --------
                              if (parsedJson && parsedJson.type) {
                                return renderJsonResponse(parsedJson)
                              }

                              // -------- TEXT MODE --------
                              return renderAssistantResponse(msg.text)
                            })()
                          ) : (
                            msg.text
                          )}
                          <div className={`chat-msg-meta ${msg.sender === 'user' ? 'chat-msg-meta-user' : 'chat-msg-meta-bot'}`}>{formatTime(msg.timestamp)}</div>
                        </div>

                        {msg.sender !== 'user' && idx === arr.length - 1 && Array.isArray(msg.suggestedQuestions) && msg.suggestedQuestions.length > 0 && (
                          <div className="suggested-question-list" aria-label="Suggested follow-up questions">
                            {msg.suggestedQuestions.slice(0, 3).map((question, questionIdx) => (
                              <button
                                key={`${question}-${questionIdx}`}
                                type="button"
                                className="suggested-question-chip"
                                onClick={() => handleSuggestedQuestion(question)}
                                disabled={isLoading}
                              >
                                {question}
                              </button>
                            ))}
                          </div>
                        )}

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

        <div className="sticky bottom-0 bg-white border-t border-gray-200 px-2 py-1.5 flex flex-col gap-1.5">
          <div className="flex items-center gap-3">
            <div className="flex-1 min-h-[38px] flex items-center bg-[#D9D9D9] rounded-full px-3 py-1.5">
              <input
                type="text"
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault()
                    sendMessage(inputValue, { country: activeCountry, source: 'manual' })
                    setInputValue('')
                  }
                }}
                disabled={isLoading}
                autoFocus
                className="flex-1 bg-transparent text-black caret-black outline-none text-xs sm:text-sm md:text-[0.75rem] lg:text-[0.875rem] h-full cursor-text"
              />
              <RightArrow
                onClick={() => {
                  sendMessage(inputValue, { country: activeCountry, source: 'manual' })
                  setInputValue('')
                }}
                disabled={isLoading || !inputValue.trim()}
                className="cursor-pointer"
              />
            </div>
            <Delete className="cursor-pointer mr-2 mt-1" onClick={clearChat} />

          </div>
          <p className='2xl:text-xs text-[10px] flex justify-center items-center text-center text-gray-400'>Responses are AI-generated and may contain inaccuracies. Please verify critical information before use.</p>
        </div>
      </div>
    </div>
  )
}
