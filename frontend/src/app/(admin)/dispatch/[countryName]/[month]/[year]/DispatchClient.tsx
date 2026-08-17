'use client'

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { useParams, useRouter } from 'next/navigation'
import * as XLSX from 'xlsx'
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css'
import { Modal } from '@/components/ui/modal'
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm'
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable'
import { Calendar } from 'react-date-range'
import 'react-date-range/dist/styles.css'
import 'react-date-range/dist/theme/default.css'
import GroupedCollapsibleTable, {
  type ColGroup,
  type LeafCol,
} from '@/components/ui/table/GroupedCollapsibleTable'
import DataTable, { type ColumnDef, type Row } from '@/components/ui/table/DataTable'
import SegmentedToggle from '@/components/ui/SegmentedToggle'
import Button from '@/components/ui/button/Button'
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import PageBreadcrumb from '@/components/common/PageBreadCrumb'
import Loader from '@/components/loader/Loader'
import { exportDispatchExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { useGetUserDataQuery } from '@/lib/api/profileApi'
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";
import { CalendarDays } from 'lucide-react'

interface SkuRow {
  [key: string]: string | number | undefined
  'S. No.'?: string | number
  SKU?: string
  'Product Name'?: string
  'Inventory at Month End'?: string | number
  'total_onhand_quantity'?: string | number
  'FBA'?: string | number
  'AWD'?: string | number
  'In Transit FBA'?: string | number
  'In Transit AWD'?: string | number
  'In stock'?: string | number
  'In transit'?: string | number
  'Projected Sales Total'?: string | number
  'Inventory Coverage Ratio Before Dispatch'?: string | number
  'Shortfall Unit'?: string | number
  'To be Dispatch'?: string | number
  'SEA'?: string | number
  'AIR'?: string | number
}

type DispatchTableRow = Record<string, React.ReactNode> & {
  __isTotal?: boolean
  __isOthers?: boolean
}

type DispatchPageProps = {
  embedded?: boolean
  countryNameProp?: string
  selectedMonthProp?: string
  selectedYearProp?: string
  showAllRowsProp?: boolean
  onShowAllRowsChange?: React.Dispatch<React.SetStateAction<boolean>>
  shipmentDetailsRequestKey?: number
  onProductNameClick?: (productName: string, sku?: string) => void

  popupContainer?: HTMLElement | null
}

type AwdDispatchInputRow = {
  shipment_id: string
  shipment_status?: string | null
  sku?: string | null
  asin?: string | null
  expected_unit_quantity?: number
  created_at?: string | null
  updated_at?: string | null
  ship_by?: string | null
  dispatch_date?: string | null
  shipment_type?: string | null
  expected_reach_date?: string | null
  sku_quantities?: Array<{
    sku?: string | null
    expected_unit_quantity?: number | string | null
  }> | null
}

type FbaDispatchInputRow = {
  shipment_id: string
  shipment_status?: string | null
  sku?: string | null
  fnsku?: string | null
  asin?: string | null
  quantity?: number
  created_at?: string | null
  updated_at?: string | null
  dispatch_date?: string | null
  shipment_type?: string | null
  expected_reach_date?: string | null
  name?: string | null
}

type InboundDispatchInputRow = {
  source: 'AWD' | 'FBA'
  shipment_id: string
  display_shipment_id: string
  shipment_status?: string | null
  sku?: string | null
  units?: number
  created_at?: string | null
  updated_at?: string | null
  dispatch_date?: string | null
  shipment_type?: string | null
  expected_reach_date?: string | null
}

type ShipmentDetailsDisplayMode = 'inline' | 'modal'
type ShipmentType = 'SEA' | 'AIR'
type ShipmentTransitWeeks = Record<ShipmentType, number | null>

type InboundShipmentTableRow = Row & {
  source: InboundDispatchInputRow['source']
  shipment_id: string
  rowIndex: number
  serialNo: number
  createdAtRaw: string
  shipmentId: string
  status: string
  sku: string
  units: number
  createdAt: string
  updatedAt: string
  dispatchDate: string
  shipmentType: string
  expectedReachDate: string
}

const COUNTRY_TO_MARKETPLACE: Record<string, string> = {
  uk: 'A1F83G8C2ARO7P',
  gb: 'A1F83G8C2ARO7P',
  us: 'ATVPDKIKX0DER',
  usa: 'ATVPDKIKX0DER',
}

const AWD_SUPPORTED_COUNTRIES = new Set(['us', 'usa'])
const SHIPMENT_DETAILS_CANCELLED_MESSAGE = 'Shipment details editing was cancelled.'
const SHIPMENT_DETAILS_REQUIRED_MESSAGE =
  'Please fill dispatch date, shipment type, and expected reach date for every shipment before opening Dispatch.'
const SHIPMENT_TYPE_OPTIONS: Array<{ value: ShipmentType; label: string }> = [
  { value: 'SEA', label: 'Sea' },
  { value: 'AIR', label: 'Air' },
]
const EMPTY_SHIPMENT_TRANSIT_WEEKS: ShipmentTransitWeeks = {
  SEA: null,
  AIR: null,
}

const normalizeCountryKey = (country: string) => country.trim().toLowerCase()

const isAwdSupportedCountry = (country: string) =>
  AWD_SUPPORTED_COUNTRIES.has(normalizeCountryKey(country))

function isShipmentDetailsRequiredError(message: string) {
  const normalized = message.toLowerCase()

  return (
    normalized.includes('expected reach date is required') ||
    normalized.includes('dispatch date is required') ||
    normalized.includes('shipment type is required') ||
    (
      normalized.includes('shipment') &&
      normalized.includes('required') &&
      normalized.includes('dispatch')
    )
  )
}

const monthNames = [
  'January',
  'February',
  'March',
  'April',
  'May',
  'June',
  'July',
  'August',
  'September',
  'October',
  'November',
  'December',
] as const

const DISPLAYED_COLUMNS = [
  'S. No.',
  'Product Name',
  'SKU',
  'FBA',
  'AWD',
  'In stock',
  'In Transit FBA',
  'In Transit AWD',
  'In transit',
  'Inventory Coverage Ratio Before Dispatch',
  'Projected Sales Total',
  'Shortfall Unit',
  'SEA',
  'AIR',
  'To be Dispatch',
] as const

const NUMERIC_COLUMNS = [
  'Inventory at Month End',
  'total_onhand_quantity',
  'FBA',
  'AWD',
  'In Transit FBA',
  'In Transit AWD',
  'In stock',
  'In transit',
  'Projected Sales Total',
  'Inventory Coverage Ratio Before Dispatch',
  'Shortfall Unit',
  'To be Dispatch',
  'SEA',
  'AIR',
] as const

function capitalize(str: string) {
  if (!str) return ''
  return str.charAt(0).toUpperCase() + str.slice(1).toLowerCase()
}

function getCurrentMonthPlus1() {
  const now = new Date()
  const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1)
  return monthNames[nextMonth.getMonth()]
}

function getCurrentYear() {
  const now = new Date()
  const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1)
  return String(nextMonth.getFullYear())
}

function normalizeHeader(header: unknown): string {
  const cleaned = String(header ?? '')
    .replace(/\u00A0/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase()

  const headerMap: Record<string, string> = {
    sno: 'S. No.',
    'sno.': 'S. No.',
    's. no': 'S. No.',
    's. no.': 'S. No.',
    sku: 'SKU',
    'product name': 'Product Name',
    'inventory at month end': 'Inventory at Month End',
    'total onhand quantity': 'total_onhand_quantity',
    'total_onhand_quantity': 'total_onhand_quantity',
    fba: 'FBA',
    awd: 'AWD',
    'fba.1': 'In Transit FBA',
    'awd.1': 'In Transit AWD',
    'in transit fba': 'In Transit FBA',
    'in transit awd': 'In Transit AWD',
    'in stock': 'In stock',
    'in transit': 'In transit',
    'inventory coverage ratio before dispatch': 'Inventory Coverage Ratio Before Dispatch',
    'projected sales total': 'Projected Sales Total',
    'shortfall unit': 'Shortfall Unit',
    'to be dispatch': 'To be Dispatch',
    sea: 'SEA',
    air: 'AIR',
  }

  return headerMap[cleaned] || String(header ?? '').trim()
}

function parseCellValue(header: string, value: unknown): string | number {
  if (value === null || value === undefined || value === '') return ''

  if (NUMERIC_COLUMNS.includes(header as (typeof NUMERIC_COLUMNS)[number])) {
    if (typeof value === 'number') return value

    const cleaned = String(value).replace(/,/g, '').trim()
    const num = Number(cleaned)
    return Number.isFinite(num) ? num : ''
  }

  return String(value).trim()
}

function findHeaderRowIndex(rows: any[][]): number {
  return rows.findIndex((row) => {
    const normalized = (row || []).map((cell) => normalizeHeader(cell))

    return (
      normalized.includes('Product Name') &&
      (
        normalized.includes('Dispatch') ||
        normalized.includes('Inventory at Month End') ||
        normalized.includes('SKU')
      )
    )
  })
}

function isMeaningfulRow(row: SkuRow): boolean {
  const productName = String(row['Product Name'] ?? '').trim()
  const sku = String(row['SKU'] ?? '').trim()
  const inventoryAtMonthEnd = row['Inventory at Month End']
  const totalOnhandQuantity = row['total_onhand_quantity']
  const fba = row['FBA']
  const awd = row['AWD']
  const inTransitFba = row['In Transit FBA']
  const inTransitAwd = row['In Transit AWD']
  const inStock = row['In stock']
  const inTransit = row['In transit']
  const projectedSalesTotal = row['Projected Sales Total']
  const coverageRatio = row['Inventory Coverage Ratio Before Dispatch']
  const shortfallUnit = row['Shortfall Unit']
  const toBeDispatch = row['To be Dispatch']
  const sea = row['SEA']
  const air = row['AIR']

  return Boolean(
    productName ||
    sku ||
    (inventoryAtMonthEnd !== '' && inventoryAtMonthEnd !== undefined) ||
    (totalOnhandQuantity !== '' && totalOnhandQuantity !== undefined) ||
    (fba !== '' && fba !== undefined) ||
    (awd !== '' && awd !== undefined) ||
    (inTransitFba !== '' && inTransitFba !== undefined) ||
    (inTransitAwd !== '' && inTransitAwd !== undefined) ||
    (inStock !== '' && inStock !== undefined) ||
    (inTransit !== '' && inTransit !== undefined) ||
    (projectedSalesTotal !== '' && projectedSalesTotal !== undefined) ||
    (coverageRatio !== '' && coverageRatio !== undefined) ||
    (shortfallUnit !== '' && shortfallUnit !== undefined) ||
    (toBeDispatch !== '' && toBeDispatch !== undefined) ||
    (sea !== '' && sea !== undefined) ||
    (air !== '' && air !== undefined)
  )
}


function toNumber(value: unknown): number {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0
  const num = Number(String(value ?? '').replace(/,/g, '').trim())
  return Number.isFinite(num) ? num : 0
}

function getSelectedMonthBounds(monthValue: string, yearValue: string) {
  const monthIndex = monthNames.findIndex(
    (monthName) => monthName.toLowerCase() === String(monthValue || '').trim().toLowerCase()
  )
  const yearNumber = Number(yearValue)
  if (monthIndex < 0 || !Number.isFinite(yearNumber)) return null

  return {
    start: new Date(yearNumber, monthIndex, 1),
    end: new Date(yearNumber, monthIndex + 1, 0),
  }
}

function isReachDateActiveForSelectedMonth(
  value: string | null | undefined,
  monthValue: string,
  yearValue: string
) {
  if (!value) return true
  const bounds = getSelectedMonthBounds(monthValue, yearValue)
  if (!bounds) return true
  const parsed = new Date(`${String(value).slice(0, 10)}T00:00:00`)
  if (Number.isNaN(parsed.getTime())) return true
  return parsed >= bounds.start && parsed <= bounds.end
}

function formatAwdDate(value?: string | null): string {
  if (!value) return ''
  return String(value).slice(0, 10)
}

function formatReadableDate(value?: string | null): string {
  const parsed = parseIsoDate(value)
  if (!parsed) return ''

  const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
  return `${parsed.getDate()} ${monthNames[parsed.getMonth()]} ${parsed.getFullYear()}`
}

function splitSkuList(value?: string | null): string[] {
  return String(value ?? '')
    .split(',')
    .map((sku) => sku.trim().toUpperCase())
    .filter(Boolean)
}

function applyAwdShipmentDetails(
  rows: SkuRow[],
  awdRows: AwdDispatchInputRow[],
  monthValue: string,
  yearValue: string
): SkuRow[] {
  void awdRows
  void monthValue
  void yearValue
  return rows
}

function buildInboundDispatchRows(
  awdRows: AwdDispatchInputRow[],
  fbaRows: FbaDispatchInputRow[]
): InboundDispatchInputRow[] {
  return [
    ...awdRows.map((row) => ({
      source: 'AWD' as const,
      shipment_id: row.shipment_id,
      display_shipment_id: `AWD-${row.shipment_id}`,
      shipment_status: row.shipment_status,
      sku: row.sku,
      units: toNumber(row.expected_unit_quantity),
      created_at: row.created_at,
      updated_at: row.updated_at,
      dispatch_date: row.dispatch_date || '',
      shipment_type: String(row.shipment_type || 'SEA').toUpperCase(),
      expected_reach_date: row.expected_reach_date || '',
    })),
    ...fbaRows.map((row) => ({
      source: 'FBA' as const,
      shipment_id: row.shipment_id,
      display_shipment_id: `FBA-${row.shipment_id}`,
      shipment_status: row.shipment_status,
      sku: row.sku,
      units: toNumber(row.quantity),
      created_at: row.created_at,
      updated_at: row.updated_at,
      dispatch_date: row.dispatch_date || '',
      shipment_type: String(row.shipment_type || 'SEA').toUpperCase(),
      expected_reach_date: row.expected_reach_date || '',
    })),
  ]
}

function buildOthersRow(rows: SkuRow[]): SkuRow {
  return {
    'Product Name': 'Others',
    'SKU': '',
    'FBA': rows.reduce((sum, row) => sum + toNumber(row['FBA']), 0),
    'AWD': rows.reduce((sum, row) => sum + toNumber(row['AWD']), 0),
    'In Transit FBA': rows.reduce((sum, row) => sum + toNumber(row['In Transit FBA']), 0),
    'In Transit AWD': rows.reduce((sum, row) => sum + toNumber(row['In Transit AWD']), 0),
    'In stock': rows.reduce((sum, row) => sum + toNumber(row['In stock']), 0),
    'In transit': rows.reduce((sum, row) => sum + toNumber(row['In transit']), 0),
    'Projected Sales Total': rows.reduce((sum, row) => sum + toNumber(row['Projected Sales Total']), 0),
    'Inventory Coverage Ratio Before Dispatch':
      rows.length > 0
        ? (rows.reduce(
          (sum, row) =>
            sum + toNumber(row['Inventory Coverage Ratio Before Dispatch']),
          0
        ) / rows.length).toFixed(2)
        : 0,
    'Shortfall Unit': rows.reduce((sum, row) => sum + toNumber(row['Shortfall Unit']), 0),
    'To be Dispatch': rows.reduce((sum, row) => sum + toNumber(row['To be Dispatch']), 0),
    'SEA': rows.reduce((sum, row) => sum + toNumber(row['SEA']), 0),
    'AIR': rows.reduce((sum, row) => sum + toNumber(row['AIR']), 0),
  }
}

function renderSkuCell(value: unknown) {
  const skus = String(value ?? '')
    .split(',')
    .map((sku) => sku.trim())
    .filter(Boolean)

  if (!skus.length) return ''

  return (
    <div
      style={{
        width: '100%',
        maxWidth: '100%',
        display: 'flex',
        flexWrap: 'wrap',
        alignItems: 'center',
        justifyContent: 'flex-start',
        gap: '4px 8px',
        textAlign: 'left',
        lineHeight: 1.35,
        whiteSpace: 'normal',
        overflowWrap: 'anywhere',
        wordBreak: 'break-word',
      }}
    >
      {skus.map((sku, index) => (
        <span
          key={`${sku}-${index}`}
          style={{
            display: 'inline-block',
            maxWidth: '100%',
            textAlign: 'left',
            whiteSpace: 'normal',
            overflowWrap: 'anywhere',
            wordBreak: 'break-word',
          }}
        >
          {sku}
          {index < skus.length - 1 ? ',' : ''}
        </span>
      ))}
    </div>
  )
}

function renderSkuBadges(value: unknown) {
  const skus = splitSkuList(String(value ?? ''))

  if (!skus.length) return <span className="text-gray-400">-</span>

  const isSparseSkuRow = skus.length <= 2

  return (
    <div
      className={
        isSparseSkuRow
          ? "flex w-full flex-wrap items-center justify-center gap-3 whitespace-normal"
          : "grid w-full grid-cols-2 content-center items-center gap-x-3 gap-y-2 whitespace-normal"
      }
    >
      {skus.map((sku) => (
        <span
          key={sku}
          className={
            isSparseSkuRow
              ? "inline-flex min-w-[120px] max-w-full items-center justify-center rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-semibold leading-none text-slate-700 shadow-sm"
              : "inline-flex w-full min-w-0 items-center justify-center rounded-full border border-slate-200 bg-white px-3 py-1 text-xs font-semibold leading-none text-slate-700 shadow-sm"
          }
        >
          <span className="truncate">{sku}</span>
        </span>
      ))}
    </div>
  )
}

function getShipmentStatusBadgeClass(status: string) {
  const normalized = status.trim().toLowerCase()

  if (normalized.includes('in_transit') || normalized.includes('in transit')) {
    return 'border-blue-200 bg-blue-50 text-blue-700'
  }

  if (normalized.includes('created') || normalized.includes('active')) {
    return 'border-amber-200 bg-amber-50 text-amber-700'
  }

  if (normalized.includes('shipped') || normalized.includes('received') || normalized.includes('closed')) {
    return 'border-emerald-200 bg-emerald-50 text-emerald-700'
  }

  if (normalized.includes('cancel')) {
    return 'border-red-200 bg-red-50 text-red-700'
  }

  return 'border-slate-200 bg-slate-50 text-slate-700'
}

function renderStatusBadge(value: unknown) {
  const status = String(value || '-')

  const label = status
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase())

  return (
    <span
      className={`inline-flex items-center justify-center rounded-full border px-3 py-1 text-xs font-semibold leading-none ${getShipmentStatusBadgeClass(status)}`}
    >
      {label}
    </span>
  )
}

function parseIsoDate(value?: string | null) {
  if (!value) return null
  const parsed = new Date(`${String(value).slice(0, 10)}T00:00:00`)
  return Number.isNaN(parsed.getTime()) ? null : parsed
}

function getStartOfLocalDay(date: Date) {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate())
}

function compareLocalDates(first: Date, second: Date) {
  return getStartOfLocalDay(first).getTime() - getStartOfLocalDay(second).getTime()
}

function isBeforeMinDate(value?: string | null, minDate?: Date | null) {
  if (!value || !minDate) return false
  const parsed = parseIsoDate(value)
  if (!parsed) return false
  return compareLocalDates(parsed, minDate) < 0
}

function getLatestDate(...dates: Array<Date | null | undefined>) {
  const validDates = dates.filter((date): date is Date => Boolean(date))
  if (!validDates.length) return null

  return validDates.reduce((latest, date) =>
    compareLocalDates(date, latest) > 0 ? date : latest
  )
}

function getTodayDate() {
  return getStartOfLocalDay(new Date())
}

function getDispatchMinDate(row: Pick<InboundDispatchInputRow, 'created_at'>) {
  return parseIsoDate(row.created_at)
}

function getExpectedReachMinDate(
  row: Pick<InboundDispatchInputRow, 'created_at' | 'dispatch_date'>
) {
  return getLatestDate(parseIsoDate(row.created_at), parseIsoDate(row.dispatch_date))
}

function formatIsoDate(date: Date) {
  const year = date.getFullYear()
  const month = String(date.getMonth() + 1).padStart(2, '0')
  const day = String(date.getDate()).padStart(2, '0')
  return `${year}-${month}-${day}`
}

function toPositiveNumber(value: unknown) {
  const parsed = Number(value)
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null
}

function normalizeShipmentType(value: unknown): ShipmentType {
  return String(value || 'SEA').trim().toUpperCase() === 'AIR' ? 'AIR' : 'SEA'
}

function calculateExpectedReachDate(dispatchDate: string | null | undefined, transitWeeks: number | null) {
  const parsedDispatchDate = parseIsoDate(dispatchDate)
  if (!parsedDispatchDate || !transitWeeks) return ''

  const expectedReachDate = getStartOfLocalDay(parsedDispatchDate)
  expectedReachDate.setDate(expectedReachDate.getDate() + transitWeeks * 7)

  return formatIsoDate(expectedReachDate)
}

function getTransitWeeksForShipmentType(
  shipmentTransitWeeks: ShipmentTransitWeeks,
  shipmentType: unknown
) {
  return shipmentTransitWeeks[normalizeShipmentType(shipmentType)]
}

function withAutoExpectedReachDate(
  row: InboundDispatchInputRow,
  shipmentTransitWeeks: ShipmentTransitWeeks,
  options: { overwriteExisting: boolean }
) {
  if (!row.dispatch_date) return row
  if (!options.overwriteExisting && row.expected_reach_date) return row

  const transitWeeks = getTransitWeeksForShipmentType(shipmentTransitWeeks, row.shipment_type)
  const expectedReachDate = calculateExpectedReachDate(row.dispatch_date, transitWeeks)

  return expectedReachDate
    ? {
      ...row,
      expected_reach_date: expectedReachDate,
    }
    : row
}

function getScrollableAncestor(element: HTMLElement | null) {
  if (typeof window === 'undefined') return null

  let current = element?.parentElement ?? null

  while (current) {
    const { overflowY } = window.getComputedStyle(current)
    if (['auto', 'scroll', 'overlay'].includes(overflowY)) {
      return current
    }

    current = current.parentElement
  }

  return null
}

function ShipmentDatePicker({
  id,
  value,
  placeholder,
  minDate,
  onChange,
}: {
  id: string
  value?: string
  placeholder: string
  minDate?: Date | null
  onChange: (dateStr: string) => void
}) {
  const wrapperRef = useRef<HTMLDivElement | null>(null)
  const buttonRef = useRef<HTMLButtonElement | null>(null)
  const [open, setOpen] = useState(false)
  const [mounted, setMounted] = useState(false)
  const [position, setPosition] = useState<{ top: number; left: number } | null>(null)
  const selectedDate = parseIsoDate(value)
  const normalizedMinDate = minDate ? getStartOfLocalDay(minDate) : undefined
  const todayDate = getTodayDate()
  const displayValue = value ? formatReadableDate(value) || value : ''
  const calendarDate =
    selectedDate && (!normalizedMinDate || compareLocalDates(selectedDate, normalizedMinDate) >= 0)
      ? selectedDate
      : normalizedMinDate && compareLocalDates(normalizedMinDate, todayDate) > 0
        ? normalizedMinDate
        : todayDate

  useEffect(() => {
    setMounted(true)
  }, [])

  const getCalendarPosition = () => {
    const button = buttonRef.current
    if (!button) return null

    const rect = button.getBoundingClientRect()
    const calendarWidth = 340
    const calendarHeight = 390
    const gap = 8
    const padding = 12
    const spaceBelow = window.innerHeight - rect.bottom
    const top =
      spaceBelow >= calendarHeight + gap
        ? rect.bottom + gap
        : Math.max(padding, rect.top - calendarHeight - gap)
    const left = Math.min(
      Math.max(padding, rect.left),
      Math.max(padding, window.innerWidth - calendarWidth - padding)
    )

    return { top, left }
  }

  const toggleCalendar = () => {
    if (open) {
      setOpen(false)
      return
    }

    const nextPosition = getCalendarPosition()
    if (!nextPosition) return

    setPosition(nextPosition)
    setOpen(true)
  }

  useEffect(() => {
    if (!open) return

    const updatePosition = () => {
      const nextPosition = getCalendarPosition()
      if (nextPosition) setPosition(nextPosition)
    }

    updatePosition()
    window.addEventListener('resize', updatePosition)
    window.addEventListener('scroll', updatePosition, true)

    return () => {
      window.removeEventListener('resize', updatePosition)
      window.removeEventListener('scroll', updatePosition, true)
    }
  }, [open])

  useEffect(() => {
    if (!open) return

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target as Node
      if (wrapperRef.current?.contains(target) || buttonRef.current?.contains(target)) {
        return
      }
      setOpen(false)
    }

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false)
    }

    document.addEventListener('pointerdown', handlePointerDown, true)
    document.addEventListener('keydown', handleKeyDown)

    return () => {
      document.removeEventListener('pointerdown', handlePointerDown, true)
      document.removeEventListener('keydown', handleKeyDown)
    }
  }, [open])

  return (
    <>
      <button
        ref={buttonRef}
        id={id}
        type="button"
        onClick={toggleCalendar}
        className="flex h-9 w-full min-w-0 items-center justify-between gap-2 rounded-lg border border-gray-300 bg-white px-3 text-left text-sm text-gray-800 shadow-sm transition focus:border-brand-300 focus:outline-none focus:ring-3 focus:ring-brand-500/20"
      >
        <span className={displayValue ? 'truncate' : 'truncate text-gray-400'}>
          {displayValue || placeholder}
        </span>
        <CalendarDays className="h-4 w-4 shrink-0 text-gray-500" />
      </button>

      {open && mounted && position && createPortal(
        <div
          ref={wrapperRef}
          className="fixed z-[10000] rounded-lg border border-slate-200 bg-white p-2 shadow-xl"
          style={{
            top: position.top,
            left: position.left,
            width: 340,
          }}
        >
          <Calendar
            date={calendarDate}
            onChange={(date: Date) => {
              onChange(formatIsoDate(date))
              setOpen(false)
            }}
            color="#5EA68E"
            minDate={normalizedMinDate}
            showMonthAndYearPickers={false}
          />
          <div className="mt-2 flex justify-between gap-2">
            <button
              type="button"
              onClick={() => {
                onChange('')
                setOpen(false)
              }}
              className="rounded border border-slate-200 px-2 py-1 text-xs text-charcoal-500"
            >
              Clear
            </button>
            <button
              type="button"
              onClick={() => setOpen(false)}
              className="rounded border border-charcoal-500 px-2 py-1 text-xs text-charcoal-500"
            >
              Close
            </button>
          </div>
        </div>,
        document.body
      )}
    </>
  )
}

export default function DispatchPage({
  embedded = false,
  countryNameProp,
  selectedMonthProp,
  selectedYearProp,
  showAllRowsProp,
  onShowAllRowsChange,
  shipmentDetailsRequestKey,
  onProductNameClick,
  popupContainer,
}: DispatchPageProps) {
  const params = useParams<{ countryName?: string; month?: string; year?: string }>()
  const router = useRouter()

  const { data: userData } = useGetUserDataQuery();

  const companyName =
    (userData as any)?.companyName ||
    (userData as any)?.company_name ||
    (userData as any)?.company ||
    "";

  const brandName =
    (userData as any)?.brandName ||
    (userData as any)?.brand_name ||
    (userData as any)?.brand ||
    "";

  const countryName = useMemo(
    () => (countryNameProp ?? params?.countryName ?? '').toString(),
    [countryNameProp, params]
  )

  const month = useMemo(
    () => (selectedMonthProp ?? params?.month ?? '').toString(),
    [selectedMonthProp, params]
  )

  const year = useMemo(
    () => (selectedYearProp ?? params?.year ?? '').toString(),
    [selectedYearProp, params]
  )

  const [monthdp, setMonthDp] = useState<string>(getCurrentMonthPlus1())
  const [yeardp, setYearDp] = useState<string>(getCurrentYear())
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [skuData, setSkuData] = useState<SkuRow[]>([])
  // const [showForecastMessage, setShowForecastMessage] = useState(false)
  const [isInitialized, setIsInitialized] = useState(false)
  const [showUpload, setShowUpload] = useState(false)
  const [noData, setNoData] = useState(false)
  const monthdps = useMemo<string[]>(
    () => [...monthNames],
    []
  )
  const [localShowAllDispatchRows, setLocalShowAllDispatchRows] = useState(false)
  const [awdInputRows, setAwdInputRows] = useState<AwdDispatchInputRow[]>([])
  const [fbaInputRows, setFbaInputRows] = useState<FbaDispatchInputRow[]>([])
  const [inboundInputRows, setInboundInputRows] = useState<InboundDispatchInputRow[]>([])
  const [awdInputOpen, setAwdInputOpen] = useState(false)
  const [awdInputDisplayMode, setAwdInputDisplayMode] = useState<ShipmentDetailsDisplayMode>('modal')
  const [awdInputSaving, setAwdInputSaving] = useState(false)
  const [awdInputError, setAwdInputError] = useState('')
  const [shipmentTransitWeeks, setShipmentTransitWeeks] = useState<ShipmentTransitWeeks>(
    EMPTY_SHIPMENT_TRANSIT_WEEKS
  )
  const [dispatchPopupContainer, setDispatchPopupContainer] = useState<HTMLDivElement | null>(null)
  const [modalOverlayRect, setModalOverlayRect] = useState<React.CSSProperties | null>(null)
  const awdInputResolverRef = useRef<((rows: AwdDispatchInputRow[] | null) => void) | null>(null)
  const lastShipmentDetailsRequestKeyRef = useRef(shipmentDetailsRequestKey ?? 0)
  const modalPopupContainer = popupContainer ?? dispatchPopupContainer

  const showAllDispatchRows =
    typeof showAllRowsProp === 'boolean'
      ? showAllRowsProp
      : localShowAllDispatchRows

  const setShowAllDispatchRows =
    onShowAllRowsChange ?? setLocalShowAllDispatchRows

  useEffect(() => {
    if (!awdInputOpen || awdInputDisplayMode !== 'modal' || !modalPopupContainer) {
      setModalOverlayRect(null)
      return
    }

    const scrollHost = getScrollableAncestor(modalPopupContainer)
    const overlayHost = scrollHost ?? modalPopupContainer
    const previousOverflow = scrollHost?.style.overflow
    const previousOverflowY = scrollHost?.style.overflowY
    const previousBodyOverflow = document.body.style.overflow
    const previousHtmlOverflow = document.documentElement.style.overflow

    const updateOverlayRect = () => {
      const rect = overlayHost.getBoundingClientRect()
      setModalOverlayRect({
        top: rect.top,
        left: rect.left,
        width: rect.width,
        height: rect.height,
      })
    }

    if (scrollHost) {
      scrollHost.style.overflow = 'hidden'
      scrollHost.style.overflowY = 'hidden'
    }

    document.body.style.overflow = 'hidden'
    document.documentElement.style.overflow = 'hidden'
    updateOverlayRect()

    const resizeObserver =
      typeof ResizeObserver !== 'undefined'
        ? new ResizeObserver(updateOverlayRect)
        : null

    resizeObserver?.observe(overlayHost)
    window.addEventListener('resize', updateOverlayRect)

    return () => {
      resizeObserver?.disconnect()
      window.removeEventListener('resize', updateOverlayRect)

      if (scrollHost) {
        scrollHost.style.overflow = previousOverflow ?? ''
        scrollHost.style.overflowY = previousOverflowY ?? ''
      }

      document.body.style.overflow = previousBodyOverflow
      document.documentElement.style.overflow = previousHtmlOverflow
      setModalOverlayRect(null)
    }
  }, [awdInputOpen, awdInputDisplayMode, modalPopupContainer])

  async function fetchCountryTransitWeeks(token: string): Promise<ShipmentTransitWeeks> {
    const countryKey = normalizeCountryKey(countryName)
    const marketplaceId = COUNTRY_TO_MARKETPLACE[countryKey]

    if (!countryKey || !marketplaceId) {
      return EMPTY_SHIPMENT_TRANSIT_WEEKS
    }

    const params = new URLSearchParams({
      country: countryKey,
      marketplace: marketplaceId,
    })

    const response = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/country-profile?${params.toString()}`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
        },
      }
    )

    const text = await response.text()
    let data: any = {}
    try {
      data = text ? JSON.parse(text) : {}
    } catch {
      data = { raw: text }
    }

    if (!response.ok) {
      throw new Error(data?.error || data?.message || 'Failed to fetch country profile')
    }

    const profile = data?.profile || {}

    return {
      SEA: toPositiveNumber(profile.ship_time_weeks ?? profile.transit_time),
      AIR: toPositiveNumber(profile.air_time_weeks ?? profile.transit_time),
    }
  }

  async function fetchAwdDispatchInputs(token: string): Promise<AwdDispatchInputRow[]> {
    if (!isAwdSupportedCountry(countryName)) return []

    const marketplaceId = COUNTRY_TO_MARKETPLACE[normalizeCountryKey(countryName)]
    if (!marketplaceId) return []

    const response = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/awd/inbound-shipments/dispatch-inputs?marketplace_id=${encodeURIComponent(marketplaceId)}`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
        },
      }
    )

    const text = await response.text()
    let data: any = {}
    try {
      data = text ? JSON.parse(text) : {}
    } catch {
      data = { raw: text }
    }

    if (!response.ok || data?.success === false) {
      throw new Error(data?.error || 'Failed to fetch AWD shipment details')
    }

    return Array.isArray(data?.items) ? data.items : []
  }

  async function fetchFbaDispatchInputs(token: string): Promise<FbaDispatchInputRow[]> {
    const countryKey = countryName.trim().toLowerCase()
    const marketplaceId = COUNTRY_TO_MARKETPLACE[countryKey]
    if (!marketplaceId) return []
    const shipmentStatuses = ['uk', 'gb'].includes(countryKey) ? 'SHIPPED' : 'IN_TRANSIT'

    const response = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/fba/inbound-shipments/dispatch-inputs?marketplace_id=${encodeURIComponent(marketplaceId)}&shipment_statuses=${encodeURIComponent(shipmentStatuses)}`,
      {
        method: 'GET',
        headers: {
          Authorization: `Bearer ${token}`,
        },
      }
    )

    const text = await response.text()
    let data: any = {}
    try {
      data = text ? JSON.parse(text) : {}
    } catch {
      data = { raw: text }
    }

    if (!response.ok || data?.success === false) {
      throw new Error(data?.error || 'Failed to fetch FBA shipment details')
    }

    return Array.isArray(data?.items) ? data.items : []
  }

  async function fetchAndStoreInboundShipments(token: string) {
    const countryKey = countryName.trim().toLowerCase()
    const marketplaceId = COUNTRY_TO_MARKETPLACE[countryKey]
    if (!marketplaceId) return

    const headers = {
      Authorization: `Bearer ${token}`,
    }

    const requests: Promise<Response>[] = []

    if (['uk', 'gb'].includes(countryKey)) {
      requests.push(
        fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/fba/inbound-shipments?marketplace_id=${encodeURIComponent(marketplaceId)}&shipment_statuses=SHIPPED&store_in_db=true`,
          { method: 'GET', headers }
        )
      )
    } else {
      requests.unshift(
        fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/awd/inbound-shipments-complete?marketplace_id=${encodeURIComponent(marketplaceId)}&sku_quantities=SHOW&max_results=100&store_in_db=true`,
          { method: 'GET', headers }
        )
      )
      requests.push(
        fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/fba/inbound-plans-all?marketplace_id=${encodeURIComponent(marketplaceId)}&statuses=ACTIVE,SHIPPED&store_in_db=true&max_plans=50`,
          { method: 'GET', headers }
        )
      )
    }

    const results = await Promise.allSettled(requests)
    const errors: string[] = []

    for (const result of results) {
      if (result.status === 'rejected') {
        errors.push(result.reason?.message || 'Inbound shipment fetch failed')
        continue
      }

      let data: any = {}
      try {
        data = await result.value.json()
      } catch { }

      if (!result.value.ok || data?.success === false) {
        const amazonMessage =
          data?.amazon_error?.response_json?.errors?.[0]?.message ||
          data?.list_errors?.[0]?.error ||
          data?.error
        errors.push(amazonMessage || `Inbound shipment fetch failed (${result.value.status})`)
      }
    }

    if (errors.length === results.length) {
      throw new Error(errors[0] || 'Failed to fetch inbound shipments from Amazon')
    }
  }

  async function saveAwdDispatchInputs(token: string, rows: AwdDispatchInputRow[]) {
    if (!isAwdSupportedCountry(countryName)) return

    const marketplaceId = COUNTRY_TO_MARKETPLACE[normalizeCountryKey(countryName)]
    if (!marketplaceId) return

    const response = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/awd/inbound-shipments/dispatch-inputs`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          marketplace_id: marketplaceId,
          shipments: rows.map((row) => ({
            shipment_id: row.shipment_id,
            dispatch_date: row.dispatch_date,
            shipment_type: String(row.shipment_type || '').toUpperCase(),
            expected_reach_date: row.expected_reach_date,
          })),
        }),
      }
    )

    const text = await response.text()
    let data: any = {}
    try {
      data = text ? JSON.parse(text) : {}
    } catch {
      data = { raw: text }
    }

    if (!response.ok || data?.success === false) {
      throw new Error(data?.error || 'Failed to save AWD shipment details')
    }
  }

  async function saveFbaDispatchInputs(token: string, rows: FbaDispatchInputRow[]) {
    const marketplaceId = COUNTRY_TO_MARKETPLACE[countryName.trim().toLowerCase()]
    if (!marketplaceId) return

    const response = await fetch(
      `${process.env.NEXT_PUBLIC_API_BASE_URL}/amazon_api/fba/inbound-shipments/dispatch-inputs`,
      {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          marketplace_id: marketplaceId,
          shipments: rows.map((row) => ({
            shipment_id: row.shipment_id,
            dispatch_date: row.dispatch_date,
            shipment_type: String(row.shipment_type || '').toUpperCase(),
            expected_reach_date: row.expected_reach_date,
          })),
        }),
      }
    )

    const text = await response.text()
    let data: any = {}
    try {
      data = text ? JSON.parse(text) : {}
    } catch {
      data = { raw: text }
    }

    if (!response.ok || data?.success === false) {
      throw new Error(data?.error || 'Failed to save FBA shipment details')
    }
  }

  async function requestAwdDispatchInputs(
    token: string,
    initialError = '',
    displayMode: ShipmentDetailsDisplayMode = 'modal'
  ): Promise<AwdDispatchInputRow[]> {
    const shouldFetchAwd = isAwdSupportedCountry(countryName)
    const transitWeeks = await fetchCountryTransitWeeks(token).catch((err) => {
      console.error('Failed to fetch country transit times:', err)
      return EMPTY_SHIPMENT_TRANSIT_WEEKS
    })

    setShipmentTransitWeeks(transitWeeks)

    let [rows, fbaRows] = await Promise.all([
      shouldFetchAwd ? fetchAwdDispatchInputs(token) : Promise.resolve([]),
      fetchFbaDispatchInputs(token),
    ])

    if (!rows.length && !fbaRows.length) {
      await fetchAndStoreInboundShipments(token)
        ;[rows, fbaRows] = await Promise.all([
          shouldFetchAwd ? fetchAwdDispatchInputs(token) : Promise.resolve([]),
          fetchFbaDispatchInputs(token),
        ])
    }

    if (!rows.length && !fbaRows.length) {
      throw new Error(
        shouldFetchAwd
          ? 'No AWD or FBA inbound shipments found for this marketplace after fetching Amazon inbound shipments.'
          : 'No FBA inbound shipments found for this marketplace after fetching Amazon inbound shipments.'
      )
    }

    const normalizedAwdRows = rows.map((row) => ({
      ...row,
      dispatch_date: row.dispatch_date || '',
      shipment_type: String(row.shipment_type || 'SEA').toUpperCase(),
      expected_reach_date: row.expected_reach_date || '',
    }))
    const normalizedFbaRows = fbaRows.map((row) => ({
      ...row,
      dispatch_date: row.dispatch_date || '',
      shipment_type: String(row.shipment_type || 'SEA').toUpperCase(),
      expected_reach_date: row.expected_reach_date || '',
    }))

    setAwdInputRows(normalizedAwdRows)
    setFbaInputRows(normalizedFbaRows)
    setInboundInputRows(
      buildInboundDispatchRows(normalizedAwdRows, normalizedFbaRows).map((row) =>
        withAutoExpectedReachDate(row, transitWeeks, { overwriteExisting: false })
      )
    )
    setAwdInputError(initialError)
    setAwdInputDisplayMode(displayMode)
    setAwdInputOpen(true)

    const savedRows = await new Promise<AwdDispatchInputRow[] | null>((resolve) => {
      awdInputResolverRef.current = resolve
    })

    if (!savedRows) {
      throw new Error(SHIPMENT_DETAILS_CANCELLED_MESSAGE)
    }

    return savedRows
  }

  function updateInboundInputRow(
    source: InboundDispatchInputRow['source'],
    shipmentId: string,
    patch: Partial<Pick<InboundDispatchInputRow, 'dispatch_date' | 'shipment_type' | 'expected_reach_date'>>
  ) {
    setInboundInputRows((rows) =>
      rows.map((row) => {
        if (row.source !== source || row.shipment_id !== shipmentId) {
          return row
        }

        const nextRow = { ...row, ...patch }
        const shouldRecalculateExpectedReach =
          'dispatch_date' in patch || 'shipment_type' in patch

        if ('shipment_type' in patch) {
          nextRow.shipment_type = normalizeShipmentType(patch.shipment_type)
        }

        const dispatchMinDate = getDispatchMinDate(nextRow)

        if (isBeforeMinDate(nextRow.dispatch_date, dispatchMinDate)) {
          nextRow.dispatch_date = ''
        }

        if ('dispatch_date' in patch && !nextRow.dispatch_date) {
          nextRow.expected_reach_date = ''
        }

        const nextRowWithExpectedReach =
          shouldRecalculateExpectedReach && nextRow.dispatch_date
            ? withAutoExpectedReachDate(nextRow, shipmentTransitWeeks, {
              overwriteExisting: true,
            })
            : nextRow

        const expectedReachMinDate = getExpectedReachMinDate(nextRowWithExpectedReach)

        if (isBeforeMinDate(nextRowWithExpectedReach.expected_reach_date, expectedReachMinDate)) {
          nextRowWithExpectedReach.expected_reach_date = ''
        }

        return nextRowWithExpectedReach
      })
    )
  }

  function closeAwdInputModal(rows: AwdDispatchInputRow[] | null) {
    setAwdInputOpen(false)
    setInboundInputRows([])
    setAwdInputDisplayMode('modal')
    const resolve = awdInputResolverRef.current
    awdInputResolverRef.current = null
    resolve?.(rows)
  }

  async function handleSaveAwdInputRows() {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null
    if (!token) {
      setAwdInputError('Authorization token is missing')
      return
    }

    const missing = inboundInputRows.find(
      (row) => !row.dispatch_date || !row.shipment_type || !row.expected_reach_date
    )
    if (missing) {
      setAwdInputError('Please fill dispatch date, shipment type, and expected reach date for every shipment.')
      return
    }

    const invalidDispatchDate = inboundInputRows.find((row) =>
      isBeforeMinDate(row.dispatch_date, getDispatchMinDate(row))
    )
    if (invalidDispatchDate) {
      setAwdInputError(
        `Dispatch date cannot be before Created At for shipment ${invalidDispatchDate.display_shipment_id}.`
      )
      return
    }

    const invalidExpectedReachDate = inboundInputRows.find((row) =>
      isBeforeMinDate(row.expected_reach_date, getExpectedReachMinDate(row))
    )
    if (invalidExpectedReachDate) {
      setAwdInputError(
        `Expected reach date cannot be before Created At or Dispatch Date for shipment ${invalidExpectedReachDate.display_shipment_id}.`
      )
      return
    }

    try {
      setAwdInputSaving(true)
      setAwdInputError('')
      const normalizedInboundRows = inboundInputRows.map((row) => ({
        ...row,
        shipment_type: String(row.shipment_type || '').toUpperCase(),
      }))
      const normalizedAwdRows = normalizedInboundRows
        .filter((row) => row.source === 'AWD')
        .map((row) => ({
          ...awdInputRows.find((awdRow) => awdRow.shipment_id === row.shipment_id),
          shipment_id: row.shipment_id,
          shipment_status: row.shipment_status,
          sku: row.sku,
          expected_unit_quantity: row.units,
          created_at: row.created_at,
          updated_at: row.updated_at,
          dispatch_date: row.dispatch_date,
          shipment_type: row.shipment_type,
          expected_reach_date: row.expected_reach_date,
        }))
      const normalizedFbaRows = normalizedInboundRows
        .filter((row) => row.source === 'FBA')
        .map((row) => ({
          ...fbaInputRows.find((fbaRow) => fbaRow.shipment_id === row.shipment_id),
          shipment_id: row.shipment_id,
          shipment_status: row.shipment_status,
          sku: row.sku,
          quantity: row.units,
          created_at: row.created_at,
          updated_at: row.updated_at,
          dispatch_date: row.dispatch_date,
          shipment_type: row.shipment_type,
          expected_reach_date: row.expected_reach_date,
        }))
      await Promise.all([
        normalizedAwdRows.length ? saveAwdDispatchInputs(token, normalizedAwdRows) : Promise.resolve(),
        normalizedFbaRows.length ? saveFbaDispatchInputs(token, normalizedFbaRows) : Promise.resolve(),
      ])
      closeAwdInputModal(normalizedAwdRows)
    } catch (err: any) {
      setAwdInputError(err?.message || 'Failed to save shipment details')
    } finally {
      setAwdInputSaving(false)
    }
  }

  async function fetchDispatchFile(
    monthdpValue: string,
    yeardpValue: string,
    options: { promptForShipmentDetails?: boolean } = {}
  ) {
    if (!monthdpValue || !yeardpValue) {
      setError('Please select both month and year.')
      setNoData(false)
      return
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null
    if (!token) {
      setError('Authorization token is missing')
      setNoData(false)
      return
    }

    setError('')
    setNoData(false)
    setLoading(false)

    try {
      let awdRows = options.promptForShipmentDetails
        ? await requestAwdDispatchInputs(token)
        : []

      setLoading(true)
      setSkuData([])

      let response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/getDispatchfile?country=${countryName}&month=${monthdpValue}&year=${yeardpValue}`,
        {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      )

      if (!response.ok) {
        let errorData: any = {}
        try {
          errorData = await response.json()
        } catch { }

        const msg = String(errorData?.error || 'Failed to fetch dispatch file')

        if (isShipmentDetailsRequiredError(msg)) {
          setLoading(false)
          awdRows = await requestAwdDispatchInputs(
            token,
            SHIPMENT_DETAILS_REQUIRED_MESSAGE,
            'inline'
          )
          setLoading(true)

          response = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/getDispatchfile?country=${countryName}&month=${monthdpValue}&year=${yeardpValue}`,
            {
              method: 'GET',
              headers: {
                Authorization: `Bearer ${token}`,
              },
            }
          )

          if (!response.ok) {
            let retryErrorData: any = {}
            try {
              retryErrorData = await response.json()
            } catch { }

            const retryMsg = String(retryErrorData?.error || msg)

            if (
              retryMsg.includes('Forecast file not found') ||
              retryMsg.includes('Please generate inventory forecast first') ||
              retryMsg.includes('No UK or US forecast files found') ||
              retryMsg.includes('No readable UK/US dispatch data found') ||
              retryMsg.includes('No UK or US dispatch files found')
            ) {
              setError('')
              setNoData(true)
              setSkuData([])
              return
            }

            throw new Error(retryMsg)
          }
        } else if (
          msg.includes('Forecast file not found') ||
          msg.includes('Please generate inventory forecast first') ||
          msg.includes('No UK or US forecast files found') ||
          msg.includes('No readable UK/US dispatch data found') ||
          msg.includes('No UK or US dispatch files found')
        ) {
          setError('')
          setNoData(true)
          setSkuData([])
          return
        } else {
          throw new Error(msg)
        }
      }

      const blob = await response.blob()
      const data = await blob.arrayBuffer()
      const workbook = XLSX.read(new Uint8Array(data), { type: 'array' })

      const sheetName =
        workbook.SheetNames.find((name) => name.trim().toLowerCase() === 'dispatch') ||
        workbook.SheetNames[0]

      const sheet = workbook.Sheets[sheetName]

      if (!sheet) {
        throw new Error(`Sheet "${sheetName}" not found in workbook`)
      }

      const rows = XLSX.utils.sheet_to_json<any[]>(sheet, {
        header: 1,
        defval: '',
        raw: true,
      })

      if (!rows || rows.length === 0) {
        setNoData(true)
        setSkuData([])
        return
      }

      const headerRowIndex = findHeaderRowIndex(rows)

      if (headerRowIndex === -1) {
        setNoData(true)
        setSkuData([])
        return
      }

      const duplicateHeaderCounts: Record<string, number> = {}
      const rawHeaders = (rows[headerRowIndex] || []).map((h) => {
        const normalized = normalizeHeader(h)
        if (normalized === 'FBA' || normalized === 'AWD') {
          duplicateHeaderCounts[normalized] = (duplicateHeaderCounts[normalized] || 0) + 1
          if (duplicateHeaderCounts[normalized] === 2) {
            return normalized === 'FBA' ? 'In Transit FBA' : 'In Transit AWD'
          }
        }
        return normalized
      })
      const dataRows = rows.slice(headerRowIndex + 1)

      const jsonData: SkuRow[] = applyAwdShipmentDetails(dataRows
        .filter(
          (row) =>
            Array.isArray(row) &&
            row.some((cell) => String(cell ?? '').trim() !== '')
        )
        .map((row) => {
          const obj: SkuRow = {}

          rawHeaders.forEach((header, idx) => {
            if (!header) return
            if (String(header).startsWith('Unnamed:')) return
            obj[header] = parseCellValue(header, row[idx])
          })

          if (obj['FBA'] === undefined || obj['FBA'] === '') {
            obj['FBA'] = toNumber(obj['Inventory at Month End'])
          }

          if (obj['AWD'] === undefined || obj['AWD'] === '') {
            obj['AWD'] = toNumber(obj['total_onhand_quantity'])
          }

          if (obj['Projected Sales Total'] !== undefined && obj['FBA'] !== undefined) {
            if (obj['AWD'] === undefined || obj['AWD'] === '') {
              obj['AWD'] = 0
            }

            const availableStock = toNumber(obj['FBA']) + toNumber(obj['AWD'])
            const hasBackendDispatchPlan =
              obj['Shortfall Unit'] !== undefined &&
              obj['Shortfall Unit'] !== '' &&
              obj['To be Dispatch'] !== undefined &&
              obj['To be Dispatch'] !== ''

            obj['In stock'] = availableStock

            if (!hasBackendDispatchPlan) {
              const inTransit = toNumber(obj['In transit'])
              const computedShortfall = Math.max(
                toNumber(obj['Projected Sales Total']) - availableStock - inTransit,
                0,
              )

              obj['Shortfall Unit'] = computedShortfall
              obj['To be Dispatch'] = computedShortfall

              if (
                obj['SEA'] === undefined ||
                obj['SEA'] === '' ||
                obj['AIR'] === undefined ||
                obj['AIR'] === '' ||
                toNumber(obj['SEA']) + toNumber(obj['AIR']) !== toNumber(obj['To be Dispatch'])
              ) {
                obj['SEA'] = obj['To be Dispatch']
                obj['AIR'] = 0
              }
            }
          }

          return obj
        })
        .filter((row) => isMeaningfulRow(row)), awdRows, monthdpValue, yeardpValue)

      if (!jsonData.length) {
        setNoData(true)
        setSkuData([])
        return
      }

      setSkuData(jsonData)
      setNoData(false)
    } catch (err: any) {
      if (err?.message === SHIPMENT_DETAILS_CANCELLED_MESSAGE) {
        return
      }

      console.error('Fetch error:', err)
      setError(err?.message ?? 'Unknown error')
      setNoData(false)
      setSkuData([])
    } finally {
      setLoading(false)
    }
  }

  function handleRedirectToForecast() {
    router.push(`/inventory-forecast/${countryName}/${month}/${year}`)
  }

  useEffect(() => {
    if (month && year) {
      const capitalizeMonth = capitalize(month)
      const monthIndex = monthdps.indexOf(capitalizeMonth)

      const dispatchMonth =
        monthIndex >= 0
          ? monthdps[(monthIndex + 1) % 12]
          : getCurrentMonthPlus1()

      const dispatchYear =
        monthIndex === 11
          ? String(Number(year) + 1)
          : year

      setMonthDp((previous) =>
        previous === dispatchMonth ? previous : dispatchMonth
      )

      setYearDp((previous) =>
        previous === dispatchYear ? previous : dispatchYear
      )

      setIsInitialized(true)
      return
    }

    setMonthDp(getCurrentMonthPlus1())
    setYearDp(getCurrentYear())
    setIsInitialized(true)
  }, [month, year, monthdps])

  useEffect(() => {
    if (isInitialized && monthdp && yeardp) {
      void fetchDispatchFile(monthdp, yeardp)
    }
  }, [isInitialized, monthdp, yeardp])

  useEffect(() => {
    if (!shipmentDetailsRequestKey || !isInitialized || !monthdp || !yeardp) return
    if (awdInputOpen) {
      lastShipmentDetailsRequestKeyRef.current = shipmentDetailsRequestKey
      return
    }
    if (lastShipmentDetailsRequestKeyRef.current === shipmentDetailsRequestKey) return

    lastShipmentDetailsRequestKeyRef.current = shipmentDetailsRequestKey
    void fetchDispatchFile(monthdp, yeardp, { promptForShipmentDetails: true })
  }, [shipmentDetailsRequestKey, isInitialized, monthdp, yeardp, awdInputOpen])


  function isTotalRow(row: SkuRow) {
    return (
      String(row['Product Name'] ?? '').trim().toLowerCase() === 'total' ||
      String(row['SKU'] ?? '').trim().toLowerCase() === 'total'
    )
  }

  function calculateColumnTotal(columnName: string) {
    return skuData
      .filter((row) => !isTotalRow(row))
      .reduce((sum, row) => {
        const value = row[columnName]
        const num =
          typeof value === 'number'
            ? value
            : Number(String(value ?? '').replace(/,/g, '').trim())

        return sum + (Number.isFinite(num) ? num : 0)
      }, 0)
  }

  const displayedColumns = [...DISPLAYED_COLUMNS]

  function handleExportToExcel() {
    if (!skuData.length || noData || loading) return;

    let exportSerialNo = 0;

    const exportRows = skuData.map((row) => {
      const formattedRow: Record<string, string | number> = {
      };

      displayedColumns.forEach((col) => {
        if (col === 'S. No.') {
          formattedRow[col] = isTotalRow(row) ? '' : ++exportSerialNo;
          return;
        }

        if (col === 'SKU' && isTotalRow(row)) {
          formattedRow['SKU'] = '';
          return;
        }

        if (
          isTotalRow(row) &&
          [
            'FBA',
            'AWD',
            'In Transit FBA',
            'In Transit AWD',
            'In stock',
            'In transit',
            'Projected Sales Total',
            'Inventory Coverage Ratio Before Dispatch',
            'Shortfall Unit',
            'To be Dispatch',
            'SEA',
            'AIR',
          ].includes(col)
        ) {
          formattedRow[col] =
            col === 'Inventory Coverage Ratio Before Dispatch'
              ? '-'
              : calculateColumnTotal(col);
          return;
        }

        if (isTotalRow(row) && col === 'Product Name') {
          formattedRow[col] = 'Total';
          return;
        }

        const v = row[col];
        formattedRow[col] =
          typeof v === 'number' || typeof v === 'string' ? v : '';
      });

      return formattedRow;
    });

    void exportDispatchExcel({
      filename: `Dispatch Report ${monthdp}-${yeardp}.xlsx`,
      titleLine: `Amazon ${countryName?.toLowerCase() === 'global' ? 'Global' : countryName?.toUpperCase()
        } - Dispatch Report - ${monthdp} ${yeardp}`,
      titleCountry:
        countryName?.toLowerCase() === 'global'
          ? 'Global'
          : countryName?.toUpperCase(),
      platformLabel: 'Phormula',
      periodLabel: `${monthdp} ${yeardp}`,
      companyName,
      brandName,
      dataRows: exportRows,
    });
  }

  useEffect(() => {
    const handleDownload = () => {
      handleExportToExcel();
    };

    window.addEventListener('dispatch-report-download', handleDownload);

    return () => {
      window.removeEventListener('dispatch-report-download', handleDownload);
    };
  }, [
    skuData,
    noData,
    loading,
    monthdp,
    yeardp,
    countryName,
    companyName,
    brandName,
  ]);

  const tableRows = useMemo<DispatchTableRow[]>(() => {
    const totalRow = skuData.find((row) => isTotalRow(row))

    const sortedRows = [...skuData]
      .filter((row) => !isTotalRow(row))
      .sort((a, b) => {
        const valA = Number(a['FBA'] ?? 0)
        const valB = Number(b['FBA'] ?? 0)
        return valB - valA
      })

    let rowsForDisplay: SkuRow[] = []

    if (showAllDispatchRows || sortedRows.length <= 9) {
      rowsForDisplay = [...sortedRows]
    } else {
      const firstNine = sortedRows.slice(0, 9)
      const remainingRows = sortedRows.slice(9)
      const othersRow = buildOthersRow(remainingRows)

      rowsForDisplay = [...firstNine, othersRow]
    }

    if (totalRow) {
      rowsForDisplay.push(totalRow)
    }

    let serialNo = 0

    return rowsForDisplay.map((row) => {
      const isTotal = isTotalRow(row)
      const isOthers = String(row['Product Name'] ?? '').trim().toLowerCase() === 'others'

      const obj: DispatchTableRow = {
        __isTotal: isTotal,
        __isOthers: isOthers,
      }

      displayedColumns.forEach((col) => {
        if (col === 'S. No.') {
          obj[col] = isTotal || isOthers ? '' : ++serialNo
          return
        }

        if (isTotal) {
          if (col === 'FBA') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'AWD') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'In Transit FBA') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'In Transit AWD') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'In stock') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'In transit') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'Projected Sales Total') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'Shortfall Unit') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'To be Dispatch') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'SEA') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'AIR') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'Inventory Coverage Ratio Before Dispatch') {
            obj[col] = '-'
            return
          }
        }

        if (isTotal) {
          if (col === 'SKU') {
            obj[col] = ''
            return
          }
          if (col === 'Product Name') {
            obj[col] = 'Total'
            return
          }
        }

        const v = row[col]

        if (col === 'SKU') {
          obj[col] = renderSkuCell(v)
          return
        }

        obj[col] =
          typeof v === 'number'
            ? v.toLocaleString('en-US')
            : v !== undefined && v !== null
              ? String(v)
              : ''
      })

      obj.sno = obj['S. No.']

      const rawProductName = String(row['Product Name'] ?? '').trim()
      const rawSku = String(row['SKU'] ?? '').trim()
      const isClickableProduct =
        !!rawProductName &&
        !isTotal &&
        !isOthers &&
        !['total', 'others', 'other skus', '-'].includes(rawProductName.toLowerCase())

      obj.product_name = isClickableProduct ? (
        <button
          type="button"
          onClick={(event) => {
            event.stopPropagation()
            onProductNameClick?.(rawProductName, rawSku || undefined)
          }}
          className="cursor-zoom-in text-left  text-green-500"
        >
          {rawProductName}
        </button>
      ) : obj['Product Name']

      obj.sku = obj['SKU']
      obj.fba = obj['FBA']
      obj.awd = obj['AWD']
      obj.in_stock_total = obj['In stock']
      obj.in_transit_fba = obj['In Transit FBA']
      obj.in_transit_awd = obj['In Transit AWD']
      obj.in_transit_total = obj['In transit']
      obj.coverage_ratio = obj['Inventory Coverage Ratio Before Dispatch']
      obj.projected_sales_total = obj['Projected Sales Total']
      obj.shortfall_units = obj['Shortfall Unit']
      obj.sea = obj['SEA']
      obj.air = obj['AIR']
      obj.dispatch_total = obj['To be Dispatch']

      return obj
    })
  }, [skuData, displayedColumns, showAllDispatchRows, onProductNameClick])

  const dispatchLeftCols = useMemo<LeafCol<DispatchTableRow>[]>(
    () => [
      { key: 'sno', label: 'S. No.', width: 58, align: 'center' },
      {
        key: 'product_name',
        label: 'Product Name',
        width: 220,
        align: 'left',
        tdClassName: '!whitespace-normal !text-clip break-words align-middle',
      },
      {
        key: 'sku',
        label: 'SKU',
        width: 160,
        align: 'left',
        tdClassName: 'dispatch-sku-cell',
      },
    ],
    []
  )

  const dispatchGroups = useMemo<ColGroup<DispatchTableRow>[]>(
    () => [
      {
        id: 'in_stock',
        label: 'In Stock',
        collapsedCols: [
          { key: 'in_stock_total', label: 'Total', width: 110, align: 'center' },
        ],
        expandedCols: [
          { key: 'fba', label: 'FBA', width: 100, align: 'center' },
          { key: 'awd', label: 'AWD', width: 100, align: 'center' },
          { key: 'in_stock_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
      {
        id: 'in_transit',
        label: 'In Transit',
        collapsedCols: [
          { key: 'in_transit_total', label: 'Total', width: 110, align: 'center' },
        ],
        expandedCols: [
          { key: 'in_transit_fba', label: 'In Transit FBA', width: 120, align: 'center' },
          { key: 'in_transit_awd', label: 'In Transit AWD', width: 120, align: 'center' },
          { key: 'in_transit_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
      {
        id: 'to_be_dispatched',
        label: 'To be Dispatched',
        collapsedCols: [
          { key: 'dispatch_total', label: 'Total', width: 110, align: 'center' },
        ],
        expandedCols: [
          { key: 'sea', label: 'SEA', width: 100, align: 'center' },
          { key: 'air', label: 'AIR', width: 100, align: 'center' },
          { key: 'dispatch_total', label: 'Total', width: 110, align: 'center' },
        ],
      },
    ],
    []
  )

  const dispatchSingleCols = useMemo<LeafCol<DispatchTableRow>[]>(
    () => [
      {
        key: 'coverage_ratio',
        label: 'Coverage Ratio Before Dispatch',
        width: 160,
        align: 'center',
      },
      {
        key: 'projected_sales_total',
        label: 'Projected Sales Total',
        width: 150,
        align: 'center',
      },
      {
        key: 'shortfall_units',
        label: 'Shortfall Units',
        width: 130,
        align: 'center',
      },
    ],
    []
  )

  const dispatchTableLayout = useMemo(
    () => [
      { type: 'group' as const, id: 'in_stock' },
      { type: 'group' as const, id: 'in_transit' },
      { type: 'single' as const, key: 'coverage_ratio' },
      { type: 'single' as const, key: 'projected_sales_total' },
      { type: 'single' as const, key: 'shortfall_units' },
      { type: 'group' as const, id: 'to_be_dispatched' },
    ],
    []
  )

  const dispatchInitialCollapsed = useMemo(
    () => ({
      in_stock: true,
      in_transit: true,
      to_be_dispatched: true,
    }),
    []
  )

  function renderInboundShipmentDetailsPanel(displayMode: ShipmentDetailsDisplayMode) {
    const isInline = displayMode === 'inline'
    const shipmentTableBodyMaxHeight = 'calc(100% - 40px)'
    const shipmentTableClassName =
      'inbound-shipment-details-table !w-full !min-w-[1450px] 2xl:!min-w-0'
    const rows: InboundShipmentTableRow[] = inboundInputRows.map((row, rowIndex) => ({
      source: row.source,
      shipment_id: row.shipment_id,
      rowIndex,
      serialNo: rowIndex + 1,
      createdAtRaw: row.created_at || '',
      shipmentId: row.display_shipment_id,
      status: row.shipment_status || '-',
      sku: row.sku || '',
      units: row.units ?? 0,
      createdAt: formatReadableDate(row.created_at) || '-',
      updatedAt: formatReadableDate(row.updated_at) || '-',
      dispatchDate: row.dispatch_date || '',
      shipmentType: row.shipment_type || '',
      expectedReachDate: row.expected_reach_date || '',
    }))

    const columns: ColumnDef<InboundShipmentTableRow>[] = [
      {
        key: 'serialNo',
        header: 'S. No.',
        width: '5%',
        cellClassName: 'font-semibold text-slate-600',
      },
      {
        key: 'shipmentId',
        header: 'Shipment ID',
        width: '12%',
        cellClassName: '!whitespace-normal break-words text-left font-medium',
      },
      {
        key: 'status',
        header: 'Status',
        width: '8%',
        render: (_row, value) => renderStatusBadge(value),
      },
      {
        key: 'sku',
        header: 'SKU',
        width: '19%',
        cellClassName: '!whitespace-normal align-middle',
        render: (_row, value) => renderSkuBadges(value),
      },
      {
        key: 'units',
        header: 'Units',
        width: '6%',
      },
      {
        key: 'createdAt',
        header: 'Created At',
        width: '8%',
      },
      {
        key: 'updatedAt',
        header: 'Updated At',
        width: '8%',
      },
      {
        key: 'dispatchDate',
        header: 'Dispatch Date',
        width: '13%',
        render: (row) => (
          <div className="w-full min-w-0">
            <ShipmentDatePicker
              id={`inbound-dispatch-date-${row.source}-${row.rowIndex}`}
              value={String(row.dispatchDate || '')}
              placeholder="Select date"
              minDate={getDispatchMinDate({
                created_at: String(row.createdAtRaw || ''),
              })}
              onChange={(dateStr) =>
                updateInboundInputRow(row.source, row.shipment_id, {
                  dispatch_date: dateStr,
                })
              }
            />
          </div>
        ),
      },
      {
        key: 'shipmentType',
        header: 'Shipment Type',
        width: '10%',
        render: (row) => {
          const selectedShipmentType = String(row.shipmentType || 'SEA').toUpperCase() === 'AIR' ? 'AIR' : 'SEA'

          return (
            <div className="flex w-full justify-center">
              <SegmentedToggle<'SEA' | 'AIR'>
                value={selectedShipmentType}
                options={SHIPMENT_TYPE_OPTIONS}
                onChange={(shipmentType) =>
                  updateInboundInputRow(row.source, row.shipment_id, {
                    shipment_type: shipmentType,
                  })
                }
                className="mx-auto"
                compact
                textSizeClass="text-xs"
              />
            </div>
          )
        },
      },
      {
        key: 'expectedReachDate',
        header: 'Expected Reach Date',
        width: '11%',
        render: (row) => (
          <div className="w-full min-w-0">
            <ShipmentDatePicker
              id={`inbound-expected-reach-date-${row.source}-${row.rowIndex}`}
              value={String(row.expectedReachDate || '')}
              placeholder="Select date"
              minDate={getExpectedReachMinDate({
                created_at: String(row.createdAtRaw || ''),
                dispatch_date: String(row.dispatchDate || ''),
              })}
              onChange={(dateStr) =>
                updateInboundInputRow(row.source, row.shipment_id, {
                  expected_reach_date: dateStr,
                })
              }
            />
          </div>
        ),
      },
    ]

    return (
      <div
        className={
          isInline
            ? "flex h-[calc(100vh-260px)] min-h-[360px] w-full flex-col overflow-hidden rounded-xl bg-white"
            : "flex h-[calc(100%-32px)] max-h-[calc(100%-32px)] w-[calc(100%-32px)] max-w-none flex-col overflow-hidden rounded-xl bg-white shadow-2xl"
        }
      >
        <div className="shrink-0 border-b border-gray-200 px-5 py-4">
          <PageBreadcrumb
            pageTitle="Inbound Shipment Details"
            align='left'
            textSize='xl'
          />

          <p className="mt-1 text-xs text-charcoal-500">
            Fill dispatch date, shipment type, and expected reach date before opening the dispatch file.
          </p>
        </div>

        <div className="min-h-0 flex-1 overflow-hidden px-5 py-4">
          <DataTable<InboundShipmentTableRow>
            columns={columns}
            data={rows}
            paginate={false}
            zebra
            maxHeight="100%"
            bodyMaxHeight={shipmentTableBodyMaxHeight}
            emptyMessage="No inbound shipments found."
            className="h-full shadow-none [&>div]:h-full [&>div]:overflow-y-hidden 2xl:[&>div]:overflow-x-hidden"
            tableClassName={shipmentTableClassName}
            headerMaxWidth={120}
            rowClassName={(_row, index) => (index % 2 === 0 ? 'bg-white' : 'bg-gray-50')}
          />
        </div>

        {!isInline && awdInputError && (
          <div className="shrink-0 px-5 pb-2 text-sm font-medium text-red-600">{awdInputError}</div>
        )}

        <div className="flex shrink-0 justify-end gap-3 border-t border-gray-200 px-5 py-4">

          {!isInline && (
            <Button
              type="button"
              variant="outline"
              size="md"
              onClick={() => closeAwdInputModal(null)}
              disabled={awdInputSaving}
            >
              Cancel
            </Button>
          )}

          <Button
            type="button"
            variant="primary"
            size="md"
            onClick={handleSaveAwdInputRows}
            disabled={awdInputSaving}
          >
            {awdInputSaving ? 'Saving...' : 'Save and Open Dispatch'}
          </Button>

        </div>
      </div>
    )
  }

  return (
    <>
      <style jsx global>{`
  .inline-dropdowns {
    display: flex;
    flex-wrap: wrap;
    gap: 12px;
    align-items: center;
    justify-content: flex-end;
    margin-bottom: 24px;
  }

  .forecast-data {
    // margin-top: 20px;
    width: 100%;
    overflow-x: auto;
  }

  .forecast-data table {
    width: 100%;
    table-layout: fixed;
  }

.dispatch-sku-cell {
  text-align: left !important;
  white-space: normal !important;
  overflow: visible !important;
  vertical-align: middle !important;
  padding-left: 8px !important;
  padding-right: 8px !important;
}

.dispatch-sku-cell > * {
  margin-left: 0 !important;
  margin-right: 0 !important;
  justify-content: flex-start !important;
  text-align: left !important;
}

.forecast-data {
  // margin-top: 20px;
  width: 100%;
  overflow-x: auto;
}

.forecast-data table {
  width: 100%;
  table-layout: fixed;
}

@media (max-width: 1440px) {
  .forecast-data table {
    min-width: 1080px;
  }
}

  @media (max-width: 768px) {
    .inline-dropdowns {
      width: 100%;
      justify-content: flex-start;
    }
  }

  @media (max-width: 600px) {
    .inline-dropdowns {
      flex-direction: column;
      gap: 3vh;
    }

    .dropdown-table,
    .uploads-table {
      width: 90vw;
    }

    .styled-button2 {
      display: block;
    }

    .uploads-cell {
      padding: 1px;
    }
  }

  /* keep the rest of your existing CSS below this */
`}</style>

      {awdInputOpen &&
        awdInputDisplayMode === 'modal' &&
        modalOverlayRect &&
        createPortal(
          <div
            className="fixed z-[1050] flex items-center justify-center overflow-hidden bg-transparent p-4"
            style={modalOverlayRect}
          >
            {renderInboundShipmentDetailsPanel('modal')}
          </div>,
          document.body
        )}

      <div
        ref={setDispatchPopupContainer}
        className={embedded ? "relative min-h-0" : "relative min-h-[calc(100vh-180px)]"}
      >
        {/* {awdInputOpen && awdInputDisplayMode === 'modal' && (
          <div className="absolute inset-0 z-30 flex items-start justify-center overflow-y-auto bg-black/45 px-4 py-6">
            {renderInboundShipmentDetailsPanel('modal')}
          </div>
        )} */}

        {!embedded && (
          <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
            <div className="flex flex-wrap items-baseline gap-2 justify-start">
              <PageBreadcrumb
                pageTitle="Dispatch Report - "
                variant="page"
                align="left"
                className=""
              />
              <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                Amazon {countryName?.toLowerCase() === 'global' ? 'Global' : countryName?.toUpperCase()}
              </span>
            </div>

            <div className={`inline-dropdowns ${embedded ? 'w-full flex justify-end' : ''}`}>
              <MonthYearPickerTable
                month={monthdp}
                year={yeardp}
                yearOptions={[new Date().getFullYear(), new Date().getFullYear() - 1]}
                onMonthChange={(v) => setMonthDp(capitalize(v))}
                onYearChange={(v) => setYearDp(String(v))}
                valueMode="lower"
              />

              <button
                type="button"
                onClick={() => fetchDispatchFile(monthdp, yeardp, { promptForShipmentDetails: true })}
                title="Edit inbound shipment dates"
                aria-label="Edit inbound shipment dates"
                disabled={loading || awdInputOpen}
                className="inline-flex h-8 items-center gap-2 rounded-md border border-gray-300 bg-white px-3 text-sm font-medium text-blue-700 transition-all duration-200 ease-out hover:-translate-y-0.5 hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50 lg:h-9"
              >
                <CalendarDays className="h-4 w-4" />
                <span>Inbound Shipments</span>
              </button>

              {/* <div className="centralised-fetch-button">
              <button className="fetch-button" onClick={() => fetchDispatchFile(monthdp, yeardp)}>
                Get Report
              </button>
            </div> */}

              {skuData.filter((row) => !isTotalRow(row)).length > 9 && (
                <button
                  type="button"
                  onClick={() => setShowAllDispatchRows((prev) => !prev)}
                  title={showAllDispatchRows ? "Collapse rows" : "Expand all rows"}
                  aria-label={showAllDispatchRows ? "Collapse rows" : "Expand all rows"}
                  disabled={loading || noData}
                  className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-0.5 hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50 lg:h-9 lg:w-9"
                >
                  {showAllDispatchRows ? (
                    <RiCollapseDiagonalFill className="h-4 w-4 font-extrabold lg:h-4.5 lg:w-4.5" />
                  ) : (
                    <RiExpandDiagonalFill className="h-4 w-4 font-extrabold lg:h-4.5 lg:w-4.5" />
                  )}
                </button>
              )}

              <DownloadIconButton onClick={handleExportToExcel} size="md" />
            </div>
          </div>
        )}

        {loading ? (
          <div className="flex flex-col items-center justify-center py-12 text-center">
            <Loader fullscreen contained transparent zIndex={20} />
          </div>
        ) : error ? (
          <div className="alert-container">
            <div className="alert-message">
              <i className="fa-solid fa-circle-exclamation alert-icon"></i>
              <span>{error}</span>
            </div>
            <button className="alert-button" onClick={() => setShowUpload(true)}>
              Run Now <i className="fa-solid fa-chevron-right"></i>
            </button>
          </div>
        ) : (
          <>
            {/* {embedded && skuData.filter((row) => !isTotalRow(row)).length > 9 && (
            <div className="mb-3 flex justify-end">
              <button
                type="button"
                onClick={() => setShowAllDispatchRows((prev) => !prev)}
                title={showAllDispatchRows ? "Collapse rows" : "Expand all rows"}
                aria-label={showAllDispatchRows ? "Collapse rows" : "Expand all rows"}
                disabled={loading || noData}
                className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50 lg:h-9 lg:w-9"
              >
                {showAllDispatchRows ? (
                  <RiCollapseDiagonalFill className="h-4 w-4 font-extrabold lg:h-[18px] lg:w-[18px]" />
                ) : (
                  <RiExpandDiagonalFill className="h-4 w-4 font-extrabold lg:h-[18px] lg:w-[18px]" />
                )}
              </button>
            </div>
          )} */}

            <div className="forecast-data border border-slate-200 bg-white shadow-sm rounded-xl">
              {awdInputOpen && awdInputDisplayMode === 'inline' ? (
                renderInboundShipmentDetailsPanel('inline')
              ) : tableRows.length === 0 ? (
                <div className="flex min-h-55 items-center justify-center rounded-lg border border-gray-200 bg-white p-6 text-center text-sm text-neutral-600">
                  {noData
                    ? "No Data Available for selected period"
                    : "Select Month and Year to see Dispatch!"}
                </div>
              ) : (
                <GroupedCollapsibleTable<DispatchTableRow>
                  rows={tableRows}
                  getRowKey={(row, index) =>
                    row.__isTotal ? 'total' : row.__isOthers ? 'others' : index
                  }
                  leftCols={dispatchLeftCols}
                  groups={dispatchGroups}
                  singleCols={dispatchSingleCols}
                  layout={dispatchTableLayout}
                  initialCollapsed={dispatchInitialCollapsed}
                  getValue={(row, colKey) => row[colKey] ?? ''}
                  getRowClassName={(row, index) => {
                    if (row.__isTotal) return "bg-[#EFEFEF] font-semibold"
                    if (row.__isOthers && !showAllDispatchRows) return "cursor-pointer bg-white"
                    return index % 2 === 0 ? "bg-white" : "bg-gray-50"
                  }}
                  onRowClick={(row) => {
                    if (!showAllDispatchRows && row.__isOthers) {
                      setShowAllDispatchRows(true)
                    }
                  }}
                  isTotalRow={(row) => !!row.__isTotal}
                  bodyMaxHeight={
                    showAllDispatchRows &&
                      tableRows.filter((row) => !row.__isTotal).length > 15
                      ? 40 * 15
                      : undefined
                  }
                  tableClassName="w-full table-fixed border-collapse bg-white text-[#414042] text-xs 2xl:text-sm"
                  headerRow1ClassName="bg-[#5EA68E] text-[#f8edcf]"
                  headerRow2ClassName="bg-[#5EA68E] text-[#f8edcf]"
                  preserveColumnWidths="responsive"
                  stickyLeftBorderMode="shadow-only"
                  stickyLeftDividerMode="leading"
                  stickyLeftHorizontalBorderMode="border"
                />
              )}
            </div>
          </>
        )}

        <Modal
          isOpen={showUpload}
          onClose={() => setShowUpload(false)}
          showCloseButton
          className="max-w-4xl w-full mx-auto p-0"
        >
          <FileUploadForm
            initialCountry={''}
            onClose={() => setShowUpload(false)}
            onComplete={() => {
              setShowUpload(false)
              void fetchDispatchFile(monthdp, yeardp)
            }}
          />
        </Modal>
      </div>
    </>
  )
}
