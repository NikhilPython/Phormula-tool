'use client'

import React, { useEffect, useMemo, useRef, useState } from 'react'
import { useParams, useRouter } from 'next/navigation'
import * as XLSX from 'xlsx'
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css'
import { Modal } from '@/components/ui/modal'
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm'
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable'
import GroupedCollapsibleTable, {
  type ColGroup,
  type LeafCol,
} from '@/components/ui/table/GroupedCollapsibleTable'
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import PageBreadcrumb from '@/components/common/PageBreadCrumb'
import Loader from '@/components/loader/Loader'
import { exportDispatchExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { useGetUserDataQuery } from '@/lib/api/profileApi'
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";

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
  promptOnOpenKey?: number
  onProductNameClick?: (productName: string, sku?: string) => void
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
  shipment_type?: string | null
  expected_reach_date?: string | null
  sku_quantities?: Array<{
    sku?: string | null
    expected_unit_quantity?: number | string | null
  }> | null
}

const COUNTRY_TO_MARKETPLACE: Record<string, string> = {
  uk: 'A1F83G8C2ARO7P',
  gb: 'A1F83G8C2ARO7P',
  us: 'ATVPDKIKX0DER',
  usa: 'ATVPDKIKX0DER',
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

function formatAwdDate(value?: string | null): string {
  if (!value) return ''
  return String(value).slice(0, 10)
}

function splitSkuList(value?: string | null): string[] {
  return String(value ?? '')
    .split(',')
    .map((sku) => sku.trim().toUpperCase())
    .filter(Boolean)
}

function applyAwdShipmentDetails(rows: SkuRow[], awdRows: AwdDispatchInputRow[]): SkuRow[] {
  if (!awdRows.length) return rows

  const unitsBySku = new Map<string, number>()
  awdRows.forEach((shipment) => {
    const skuQuantities = Array.isArray(shipment.sku_quantities) ? shipment.sku_quantities : []

    if (skuQuantities.length) {
      skuQuantities.forEach((item) => {
        const sku = String(item?.sku ?? '').trim().toUpperCase()
        if (!sku) return
        unitsBySku.set(
          sku,
          (unitsBySku.get(sku) ?? 0) + toNumber(item?.expected_unit_quantity)
        )
      })
      return
    }

    const skus = splitSkuList(shipment.sku)
    if (skus.length === 1) {
      const sku = skus[0]
      unitsBySku.set(
        sku,
        (unitsBySku.get(sku) ?? 0) + toNumber(shipment.expected_unit_quantity)
      )
    }
  })

  return rows.map((row) => {
    const sku = String(row.SKU ?? '').trim().toUpperCase()
    const awdUnits = unitsBySku.get(sku)
    if (awdUnits === undefined) return row

    const inTransitFba = toNumber(row['In Transit FBA'])
    const nextInTransitAwd = Math.round(awdUnits)

    return {
      ...row,
      'In Transit AWD': nextInTransitAwd,
      'In transit': inTransitFba + nextInTransitAwd,
    }
  })
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

export default function DispatchPage({
  embedded = false,
  countryNameProp,
  selectedMonthProp,
  selectedYearProp,
  showAllRowsProp,
  onShowAllRowsChange,
  promptOnOpenKey,
  onProductNameClick,
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
  const [awdInputOpen, setAwdInputOpen] = useState(false)
  const [awdInputSaving, setAwdInputSaving] = useState(false)
  const [awdInputError, setAwdInputError] = useState('')
  const awdInputResolverRef = useRef<((rows: AwdDispatchInputRow[] | null) => void) | null>(null)

  const showAllDispatchRows =
    typeof showAllRowsProp === 'boolean'
      ? showAllRowsProp
      : localShowAllDispatchRows

  const setShowAllDispatchRows =
    onShowAllRowsChange ?? setLocalShowAllDispatchRows

  async function fetchAwdDispatchInputs(token: string): Promise<AwdDispatchInputRow[]> {
    const marketplaceId = COUNTRY_TO_MARKETPLACE[countryName.trim().toLowerCase()]
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

  async function saveAwdDispatchInputs(token: string, rows: AwdDispatchInputRow[]) {
    const marketplaceId = COUNTRY_TO_MARKETPLACE[countryName.trim().toLowerCase()]
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

  async function requestAwdDispatchInputs(token: string): Promise<AwdDispatchInputRow[]> {
    const rows = await fetchAwdDispatchInputs(token)
    if (!rows.length) {
      throw new Error('No AWD inbound shipments found for dispatch input. Please fetch AWD inbound shipments first.')
    }

    setAwdInputRows(
      rows.map((row) => ({
        ...row,
        shipment_type: row.shipment_type || '',
        expected_reach_date: row.expected_reach_date || '',
      }))
    )
    setAwdInputError('')
    setAwdInputOpen(true)

    const savedRows = await new Promise<AwdDispatchInputRow[] | null>((resolve) => {
      awdInputResolverRef.current = resolve
    })

    if (!savedRows) {
      throw new Error('Please complete AWD shipment details before opening dispatch.')
    }

    return savedRows
  }

  function updateAwdInputRow(
    shipmentId: string,
    patch: Partial<Pick<AwdDispatchInputRow, 'shipment_type' | 'expected_reach_date'>>
  ) {
    setAwdInputRows((rows) =>
      rows.map((row) =>
        row.shipment_id === shipmentId
          ? { ...row, ...patch }
          : row
      )
    )
  }

  function closeAwdInputModal(rows: AwdDispatchInputRow[] | null) {
    setAwdInputOpen(false)
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

    const missing = awdInputRows.find((row) => !row.shipment_type || !row.expected_reach_date)
    if (missing) {
      setAwdInputError('Please select shipment type and expected reach date for every AWD shipment.')
      return
    }

    try {
      setAwdInputSaving(true)
      setAwdInputError('')
      const normalizedRows = awdInputRows.map((row) => ({
        ...row,
        shipment_type: String(row.shipment_type || '').toUpperCase(),
      }))
      await saveAwdDispatchInputs(token, normalizedRows)
      closeAwdInputModal(normalizedRows)
    } catch (err: any) {
      setAwdInputError(err?.message || 'Failed to save AWD shipment details')
    } finally {
      setAwdInputSaving(false)
    }
  }

  async function fetchDispatchFile(monthdpValue: string, yeardpValue: string) {
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
      const awdRows = await requestAwdDispatchInputs(token)

      setLoading(true)
      setSkuData([])

      const response = await fetch(
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

        if (
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
        }

        throw new Error(msg)
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
            const computedShortfall = Math.max(
              toNumber(obj['Projected Sales Total']) - availableStock,
              0,
            )

            obj['In stock'] = availableStock
            obj['Shortfall Unit'] = computedShortfall
            obj['To be Dispatch'] = Math.max(
              computedShortfall - toNumber(obj['In transit']),
              0,
            )

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

          return obj
        })
        .filter((row) => isMeaningfulRow(row)), awdRows)

      if (!jsonData.length) {
        setNoData(true)
        setSkuData([])
        return
      }

      setSkuData(jsonData)
      setNoData(false)
    } catch (err: any) {
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

    const nextMonth =
      monthIndex >= 0
        ? monthdps[(monthIndex + 1) % 12]
        : getCurrentMonthPlus1()

    const nextYear =
      monthIndex === 11
        ? String(Number(year) + 1)
        : year

    setMonthDp((previous) =>
      previous === nextMonth ? previous : nextMonth
    )

    setYearDp((previous) =>
      previous === nextYear ? previous : nextYear
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
  }, [isInitialized, monthdp, yeardp, promptOnOpenKey])


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

      {awdInputOpen && (
        <div className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/45 px-4">
          <div className="w-full max-w-6xl rounded-lg bg-white shadow-xl">
            <div className="border-b border-gray-200 px-5 py-4">
              <h3 className="text-lg font-semibold text-gray-900">AWD Shipment Details</h3>
              <p className="mt-1 text-sm text-gray-500">
                Select shipment type and expected reach date before opening the dispatch file.
              </p>
            </div>

            <div className="max-h-[60vh] overflow-auto px-5 py-4">
              <table className="min-w-full border-collapse text-sm">
                <thead>
                  <tr className="bg-gray-50 text-left text-xs uppercase text-gray-500">
                    <th className="border px-3 py-2">Shipment ID</th>
                    <th className="border px-3 py-2">Status</th>
                    <th className="border px-3 py-2">SKU</th>
                    <th className="border px-3 py-2">Units</th>
                    <th className="border px-3 py-2">Created At</th>
                    <th className="border px-3 py-2">Updated At</th>
                    <th className="border px-3 py-2">Ship By</th>
                    <th className="border px-3 py-2">Shipment Type</th>
                    <th className="border px-3 py-2">Expected Reach Date</th>
                  </tr>
                </thead>
                <tbody>
                  {awdInputRows.map((row) => (
                    <tr key={row.shipment_id} className="text-gray-700">
                      <td className="border px-3 py-2 font-medium">{row.shipment_id}</td>
                      <td className="border px-3 py-2">{row.shipment_status || '-'}</td>
                      <td className="max-w-[220px] whitespace-normal break-words border px-3 py-2">
                        {row.sku || '-'}
                      </td>
                      <td className="border px-3 py-2 text-center">{row.expected_unit_quantity ?? 0}</td>
                      <td className="border px-3 py-2">{formatAwdDate(row.created_at) || '-'}</td>
                      <td className="border px-3 py-2">{formatAwdDate(row.updated_at) || '-'}</td>
                      <td className="border px-3 py-2">{formatAwdDate(row.ship_by) || '-'}</td>
                      <td className="border px-3 py-2">
                        <select
                          className="w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm"
                          value={row.shipment_type || ''}
                          onChange={(event) =>
                            updateAwdInputRow(row.shipment_id, {
                              shipment_type: event.target.value,
                            })
                          }
                        >
                          <option value="">Select</option>
                          <option value="SEA">SEA</option>
                          <option value="AIR">AIR</option>
                        </select>
                      </td>
                      <td className="border px-3 py-2">
                        <input
                          className="w-full rounded-md border border-gray-300 px-2 py-1.5 text-sm"
                          type="date"
                          value={row.expected_reach_date || ''}
                          onChange={(event) =>
                            updateAwdInputRow(row.shipment_id, {
                              expected_reach_date: event.target.value,
                            })
                          }
                        />
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {awdInputError && (
              <div className="px-5 pb-2 text-sm font-medium text-red-600">{awdInputError}</div>
            )}

            <div className="flex justify-end gap-3 border-t border-gray-200 px-5 py-4">
              <button
                type="button"
                className="rounded-md border border-gray-300 px-4 py-2 text-sm font-medium text-gray-700 disabled:opacity-60"
                onClick={() => closeAwdInputModal(null)}
                disabled={awdInputSaving}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded-md bg-green-600 px-4 py-2 text-sm font-medium text-white disabled:opacity-60"
                onClick={handleSaveAwdInputRows}
                disabled={awdInputSaving}
              >
                {awdInputSaving ? 'Saving...' : 'Save and Open Dispatch'}
              </button>
            </div>
          </div>
        </div>
      )}

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
          <Loader fullscreen transparent />
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
            {tableRows.length === 0 ? (
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
    </>
  )
}
