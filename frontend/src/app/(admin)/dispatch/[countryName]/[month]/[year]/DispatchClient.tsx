'use client'

import React, { useEffect, useMemo, useState } from 'react'
import { useParams, useRouter } from 'next/navigation'
import * as XLSX from 'xlsx'
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css'
import { Modal } from '@/components/ui/modal'
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm'
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable'
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable'
import DownloadIconButton from '@/components/ui/button/DownloadButton'
import PageBreadcrumb from '@/components/common/PageBreadCrumb'
import Loader from '@/components/loader/Loader'
import { exportDispatchExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { useGetUserDataQuery } from '@/lib/api/profileApi'

interface SkuRow {
  [key: string]: string | number | undefined
  SKU?: string
  'Product Name'?: string
  'Inventory at Month End'?: string | number
  'Inventory Coverage Ratio Before Dispatch'?: string | number
  'Dispatch'?: string | number
  'Current Inventory + Dispatch'?: string | number
}

type DispatchPageProps = {
  embedded?: boolean
  countryNameProp?: string
  selectedMonthProp?: string
  selectedYearProp?: string
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
  'Inventory at Month End',
  'Inventory Coverage Ratio Before Dispatch',
  'Dispatch',
  'Current Inventory + Dispatch',
] as const

const NUMERIC_COLUMNS = [
  'Inventory at Month End',
  'Inventory Coverage Ratio Before Dispatch',
  'Dispatch',
  'Current Inventory + Dispatch',
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
    'inventory coverage ratio before dispatch': 'Inventory Coverage Ratio Before Dispatch',
    dispatch: 'Dispatch',
    'current inventory + dispatch': 'Current Inventory + Dispatch',
    'projected sales total': 'Projected Sales Total',
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
  const dispatch = row['Dispatch']
  const currentInventoryDispatch = row['Current Inventory + Dispatch']
  const coverageRatio = row['Inventory Coverage Ratio Before Dispatch']

  return Boolean(
    productName ||
    sku ||
    (inventoryAtMonthEnd !== '' && inventoryAtMonthEnd !== undefined) ||
    (dispatch !== '' && dispatch !== undefined) ||
    (currentInventoryDispatch !== '' && currentInventoryDispatch !== undefined) ||
    (coverageRatio !== '' && coverageRatio !== undefined)
  )
}


function toNumber(value: unknown): number {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0
  const num = Number(String(value ?? '').replace(/,/g, '').trim())
  return Number.isFinite(num) ? num : 0
}

function buildOthersRow(rows: SkuRow[]): SkuRow {
  return {
    'Product Name': 'Others',
    'SKU': '',
    'Inventory at Month End': rows.reduce((sum, row) => sum + toNumber(row['Inventory at Month End']), 0),
    'Inventory Coverage Ratio Before Dispatch': rows.reduce(
      (sum, row) => sum + toNumber(row['Inventory Coverage Ratio Before Dispatch']),
      0
    ),
    'Dispatch': rows.reduce((sum, row) => sum + toNumber(row['Dispatch']), 0),
    'Current Inventory + Dispatch': rows.reduce(
      (sum, row) => sum + toNumber(row['Current Inventory + Dispatch']),
      0
    ),
  }
}

export default function DispatchPage({
  embedded = false,
  countryNameProp,
  selectedMonthProp,
  selectedYearProp,
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
  const monthdps = monthNames as unknown as string[]

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

    setLoading(true)
    setError('')
    setNoData(false)
    setSkuData([])

    try {
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

      const rawHeaders = (rows[headerRowIndex] || []).map((h) => normalizeHeader(h))
      const dataRows = rows.slice(headerRowIndex + 1)

      const jsonData: SkuRow[] = dataRows
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

          return obj
        })
        .filter((row) => isMeaningfulRow(row))

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
      const nextMonth = monthdps[(monthIndex + 1) % 12]
      setMonthDp(nextMonth)
      setYearDp(year)
      setIsInitialized(true)
    } else {
      setMonthDp(getCurrentMonthPlus1())
      setYearDp(getCurrentYear())
      setIsInitialized(true)
    }
  }, [month, year, monthdps])

  useEffect(() => {
    if (isInitialized && monthdp && yeardp) {
      void fetchDispatchFile(monthdp, yeardp)
    }
  }, [isInitialized, monthdp, yeardp])

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
    const exportRows = skuData.map((row, index) => {
      const formattedRow: Record<string, string | number> = {
        'S. No.': isTotalRow(row) ? '' : index + 1,
      }

      displayedColumns.forEach((col) => {
        if (col === 'S. No.') return

        if (col === 'SKU' && isTotalRow(row)) {
          formattedRow['SKU'] = ''
          return
        }

        if (
          isTotalRow(row) &&
          [
            'Inventory at Month End',
            'Dispatch',
            'Current Inventory + Dispatch',
            'Inventory Coverage Ratio Before Dispatch',
          ].includes(col)
        ) {
          formattedRow[col] = calculateColumnTotal(col)
          return
        }

        if (isTotalRow(row) && col === 'Product Name') {
          formattedRow[col] = 'Total'
          return
        }

        const v = row[col]
        formattedRow[col] =
          typeof v === 'number' || typeof v === 'string' ? v : ''
      })

      return formattedRow
    })

    void exportDispatchExcel({
      filename: `Dispatch Report ${monthdp}-${yeardp}.xlsx`,
      titleLine: `Amazon ${countryName?.toLowerCase() === 'global' ? 'Global' : countryName?.toUpperCase()
        } - Dispatch Report - ${monthdp} ${yeardp}`,
      titleCountry:
        countryName?.toLowerCase() === 'global' ? 'Global' : countryName?.toUpperCase(),
      platformLabel: 'Phormula',
      periodLabel: `${monthdp} ${yeardp}`,
      companyName,
      brandName,
      dataRows: exportRows,
    })
  }

  const tableRows = useMemo(() => {
    const totalRow = skuData.find((row) => isTotalRow(row))

    const sortedRows = [...skuData]
      .filter((row) => !isTotalRow(row))
      .sort((a, b) => {
        const valA = Number(a['Inventory at Month End'] ?? 0)
        const valB = Number(b['Inventory at Month End'] ?? 0)
        return valB - valA
      })

    let rowsForDisplay: SkuRow[] = []

    if (sortedRows.length <= 9) {
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

    return rowsForDisplay.map((row, index) => {
      const isTotal = isTotalRow(row)
      const isOthers = String(row['Product Name'] ?? '').trim().toLowerCase() === 'others'

      const obj: Record<string, any> = {
        __isTotal: isTotal,
        __isOthers: isOthers,
        sno: isTotal ? '' : index + 1,
      }

      displayedColumns.forEach((col) => {
        if (col === 'S. No.') return

        if (
          isTotal &&
          [
            'Inventory at Month End',
            'Dispatch',
            'Current Inventory + Dispatch',
            'Inventory Coverage Ratio Before Dispatch',
          ].includes(col)
        ) {
          obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
          return
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
        obj[col] =
          typeof v === 'number'
            ? v.toLocaleString('en-US')
            : v !== undefined && v !== null
              ? String(v)
              : ''
      })

      return obj
    })
  }, [skuData, displayedColumns])

  const columns: ColumnDef<any>[] = displayedColumns.map((col) => {
    const isCoverage = col === 'Inventory Coverage Ratio Before Dispatch'
    const isSNo = col === 'S. No.'
    const isProduct = col === 'Product Name'
    const isSku = col === 'SKU'
    const isInventoryMonthEnd = col === 'Inventory at Month End'
    const isDispatch = col === 'Dispatch'
    const isCurrentInventoryDispatch = col === 'Current Inventory + Dispatch'

    return {
      key: isSNo ? 'sno' : col,
      header: col,
      width: isSNo
        ? '60px'
        : isProduct
          ? '220px'
          : isSku
            ? '180px'
            : isCoverage
              ? '200px'
              : isInventoryMonthEnd
                ? '180px'
                : isDispatch
                  ? '140px'
                  : isCurrentInventoryDispatch
                    ? '250px'
                    : '160px',
      cellClassName:
        isProduct
          ? 'text-left'
          : isSNo
            ? 'text-center'
            : 'text-center',
      headerClassName:
        'text-center whitespace-normal break-words max-w-[120px] lg:max-w-[200px]',
    }
  })

  return (
    <>
      <style jsx>{`
        .inline-dropdowns {
          display: flex;
          flex-wrap: wrap;
          gap: 12px;
          align-items: center;
          justify-content: flex-end;
          margin-bottom: 24px;
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

        .dropdown-table,
        .uploads-table {
          border-collapse: collapse;
          border-radius: 0.5vw;
          width: auto;
          min-width: 80px;
          max-width: 100px;
          font-family: 'Lato', sans-serif;
        }

        .dropdown-header,
        .uploads-header {
          background-color: #fff;
          color: #5ea68e;
          border: 0.05vw solid #414042;
        }

        .dropdown-cell,
        .uploads-cell {
          padding: 1vh 0.9vw;
          border: 0.05vw solid #414042;
          text-align: center;
          font-size: clamp(12px, 0.729vw, 16px);
        }

        .tablec tbody tr:last-child {
          background-color: #ccc !important;
          color: #414042;
          text-align: center;
          font-weight: bold;
        }

        .tablec td:first-child,
        .tablec th:first-child {
          text-align: center;
          width: 19px;
        }

        .tablec thead th {
          background-color: #5ea68e !important;
          color: #f8edcf !important;
          font-weight: bold !important;
          text-align: center !important;
          font-size: clamp(12px, 0.729vw, 16px) !important;
        }

        .tablec tbody tr:nth-child(even) {
          background-color: #5ea68e33;
        }

        .tablec tbody tr:nth-child(odd) {
          background-color: #ffffff;
        }

        .dropdown-select {
          font-size: clamp(12px, 0.729vw, 16px);
          text-align: center;
          width: auto;
          min-width: 60px;
        }

        .dropdown-table select,
        .dropdown-table option {
          font-size: clamp(12px, 0.729vw, 16px);
          border: none;
          font-family: 'Lato', sans-serif;
        }

        .dropdown-select:focus {
          outline: none;
          box-shadow: none;
        }

        .button-wrapper {
          display: flex;
          gap: 10px;
          flex-wrap: wrap;
        }

        .fetch-button {
          font-family: 'Lato', sans-serif;
          font-size: clamp(12px, 0.729vw, 16px) !important;
          background-color: #2c3e50;
          color: #f8edcf;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          text-align: center;
          padding: 10px 18px;
          transition: background-color 0.2s ease;
          box-shadow: 0 3px 6px rgba(0, 0, 0, 0.15);
          white-space: nowrap;
        }

        .fetch-button:hover:not(:disabled) {
          background-color: #1f2a36;
        }

        .fetch-button:disabled {
          background-color: #6b7280;
          cursor: not-allowed;
          opacity: 0.8;
        }

        .styled-button {
          font-family: 'Lato', sans-serif;
          font-size: clamp(12px, 0.729vw, 16px) !important;
          background-color: #2c3e50;
          color: #f8edcf;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          text-align: center;
          padding: 9px 18px;
          margin-left: auto;
        }

        .forecast-message {
          background-color: #fff3cd;
          border: 1px solid #ffeaa7;
          border-radius: 8px;
          padding: 20px;
          margin: 20px 0;
          text-align: center;
          font-family: 'Lato', sans-serif;
        }

        .forecast-message h3 {
          color: #856404;
          margin-bottom: 10px;
          font-size: 16px;
        }

        .forecast-message p {
          color: #856404;
          margin-bottom: 15px;
          font-size: 14px;
        }

        .forecast-redirect-button {
          font-family: 'Lato', sans-serif;
          font-size: 14px;
          background-color: #5ea68e;
          color: white;
          font-weight: bold;
          border: none;
          border-radius: 5px;
          cursor: pointer;
          text-align: center;
          padding: 12px 20px;
          transition: background-color 0.2s ease;
          box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }

        .forecast-redirect-button:hover {
          background-color: #4a8c73;
        }

        .forecast-banner {
          background-color: #f2f2f2;
          border-top: 4px solid #f44336;
          padding: 12px 16px;
          display: flex;
          align-items: center;
          gap: 10px;
          font-family: 'Segoe UI', sans-serif;
          font-size: 14px;
          color: #414042;
          border-radius: 4px;
        }

        .forecast-banner i.fa-circle-exclamation {
          color: #f44336;
          font-size: 16px;
        }

        .forecast-banner .forecast-action {
          margin-left: auto;
          background: none;
          border: none;
          color: #414042;
          font-weight: 600;
          cursor: pointer;
          text-decoration: underline;
          display: flex;
          align-items: center;
          gap: 5px;
        }

        .forecast-banner .forecast-action i {
          font-size: 12px;
        }

        .alert-container {
          display: flex;
          align-items: center;
          background-color: #f2f2f2;
          border-top: 4px solid #ff5c5c;
          padding: 12px 16px;
          border-radius: 6px;
          font-family: 'Lato', sans-serif;
          width: 100%;
          max-width: 700px;
          justify-content: space-between;
          box-sizing: border-box;
          margin-top: 20px;
        }

        .alert-message {
          display: flex;
          align-items: center;
          color: #414042;
          font-size: 14px;
        }

        .alert-icon {
          color: #ff5c5c;
          font-size: 18px;
          margin-right: 10px;
        }

        .alert-button {
          background: none;
          border: none;
          color: #414042;
          cursor: pointer;
          font-size: 14px;
          text-decoration: underline;
          display: inline-flex;
          align-items: center;
          white-space: nowrap;
          padding: 0;
        }

        .centralised-fetch-button {
          display: flex;
          align-items: center;
        }

        .loading-wrapper {
          text-align: center;
          padding: 20px;
          font-family: 'Lato', sans-serif;
          font-size: 16px;
        }

        .forecast-data {
          margin-top: 20px;
        }

        .ellipsis {
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
        }

        .ellipsis-center {
          white-space: nowrap;
          overflow: hidden;
          text-overflow: ellipsis;
          text-align: center;
        }

        .one-line-ellipsis {
          white-space: nowrap !important;
          overflow: hidden !important;
          text-overflow: ellipsis !important;
          text-align: center;
          display: block;
          width: 100%;
        }

.tablec th {
  white-space: normal !important;
  word-break: break-word;
  line-height: 1.2;
}

/* Force 2-line wrap on laptop screens */
@media (max-width: 1440px) {
  .tablec th {
    max-width: 140px;
  }
}

/* Even tighter on smaller screens */
@media (max-width: 1024px) {
  .tablec th {
    max-width: 110px;
    font-size: 12px;
  }
}

      `}</style>

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
        <div className="forecast-data">
          <DataTable
            columns={columns}
            data={tableRows}
            paginate={false}
            scrollY
            maxHeight="90vh"
            stickyHeader
            loading={loading}
            emptyMessage={
              noData
                ? 'No Data Available for selected period'
                : 'Select Month and Year to see Dispatch!'
            }
            rowClassName={(row: any) =>
              row.__isTotal ? 'bg-[#D9D9D9] font-bold' : ''
            }
            tableClassName="text-xs 2xl:text-sm [&_th]:whitespace-normal [&_th]:break-words [&_th]:text-center [&_th]:py-3"
          />
        </div>
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