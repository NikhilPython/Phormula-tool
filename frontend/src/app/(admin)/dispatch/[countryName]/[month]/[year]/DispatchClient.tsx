'use client'

import React, { useEffect, useMemo, useState } from 'react'
import { useParams, useRouter } from 'next/navigation'
import * as XLSX from 'xlsx'
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css'
import { Modal } from '@/components/ui/modal'
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm'
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable'
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable'
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import PageBreadcrumb from '@/components/common/PageBreadCrumb'
import Loader from '@/components/loader/Loader'
import { exportDispatchExcel } from "@/lib/excel/exportCurrentInventoryExcel";
import { useGetUserDataQuery } from '@/lib/api/profileApi'
import { RiExpandDiagonalFill, RiCollapseDiagonalFill } from "react-icons/ri";

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
  showAllRowsProp?: boolean
  onShowAllRowsChange?: React.Dispatch<React.SetStateAction<boolean>>
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
    'Inventory Coverage Ratio Before Dispatch':
      rows.length > 0
        ? (rows.reduce(
          (sum, row) =>
            sum + toNumber(row['Inventory Coverage Ratio Before Dispatch']),
          0
        ) / rows.length).toFixed(2)
        : 0,
    'Dispatch': rows.reduce((sum, row) => sum + toNumber(row['Dispatch']), 0),
    'Current Inventory + Dispatch': rows.reduce(
      (sum, row) => sum + toNumber(row['Current Inventory + Dispatch']),
      0
    ),
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
  const [localShowAllDispatchRows, setLocalShowAllDispatchRows] = useState(false)

  const showAllDispatchRows =
    typeof showAllRowsProp === 'boolean'
      ? showAllRowsProp
      : localShowAllDispatchRows

  const setShowAllDispatchRows =
    onShowAllRowsChange ?? setLocalShowAllDispatchRows

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

  function calculateTotalCoverageRatio() {
    const nonTotalRows = skuData.filter((row) => !isTotalRow(row))

    const totalCurrentInventoryDispatch = nonTotalRows.reduce(
      (sum, row) => sum + toNumber(row['Current Inventory + Dispatch']),
      0
    )

    const totalDispatch = nonTotalRows.reduce(
      (sum, row) => sum + toNumber(row['Dispatch']),
      0
    )

    if (totalDispatch === 0) return 0

    return totalCurrentInventoryDispatch / totalDispatch
  }

  const displayedColumns = [...DISPLAYED_COLUMNS]

  function handleExportToExcel() {
    if (!skuData.length || noData || loading) return;

    const exportRows = skuData.map((row, index) => {
      const formattedRow: Record<string, string | number> = {
        'S. No.': isTotalRow(row) ? '' : index + 1,
      };

      displayedColumns.forEach((col) => {
        if (col === 'S. No.') return;

        if (col === 'SKU' && isTotalRow(row)) {
          formattedRow['SKU'] = '';
          return;
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
          formattedRow[col] =
            col === 'Inventory Coverage Ratio Before Dispatch'
              ? calculateTotalCoverageRatio()
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

        if (isTotal) {
          if (col === 'Inventory at Month End') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'Dispatch') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'Current Inventory + Dispatch') {
            obj[col] = calculateColumnTotal(col).toLocaleString('en-US')
            return
          }

          if (col === 'Inventory Coverage Ratio Before Dispatch') {
            obj[col] = calculateTotalCoverageRatio().toLocaleString('en-US', {
              minimumFractionDigits: 2,
              maximumFractionDigits: 2,
            })
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

      return obj
    })
  }, [skuData, displayedColumns, showAllDispatchRows])

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
      header:
        col === 'Inventory Coverage Ratio Before Dispatch'
          ? 'Coverage Ratio Before Dispatch'
          : col,
      width: isSNo
        ? '55px'
        : isProduct
          ? '190px'
          : isSku
            ? '260px'
            : isCoverage
              ? '190px'
              : isInventoryMonthEnd
                ? '165px'
                : isDispatch
                  ? '110px'
                  : isCurrentInventoryDispatch
                    ? '200px'
                    : '150px',
      cellClassName:
        isSku
          ? 'dispatch-sku-cell'
          : isProduct
            ? 'text-left'
            : isSNo
              ? 'text-center'
              : 'text-center',
      headerClassName: 'text-center whitespace-normal break-words',
    }
  })

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
    min-width: 1140px;
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
                className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-gray-300 bg-white text-blue-700 transition-all duration-200 ease-out hover:-translate-y-[2px] hover:shadow-lg active:translate-y-0 active:shadow-md disabled:cursor-not-allowed disabled:opacity-50 lg:h-9 lg:w-9"
              >
                {showAllDispatchRows ? (
                  <RiCollapseDiagonalFill className="h-4 w-4 font-extrabold lg:h-[18px] lg:w-[18px]" />
                ) : (
                  <RiExpandDiagonalFill className="h-4 w-4 font-extrabold lg:h-[18px] lg:w-[18px]" />
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

          <div className="forecast-data">
            <div className="forecast-data">
              <DataTable
                columns={columns}
                data={tableRows}
                paginate={false}
                scrollY={false}
                maxHeight="none"
                stickyHeader
                loading={loading}
                emptyMessage={
                  noData
                    ? "No Data Available for selected period"
                    : "Select Month and Year to see Dispatch!"
                }
                rowClassName={(row: any) =>
                  row.__isTotal
                    ? "bg-[#EFEFEF] font-semibold"
                    : row.__isOthers
                      ? showAllDispatchRows
                        ? ""
                        : "cursor-pointer"
                      : ""
                }
                onRowClick={(row: any) => {
                  if (!showAllDispatchRows && row.__isOthers) {
                    setShowAllDispatchRows(true);
                  }
                }}
                isTotalRow={(row: any) => !!row.__isTotal}
                bodyMaxHeight={
                  showAllDispatchRows &&
                    tableRows.filter((row: any) => !row.__isTotal).length > 15
                    ? 40 * 15
                    : undefined
                }
                tableClassName="text-xs 2xl:text-sm [&_th]:whitespace-normal [&_th]:break-words [&_th]:text-center [&_th]:py-3"
              />
            </div>
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
