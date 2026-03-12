'use client'

import React, { useEffect, useMemo, useState } from 'react'
import { useParams, useRouter } from 'next/navigation'
import * as XLSX from 'xlsx'
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css';
import { Modal } from '@/components/ui/modal'; // Adjust path as needed
import FileUploadForm from '@/app/(admin)/(ui-elements)/modals/FileUploadForm'; // Adjust path as needed
import MonthYearPickerTable from '@/components/filters/MonthYearPickerTable';
import DataTable, { ColumnDef } from "@/components/ui/table/DataTable"
import { IoDownload } from "react-icons/io5";
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import PageBreadcrumb from "@/components/common/PageBreadCrumb";



// Types
interface SkuRow {
  [key: string]: string | number | undefined
  sku?: string
  'Product Name'?: string
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

export default function DispatchPage() {
  const params = useParams<{ countryName: string; month: string; year: string }>()
  const router = useRouter()

  const countryName = useMemo(() => (params?.countryName ?? '').toString(), [params])
  const month = useMemo(() => (params?.month ?? '').toString(), [params])
  const year = useMemo(() => (params?.year ?? '').toString(), [params])

  const [monthdp, setMonthDp] = useState<string>(getCurrentMonthPlus1())
  const [yeardp, setYearDp] = useState<string>(getCurrentYear())
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [skuData, setSkuData] = useState<SkuRow[]>([])
  const [showForecastMessage, setShowForecastMessage] = useState(false)
  const [isInitialized, setIsInitialized] = useState(false)
  const [showUpload, setShowUpload] = useState(false);

  const monthdps = monthNames as unknown as string[]
  const yeardps = useMemo(() => Array.from({ length: 3 }, (_, i) => new Date().getFullYear() - i), [])

  async function fetchDispatchFile(monthdpValue: string, yeardpValue: string) {
    if (!monthdpValue || !yeardpValue) {
      setError('Please select both month and year.')
      return
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null
    if (!token) {
      setError('Authorization token is missing')
      return
    }

    setLoading(true)
    setError('')
    setShowForecastMessage(false)

    try {
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/getDispatchfile?country=${countryName}&month=${monthdpValue}&year=${yeardpValue}`,
        {
          method: 'GET',
          headers: {
            Authorization: ` Bearer ${token}`,
          },
          // cache: 'no-store', // uncomment if the result changes frequently
        },
      )

      if (!response.ok) {
        // Try to parse error body; if not JSON, fall back
        let errorData: any = {}
        try {
          errorData = await response.json()
        } catch {}

        setShowForecastMessage(true)

        if (errorData?.error && String(errorData.error).includes('Forecast file not found')) {
          setShowForecastMessage(true)
          setError('')
        } else {
          throw new Error(errorData?.error || 'Failed to fetch dispatch file')
        }
        return
      }

      const blob = await response.blob()
      const data = await blob.arrayBuffer()
      const workbook = XLSX.read(new Uint8Array(data), { type: 'array' })
      const sheetName = workbook.SheetNames[0]
      const sheet = workbook.Sheets[sheetName]
      const jsonData: SkuRow[] = XLSX.utils.sheet_to_json(sheet)
      setSkuData(jsonData)
    } catch (err: any) {
      console.error('Fetch error:', err)
      setError(err?.message ?? 'Unknown error')
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
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [month, year])

  useEffect(() => {
    if (isInitialized && monthdp && yeardp) {
      void fetchDispatchFile(monthdp, yeardp)
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [isInitialized, monthdp, yeardp])

  function isTotalRow(row: SkuRow) {
    return (
      row['Product Name'] === 'Total' ||
      (row.sku && row.sku.toLowerCase() === 'total') ||
      (row['Product Name'] && String(row['Product Name']).toLowerCase() === 'total')
    )
  }

  function calculateColumnTotal(columnName: string) {
    return skuData
      .filter((row) => !isTotalRow(row))
      .reduce((sum, row) => {
        const value = row[columnName]
        return sum + (typeof value === 'number' ? value : 0)
      }, 0)
  }

  const displayedColumns = (countryName || '').toLowerCase() === 'global'
    ? [
        'Sno.',
        'Product Name',
        'Inventory at Month End',
        'Projected Sales Total',
        'Dispatch',
        'Current Inventory + Dispatch',
        'Inventory Coverage Ratio Before Dispatch',
      ]
    : [
        'Sno.',
        'Product Name',
        'Inventory at Month End',
        'Projected Sales Total',
        'Dispatch',
        'Current Inventory + Dispatch',
        'Inventory Coverage Ratio Before Dispatch',
      ]

  function handleExportToExcel() {
    const worksheetData = skuData.map((row, index) => {
      const formattedRow: Record<string, string | number> = { 'Sno.': isTotalRow(row) ? '' : index + 1 }
      displayedColumns.forEach((col) => {
        if (col !== 'Sno.') {
          if (col === 'sku' && isTotalRow(row)) {
            formattedRow[col] = ''
          } else if (
            isTotalRow(row) &&
            [
              'Inventory at Month End',
              'Projected Sales Total',
              'Dispatch',
              'Current Inventory + Dispatch',
              'Inventory Coverage Ratio Before Dispatch',
            ].includes(col)
          ) {
            formattedRow[col] = calculateColumnTotal(col)
          } else {
            const v = row[col]
            formattedRow[col] = (typeof v === 'number' || typeof v === 'string') ? v : ''
          }
        }
      })
      return formattedRow
    })

    const worksheet = XLSX.utils.json_to_sheet(worksheetData)

    // Number formatting
    const numericColumns = [
      'Inventory at Month End',
      'Projected Sales Total',
      'Dispatch',
      'Current Inventory + Dispatch',
      'Inventory Coverage Ratio Before Dispatch',
    ]
    if (worksheet['!ref']) {
      const range = XLSX.utils.decode_range(worksheet['!ref'])
      for (let R = range.s.r; R <= range.e.r; ++R) {
        for (let C = range.s.c; C <= range.e.c; ++C) {
          const cellAddress = XLSX.utils.encode_cell({ r: R, c: C })
          const cell = (worksheet as any)[cellAddress]
          if (cell && typeof cell.v === 'number') {
            cell.z = '#,##0'
          }
        }
      }
    }

    const workbook = XLSX.utils.book_new()
    XLSX.utils.book_append_sheet(workbook, worksheet, 'Dispatch')
    XLSX.writeFile(workbook, `Dispatch Report ${monthdp}-${yeardp}.xlsx`)
  }

  // ---- build rows for DataTable ----
const tableRows = skuData.map((row, index) => {
  const isTotal = isTotalRow(row)

  const obj: Record<string, any> = {
    __isTotal: isTotal,
    sno: isTotal ? "" : index + 1,
  }

  displayedColumns.forEach((col) => {
    if (col === "Sno.") return

    if (
      isTotal &&
      [
        "Inventory at Month End",
        "Projected Sales Total",
        "Dispatch",
        "Current Inventory + Dispatch",
        "Inventory Coverage Ratio Before Dispatch",
      ].includes(col)
    ) {
      obj[col] = calculateColumnTotal(col).toLocaleString("en-US")
    }else {
  // ✅ Fix Total row text duplication
  if (isTotal) {
    if (col === "sku") {
      obj[col] = "Total"                // SKU blank
      return
    }
    if (col === "Product Name") {
      obj[col] = ""           // Total only here
      return
    }
  }

  const v = row[col]
  obj[col] =
    typeof v === "number"
      ? v.toLocaleString("en-US")
      : v ?? ""
}
  })

  return obj
})

const columns: ColumnDef<any>[] = displayedColumns.map((col) => {
  const isCoverage =
    col === "Inventory Coverage Ratio Before Dispatch";

  return {
    key: col === "Sno." ? "sno" : col,

    header: isCoverage ? (
      <div
        className="one-line-ellipsis"
        title={col}
      >
        {col}
      </div>
    ) : (
      col
    ),

    width:
      isCoverage ? "200px" : col === "Sno." ? "60px" : undefined,

    cellClassName:
      col === "Product Name"
        ? "text-left whitespace-nowrap"
        : isCoverage
        ? "text-center whitespace-nowrap"
        : col === "Sno."
        ? "text-center whitespace-nowrap"
        : "text-center",
  };
});



  return (
    <>
      <style jsx>{`
        /* ---------- Layout & Flex Containers ---------- */
        .inline-dropdowns {
          display: flex;
          flex-wrap: nowrap;
          gap: 0.5vw;
          align-items: center;
          justify-content: flex-start;
          margin-bottom: 3vh;
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

        /* ---------- Table Styles ---------- */
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

        /* ---------- Main Data Table Styles (Enhanced for Better Application) ---------- */
        .tablec tbody tr:last-child {
  background-color: #ccc !important;
  color: #414042;
  text-align: center;
  font-weight: bold;
}
        .tablec td:first-child, .tablec th:first-child { text-align: center; width: 19px; }
        .tablec thead th {
  background-color: #5EA68E !important;
  color: #f8edcf !important;
  font-weight: bold !important;
  text-align: center !important;
  font-size: clamp(12px, 0.729vw, 16px) !important;
}
  .tablec tbody tr:nth-child(even) { background-color: #5EA68E33; }
        .tablec tbody tr:nth-child(odd) { background-color: #ffffff; }

        /* ---------- Select Dropdowns ---------- */
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
          color: #4104042;
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
          width: 34%;
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
      `}</style>
      <div className='flex justify-between items-start'>
         <div className="flex flex-wrap items-baseline gap-2 justify-start">
              <PageBreadcrumb
                pageTitle="Dispatch Report - "
                variant="page"
                align="left"
                className=""
              />
              <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
                Amazon {countryName?.toLowerCase() === "global"
                  ? "Global"
                  : countryName?.toUpperCase()}
              </span>
            </div>
 
     <div className="inline-dropdowns">
      <MonthYearPickerTable
                      month={month}
                      year={year}
                      yearOptions={[
                        new Date().getFullYear(),
                        new Date().getFullYear() - 1,
                      ]}
                      onMonthChange={(v) => setMonthDp(v)}
                      onYearChange={(v) => setYearDp(v)}
                      valueMode="lower"
                    />

      <div className="centralised-fetch-button">
        <button className="fetch-button" onClick={() => fetchDispatchFile(monthdp, yeardp)}>
          Get Report
        </button>
      </div>
      <DownloadIconButton onClick={handleExportToExcel} size="md" />
    </div>
      </div>
    

   

    {loading ? (
      <div className="loading-wrapper">
        <p>Loading...</p>
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
    ) : showForecastMessage ? (
      <div className="forecast-banner lg:w-[40%]  w-full">
        <i className="fa-solid fa-circle-exclamation"></i>
        <span>Run the Inventory Forecast to view dispatch reports.</span>
        <button className="forecast-action" onClick={handleRedirectToForecast}>
          Run Now <i className="fa-solid fa-chevron-right"></i>
        </button>
      </div>
    ) : (
      <div className="forecast-data">
        {skuData.length > 0 ? (
          <>
           <DataTable
  columns={columns}
  data={tableRows}
  paginate={false}          // ❗ pagination OFF so Total row stays visible
  scrollY
  maxHeight="90vh"
  stickyHeader
  loading={loading}
  rowClassName={(row: any) =>
    row.__isTotal ? "bg-[#D9D9D9] font-bold" : ""
  }
/>
    
            
          </>
        ) : (
          <p>Select Month and Year to see Dispatch!</p>
        )}
      </div>
    )}

    <Modal
      isOpen={showUpload}
      onClose={() => setShowUpload(false)}
      showCloseButton
      className="max-w-4xl w-full mx-auto p-0"
    >
      <FileUploadForm initialCountry={''} onClose={function (): void {
        throw new Error('Function not implemented.');
      } } onComplete={function (): void {
        throw new Error('Function not implemented.');
      } } />
    </Modal>
  </>
)}