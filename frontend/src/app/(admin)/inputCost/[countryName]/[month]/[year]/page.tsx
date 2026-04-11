// 'use client';

// import React, { use, useEffect, useMemo, useState } from 'react';
// import * as XLSX from 'xlsx';
// import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css';
// import Modalmsg from '@/components/ui/modal/Modalmsg';
// import SkuMultiuseCountryUpload from '@/components/ui/modal/SkuMultiCountryUpload';
// import { IoDownload } from "react-icons/io5";
// import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
// import PageBreadcrumb from "@/components/common/PageBreadCrumb";
// import SegmentedToggle from '@/components/ui/SegmentedToggle';
// import DownloadIconButton from '@/components/ui/button/DownloadButton';
// import { useRouter } from 'next/navigation';
// import { IoMdLock } from "react-icons/io";

// // Types
// interface Params {
//   params: Promise<{
//     countryName: string;
//     month: string;
//     year: string;
//   }>;
// }

// interface SkuRow {
//   s_no: number;
//   product_name: string;
//   sku_uk?: string;
//   sku_us?: string;
//   sku_canada?: string;
//   asin?: string;
//   product_barcode?: string;
//   price?: number;
//   currency?: string;
//   month?: string;
//   year?: string | number;
//   // ✅ ADD THESE
//   local_stock?: number;
//   in_transit_units?: number;
//   [key: string]: any;
// }

// type TableRow = {
//   id: string;
//   s_no: React.ReactNode;
//   product_name: React.ReactNode;
//   sku_uk?: React.ReactNode;
//   sku_us?: React.ReactNode;
//   sku_canada?: React.ReactNode;
//   asin?: React.ReactNode;
//   product_barcode?: React.ReactNode;
//   month_year?: React.ReactNode;
//   price?: React.ReactNode;
//   gross_margin_uk?: React.ReactNode;
//   gross_margin_us?: React.ReactNode;
//   gross_margin_canada?: React.ReactNode;
//   gross_margin_eu?: React.ReactNode;
//   gross_margin_europe?: React.ReactNode;
//   gross_margin_global?: React.ReactNode;
//   isOthersRow?: boolean;
//   [key: string]: React.ReactNode;
// };

// const getCurrencySymbol = (country: string | undefined): string => {
//   switch (country) {
//     case 'GBP':
//       return '£';
//     case 'INR':
//       return '₹';
//     case 'USD':
//       return '$';
//     case 'europe':
//     case 'eu':
//     case 'EUR':
//       return '€';
//     case 'CAD':
//       return '$';
//     case 'global':
//       return '$';
//     default:
//       return '$';
//   }
// };

// function getCurrencyForCountry(country: string): string {
//   switch (country.toLowerCase()) {
//     case 'uk':
//       return 'GBP';
//     case 'us':
//       return 'USD';
//     case 'canada':
//       return 'CAD';
//     case 'eu':
//     case 'europe':
//       return 'EUR';
//     default:
//       return 'USD';
//   }
// }

// const isPreviewNA = (month: string, year: string) =>
//   month?.toLowerCase() === 'na' || year?.toLowerCase() === 'na';

// // ✅ Warehouse format config
// const WAREHOUSE_FIXED_COLUMNS = [
//   "s_no",
//   "sku_us",
//   "sku_uk",
//   "local_stock",
//   "in_transit_units",
//   "month",
//   "year",
// ];

// const normalizeWarehouseHeader = (key: string) => {
//   const raw = key.trim().toLowerCase().replace(/\s+/g, "_");

//   const map: Record<string, string> = {
//     "s.no": "s_no",
//     "s.no.": "s_no",
//     "serial_no": "s_no",

//     "sku_us": "sku_us",
//     "sku_uk": "sku_uk",

//     "stock": "local_stock",
//     "local_stock": "local_stock",

//     "in_transit": "in_transit_units",
//     "transit_units": "in_transit_units",

//     "month": "month",
//     "year": "year",
//   };

//   return map[raw] || raw;
// };

// const validateWarehouseHeaders = (row: Record<string, any>) => {
//   const normalized = Object.keys(row).map(normalizeWarehouseHeader);

//   const missing = WAREHOUSE_FIXED_COLUMNS.filter(
//     (col) => !normalized.includes(col)
//   );

//   if (missing.length) {
//     return `Missing columns: ${missing.join(", ")}`;
//   }

//   return "";
// };

// const DUMMY_SKU_DATA: SkuRow[] = [
//   {
//     s_no: 1,
//     product_name: 'Sample Product A',
//     sku_uk: 'UK-SKU-001',
//     sku_us: 'US-SKU-001',
//     sku_canada: 'CA-SKU-001',
//     asin: 'B0DUMMY001',
//     product_barcode: '1234567890123',
//     price: 12.5,
//     currency: 'GBP',
//     month: 'January',
//     year: '2026',
//   },
//   {
//     s_no: 2,
//     product_name: 'Sample Product B',
//     sku_uk: 'UK-SKU-002',
//     sku_us: 'US-SKU-002',
//     sku_canada: 'CA-SKU-002',
//     asin: 'B0DUMMY002',
//     product_barcode: '2234567890123',
//     price: 18.75,
//     currency: 'GBP',
//     month: 'January',
//     year: '2026',
//   },
//   {
//     s_no: 3,
//     product_name: 'Sample Product C',
//     sku_uk: 'UK-SKU-003',
//     sku_us: 'US-SKU-003',
//     sku_canada: 'CA-SKU-003',
//     asin: 'B0DUMMY003',
//     product_barcode: '3234567890123',
//     price: 9.99,
//     currency: 'GBP',
//     month: 'January',
//     year: '2026',
//   },
// ];

// const DUMMY_ASP_DATA: Record<string, number> = {
//   'Sample Product A': 25,
//   'Sample Product B': 34,
//   'Sample Product C': 16,
//   'Sample Product A_uk': 25,
//   'Sample Product B_uk': 34,
//   'Sample Product C_uk': 16,
//   'Sample Product A_us': 31,
//   'Sample Product B_us': 42,
//   'Sample Product C_us': 20,
//   'Sample Product A_canada': 38,
//   'Sample Product B_canada': 49,
//   'Sample Product C_canada': 24,
// };

// const DUMMY_CURRENCY_RATES: Record<string, number> = {
//   GBP: 1,
//   USD: 1,
//   CAD: 1,
//   EUR: 1,
//   INR: 1,

//   gbp: 1,
//   usd: 1,
//   cad: 1,
//   eur: 1,
//   inr: 1,

//   GBP_uk: 1,
//   USD_us: 1,
//   CAD_canada: 1,
//   EUR_europe: 1,
//   EUR_eu: 1,
//   USD_global: 1,
// };

// const DUMMY_WAREHOUSE_DATA = [
//   {
//     s_no: 1,
//     sku_us: 'US-SKU-001',
//     sku_uk: 'UK-SKU-001',
//     local_stock: 120,
//     in_transit_units: 15,
//     month: 'January',
//     year: '2026',
//   },
//   {
//     s_no: 2,
//     sku_us: 'US-SKU-002',
//     sku_uk: 'UK-SKU-002',
//     local_stock: 85,
//     in_transit_units: 9,
//     month: 'January',
//     year: '2026',
//   },
// ];

// export default function InputCostPage({ params }: Params) {
//   const { countryName: countryNameRaw, month: monthRaw, year: yearRaw } = use(params);
//   const countryName = decodeURIComponent(countryNameRaw ?? '').toLowerCase();
//   const monthParam = decodeURIComponent(monthRaw ?? '');
//   const yearParam = decodeURIComponent(yearRaw ?? '');
//   // const isNA = isPreviewNA(monthParam, yearParam);
//   const [skuData, setSkuData] = useState<SkuRow[]>([]);
//   const [loading, setLoading] = useState(true);
//   const [error, setError] = useState<string | null>(null);
//   const [isEditing, setIsEditing] = useState(false);
//   const [editedPrices, setEditedPrices] = useState<Record<string, number>>({});
//   const [showModal, setShowModal] = useState(false);
//   const [modalMessage, setModalMessage] = useState('');
//   const [visibleColumns, setVisibleColumns] = useState<string[]>([]);
//   const [currencyRates, setCurrencyRates] = useState<Record<string, number>>({});
//   const [aspData, setAspData] = useState<Record<string, number>>({});
//   const [showMultiuseCountry, setShowMultiuseCountry] = useState(false);
//   const [showAllRows, setShowAllRows] = useState(false);
//   const [warehouseData, setWarehouseData] = useState<Record<string, any>[]>([]);
//   const [warehouseColumns, setWarehouseColumns] = useState<string[]>([]);
//   const [warehouseLoading, setWarehouseLoading] = useState(false);
//   const [showWarehouseUpload, setShowWarehouseUpload] = useState(false);
//   const [selectedWarehouseFile, setSelectedWarehouseFile] = useState<File | null>(null);

//   const router = useRouter();

//   const isNA =
//     monthParam?.toLowerCase() === 'na' ||
//     yearParam?.toLowerCase() === 'na';

//   type InputCostTab = 'sku-info' | 'extra';
//   const [activeTab, setActiveTab] = useState<InputCostTab>('sku-info');

//   const isColumnEmpty = (data: SkuRow[], columnName: string) => {
//     return data.every((row) => {
//       const value = row[columnName];
//       return (
//         value === null ||
//         value === undefined ||
//         value === '' ||
//         (typeof value === 'string' && value.trim() === '')
//       );
//     });
//   };

//   // 1) REMOVE these from SKU visible columns
//   const getVisibleColumns = (data: SkuRow[]) => {
//     if (!data || data.length === 0) return [] as string[];

//     const baseColumns: string[] = ['s_no', 'product_name'];
//     let skuColumns: string[] = [];
//     let grossMarginColumns: string[] = [];

//     if (countryName === 'global') {
//       const potentialSkuColumns = ['sku_uk', 'sku_us', 'sku_canada'];
//       skuColumns = potentialSkuColumns.filter((col) => !isColumnEmpty(data, col));
//       skuColumns.forEach((skuCol) => {
//         const c = skuCol.replace('sku_', '');
//         grossMarginColumns.push(`gross_margin_${c}`);
//       });
//     } else {
//       const skuColumn = `sku_${countryName}`;
//       if (!isColumnEmpty(data, skuColumn)) {
//         skuColumns.push(skuColumn);
//       }
//       grossMarginColumns.push(`gross_margin_${countryName}`);
//     }

//     const otherColumns = [
//       'asin',
//       'product_barcode',
//       'month_year',
//       'price',
//     ];

//     const visibleOtherColumns = otherColumns.filter((col) => {
//       if (col === 'month_year') {
//         return data.some((row) => row.month || row.year);
//       }
//       return !isColumnEmpty(data, col);
//     });

//     return [...baseColumns, ...skuColumns, ...visibleOtherColumns, ...grossMarginColumns];
//   };

//   const getColumnDisplayName = (column: string): React.ReactNode => {
//     switch (column) {
//       case 's_no':
//         return 'Sno.';
//       case 'product_name':
//         return 'Product Name';
//       case 'sku_uk':
//         return 'SKU (UK)';
//       case 'sku_us':
//         return 'SKU (US)';
//       case 'sku_canada':
//         return 'SKU (CANADA)';
//       case 'asin':
//         return 'ASIN';
//       case 'product_barcode':
//         return 'Product Barcode';
//       case 'month_year':
//         return 'Month / Year';
//       case 'price':
//         return 'Landing Cost';
//       case 'local_stock':
//         return 'Local Stock';

//       case 'in_transit_units':
//         return 'In Transit Units';
//       default:
//         if (column.startsWith('sku_')) {
//           const c = column.replace('sku_', '').toUpperCase();
//           return `SKU (${c})`;
//         }
//         if (column.startsWith('gross_margin_')) {
//           const c = column.replace('gross_margin_', '').toUpperCase();
//           return (
//             <>
//               {`Gross Margin (%) ${c} `}
//               <span
//                 style={{ position: 'relative', cursor: 'pointer' }}
//                 title="*Gross Margin calculation is based on previous month’s ASP"
//               >
//                 &nbsp;<i className="fa-solid fa-circle-info" style={{ color: '#f8edcf' }}></i>
//               </span>
//             </>
//           );
//         }
//         return column.charAt(0).toUpperCase() + column.slice(1);
//     }
//   };

//   const getCurrencyRate = (currency: string | undefined, country: string) => {
//     if (!currency || !currencyRates || Object.keys(currencyRates).length === 0) return 1;
//     const possibleKeys = [
//       `${currency}_${country}`,
//       `${currency.toLowerCase()}_${country.toLowerCase()}`,
//       `${currency.toUpperCase()}_${country.toLowerCase()}`,
//       currency,
//       currency.toLowerCase(),
//       currency.toUpperCase(),
//     ];
//     for (const key of possibleKeys) {
//       if (currencyRates[key] !== undefined) return currencyRates[key];
//     }
//     return 1;
//   };

//   const getAspForProduct = (productName: string, targetCountry: string | null = null) => {
//     if (!aspData || Object.keys(aspData).length === 0) return null;

//     if (targetCountry && countryName === 'global') {
//       const countrySpecificKey = `${productName}_${targetCountry}`;
//       if (aspData[countrySpecificKey] !== undefined) return aspData[countrySpecificKey];
//       for (const key in aspData) {
//         if (key.includes(`_${targetCountry}`) && key.includes(productName)) return aspData[key];
//       }
//     }

//     if (countryName === 'global') {
//       if (aspData[productName] !== undefined) return aspData[productName];
//       for (const key in aspData) {
//         if (key.includes(productName) || productName.includes(key)) return aspData[key];
//       }
//     } else {
//       return aspData[productName] ?? null;
//     }

//     return null;
//   };

//   const calculateGrossMargin = (
//     price: number | undefined,
//     sourceCurrency: string | undefined,
//     targetCountry: string,
//     productName: string
//   ): string => {
//     try {
//       const asp = getAspForProduct(productName, targetCountry);
//       if (!price || !asp || asp === 0) return 'N/A';

//       let convertedPrice: number;
//       if (countryName === 'global') {
//         const targetCurrency = getCurrencyForCountry(targetCountry);
//         if (sourceCurrency === targetCurrency) {
//           convertedPrice = price;
//         } else {
//           const sourceToUsdRate = getCurrencyRate(sourceCurrency, 'global') || 1;
//           const usdToTargetRate = getCurrencyRate(targetCurrency, targetCountry) || 1;
//           convertedPrice = price * sourceToUsdRate * usdToTargetRate;
//         }
//       } else {
//         const currencyRate = getCurrencyRate(sourceCurrency, targetCountry);
//         convertedPrice = price * currencyRate;
//       }

//       const grossMargin = ((asp - convertedPrice) / asp) * 100;
//       return grossMargin.toFixed(2);
//     } catch {
//       return 'N/A';
//     }
//   };

//   // const fetchCurrencyRates = async () => {
//   //   const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
//   //   if (!token) return;

//   //   try {
//   //     const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/currency-rates`, {
//   //       method: 'GET',
//   //       headers: { Authorization: `Bearer ${token}` },
//   //     });
//   //     if (response.ok) {
//   //       const rates: Array<{ user_currency: string; country: string; conversion_rate: number }> =
//   //         await response.json();
//   //       const map: Record<string, number> = {};
//   //       rates.forEach((rate) => {
//   //         const keys = [
//   //           `${rate.user_currency}_${rate.country}`,
//   //           `${rate.user_currency.toLowerCase()}_${rate.country.toLowerCase()}`,
//   //           `${rate.user_currency.toUpperCase()}_${rate.country.toLowerCase()}`,
//   //           rate.user_currency,
//   //           rate.user_currency.toLowerCase(),
//   //           rate.user_currency.toUpperCase(),
//   //         ];
//   //         keys.forEach((k) => (map[k] = rate.conversion_rate));
//   //       });
//   //       setCurrencyRates(map);
//   //     }
//   //   } catch (e) {
//   //     console.error('Error fetching currency rates', e);
//   //   }
//   // };

//   const fetchCurrencyRates = async () => {
//     if (isNA) {
//       setCurrencyRates(DUMMY_CURRENCY_RATES);
//       return;
//     }

//     const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
//     if (!token) return;

//     try {
//       const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/currency-rates`, {
//         method: 'GET',
//         headers: { Authorization: `Bearer ${token}` },
//       });
//       if (response.ok) {
//         const rates: Array<{ user_currency: string; country: string; conversion_rate: number }> =
//           await response.json();
//         const map: Record<string, number> = {};
//         rates.forEach((rate) => {
//           const keys = [
//             `${rate.user_currency}_${rate.country}`,
//             `${rate.user_currency.toLowerCase()}_${rate.country.toLowerCase()}`,
//             `${rate.user_currency.toUpperCase()}_${rate.country.toLowerCase()}`,
//             rate.user_currency,
//             rate.user_currency.toLowerCase(),
//             rate.user_currency.toUpperCase(),
//           ];
//           keys.forEach((k) => (map[k] = rate.conversion_rate));
//         });
//         setCurrencyRates(map);
//       }
//     } catch (e) {
//       console.error('Error fetching currency rates', e);
//     }
//   };

//   const fetchAspData = async () => {
//     if (isNA) {
//       setAspData(DUMMY_ASP_DATA);
//       return;
//     }

//     const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
//     if (!token) return;

//     const monthNames = [
//       'january',
//       'february',
//       'march',
//       'april',
//       'may',
//       'june',
//       'july',
//       'august',
//       'september',
//       'october',
//       'november',
//       'december',
//     ];

//     const normalizedMonth = (() => {
//       const m = monthParam.toLowerCase();
//       if (/^\d+$/.test(m)) {
//         const idx = Math.min(Math.max(parseInt(m, 10) - 1, 0), 11);
//         return monthNames[idx];
//       }
//       return monthNames.includes(m) ? m : monthNames[new Date().getMonth()];
//     })();

//     const normalizedYear = (() => {
//       const y = parseInt(yearParam, 10);
//       if (!isNaN(y) && y > 2000 && y < 2100) return y;
//       return new Date().getFullYear();
//     })();

//     try {
//       let currentMonthIndex = monthNames.indexOf(normalizedMonth);
//       let currentYear = normalizedYear;

//       for (let attempt = 0; attempt < 12; attempt++) {
//         const monthName = monthNames[currentMonthIndex];
//         try {
//           const response = await fetch(
//             `${process.env.NEXT_PUBLIC_API_BASE_URL}/asp-data?country=${countryName}&month=${monthName}&year=${currentYear}`,
//             {
//               method: 'GET',
//               headers: { Authorization: `Bearer ${token}` },
//             }
//           );
//           if (response.ok) {
//             const aspArray: Array<{ product_name: string; asp: number; source_country?: string }> =
//               await response.json();
//             const map: Record<string, number> = {};
//             aspArray.forEach((item) => {
//               map[item.product_name] = item.asp;
//             });
//             setAspData(map);
//             return;
//           }
//         } catch {
//           // continue
//         }
//         currentMonthIndex--;
//         if (currentMonthIndex < 0) {
//           currentMonthIndex = 11;
//           currentYear--;
//         }
//       }
//       setAspData({});
//     } catch (e) {
//       console.error('Error in fetchAspData', e);
//       setAspData({});
//     }
//   };

//   useEffect(() => {
//     const fetchSkuData = async () => {
//       if (isNA) {
//         const previewCurrency =
//           countryName === 'uk'
//             ? 'GBP'
//             : countryName === 'us'
//               ? 'USD'
//               : countryName === 'canada'
//                 ? 'CAD'
//                 : countryName === 'eu' || countryName === 'europe'
//                   ? 'EUR'
//                   : 'USD';

//         const previewRows = DUMMY_SKU_DATA.map((row) => ({
//           ...row,
//           currency: countryName === 'global' ? row.currency : previewCurrency,
//           month: 'January',
//           year: '2026',
//         }));

//         const sorted = [...previewRows].sort((a, b) => (a.s_no ?? 0) - (b.s_no ?? 0));
//         setSkuData(sorted);
//         setVisibleColumns(getVisibleColumns(sorted));
//         setLoading(false);
//         setError(null);
//         return;
//       }

//       const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
//       if (!token) {
//         setError('Authorization token is missing');
//         setLoading(false);
//         return;
//       }

//       try {
//         const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/skuprice`, {
//           method: 'GET',
//           headers: { Authorization: `Bearer ${token}` },
//         });
//         if (!response.ok) throw new Error('Failed to fetch data');
//         const data: SkuRow[] = await response.json();
//         const sorted = [...data].sort((a, b) => (a.s_no ?? 0) - (b.s_no ?? 0));
//         setSkuData(sorted);
//         const columns = getVisibleColumns(sorted);
//         setVisibleColumns(columns);
//       } catch (err: any) {
//         setError(err.message);
//       } finally {
//         setLoading(false);
//       }
//     };

//     fetchSkuData();
//     fetchCurrencyRates();
//     fetchAspData();
//     // eslint-disable-next-line react-hooks/exhaustive-deps
//   }, [countryName, monthParam, yearParam, isNA]);

//   const handlePriceChange = (productName: string, value: string) => {
//     setEditedPrices((prev) => ({
//       ...prev,
//       [productName]: value === '' ? NaN : parseFloat(value),
//     }));
//   };

//   const saveChanges = async () => {
//     const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
//     if (!token) {
//       alert('Authorization token is missing');
//       return;
//     }
//     if (Object.keys(editedPrices).length === 0) {
//       alert('No changes to save.');
//       return;
//     }
//     try {
//       const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/updatePrices`, {
//         method: 'POST',
//         headers: {
//           'Content-Type': 'application/json',
//           Authorization: `Bearer ${token}`,
//         },
//         body: JSON.stringify({ prices: editedPrices }),
//       });
//       if (response.ok) {
//         const result = await response.json();
//         setModalMessage('Prices updated successfully');
//         setShowModal(true);
//         setIsEditing(false);
//         setEditedPrices({});
//         if (result.data) {
//           const sortedData: SkuRow[] = result.data.sort((a: SkuRow, b: SkuRow) => (a.s_no ?? 0) - (b.s_no ?? 0));
//           setSkuData(sortedData);
//           const columns = getVisibleColumns(sortedData);
//           setVisibleColumns(columns);
//         } else {
//           window.location.reload();
//         }
//       } else {
//         const errorData = await response.json();
//         throw new Error(errorData.error || 'Failed to update prices');
//       }
//     } catch (e: any) {
//       alert(`Error: ${e.message}`);
//       console.error('Update prices error:', e);
//     }
//   };

//   const fetchWarehouseData = async () => {
//     if (isNA) {
//       setWarehouseData(DUMMY_WAREHOUSE_DATA);
//       setWarehouseColumns(getOrderedWarehouseColumns(Object.keys(DUMMY_WAREHOUSE_DATA[0] || {})));
//       setWarehouseLoading(false);
//       return;
//     }

//     const token =
//       typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

//     // fallback from skuData
//     const skuFallbackRows = skuData
//       .filter((row) => row.local_stock !== undefined || row.in_transit_units !== undefined)
//       .map((row, index) => ({
//         s_no: row.s_no ?? index + 1,
//         sku_us: row.sku_us ?? '',
//         sku_uk: row.sku_uk ?? '',
//         local_stock: row.local_stock ?? '',
//         in_transit_units: row.in_transit_units ?? '',
//         month: row.month ?? '',
//         year: row.year ?? '',
//       }));

//     if (!token) {
//       setWarehouseData(skuFallbackRows);
//       setWarehouseColumns(
//         skuFallbackRows.length > 0
//           ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
//           : []
//       );
//       return;
//     }

//     try {
//       setWarehouseLoading(true);

//       const response = await fetch(
//         `${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData?country=${countryName}`,
//         {
//           method: 'GET',
//           headers: {
//             Authorization: `Bearer ${token}`,
//           },
//         }
//       );

//       const result = await response.json().catch(() => ({}));

//       if (!response.ok) {
//         setWarehouseData(skuFallbackRows);
//         setWarehouseColumns(
//           skuFallbackRows.length > 0
//             ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
//             : []
//         );
//         return;
//       }

//       const rows = Array.isArray(result?.data) ? result.data : [];

//       if (rows.length > 0) {
//         const cols = Array.isArray(result?.columns)
//           ? result.columns
//           : Object.keys(rows[0]);

//         setWarehouseData(rows);
//         setWarehouseColumns(getOrderedWarehouseColumns(cols));
//       } else {
//         setWarehouseData(skuFallbackRows);
//         setWarehouseColumns(
//           skuFallbackRows.length > 0
//             ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
//             : []
//         );
//       }
//     } catch (e) {
//       console.error('Failed to fetch warehouse data', e);

//       setWarehouseData(skuFallbackRows);
//       setWarehouseColumns(
//         skuFallbackRows.length > 0
//           ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
//           : []
//       );
//     } finally {
//       setWarehouseLoading(false);
//     }
//   };

//   const handleWarehouseUpload = async (file: File) => {
//     if (isNA) {
//       setModalMessage('Preview mode only. Connect your account to upload warehouse data.');
//       setShowModal(true);
//       return;
//     }

//     const token =
//       typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

//     if (!token) {
//       setModalMessage('Authorization token is missing');
//       setShowModal(true);
//       return;
//     }

//     try {
//       setWarehouseLoading(true);

//       const formData = new FormData();
//       formData.append('file', file);
//       formData.append('country', countryName);

//       const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData`, {
//         method: 'POST',
//         headers: {
//           Authorization: `Bearer ${token}`,
//         },
//         body: formData,
//       });

//       const result = await response.json().catch(() => ({}));

//       if (!response.ok) {
//         throw new Error(result?.error || result?.message || 'Failed to upload warehouse file');
//       }

//       const rows = Array.isArray(result?.data) ? result.data : [];
//       setWarehouseData(rows);

//       const cols = Array.isArray(result?.columns)
//         ? result.columns
//         : rows.length > 0
//           ? Object.keys(rows[0])
//           : [];

//       setWarehouseColumns(getOrderedWarehouseColumns(cols));

//       setShowWarehouseUpload(false);
//       setModalMessage(result?.message || 'Warehouse file uploaded successfully');
//       setShowModal(true);
//     } catch (e: any) {
//       setModalMessage(e?.message || 'Failed to upload warehouse file');
//       setShowModal(true);
//     } finally {
//       setWarehouseLoading(false);
//     }
//   };

//   useEffect(() => {
//     if (activeTab === 'extra') {
//       void fetchWarehouseData();
//     }
//   }, [activeTab, countryName, skuData]);

//   const renderGrossMarginCell = (row: SkuRow, column: string) => {
//     const targetCountry = column.replace('gross_margin_', '');
//     const currentPrice =
//       editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price;
//     const currency = row.currency;
//     const grossMargin = calculateGrossMargin(currentPrice, currency, targetCountry, row.product_name);

//     if (grossMargin === 'N/A') return <span className="gross-margin-na">N/A</span>;
//     const marginValue = parseFloat(grossMargin);
//     const className = marginValue >= 0 ? 'gross-margin-positive' : 'gross-margin-negative';
//     return <span className={className}>{grossMargin}%</span>;
//   };

//   const getMonthYearDisplay = (row: SkuRow) => {
//     const month = row.month ?? '';
//     const year = row.year ?? '';
//     if (month && year) return `${month} ${year}`;
//     if (month) return String(month);
//     if (year) return String(year);
//     return '—';
//   };

//   const handleDownloadXLSX = () => {
//     if (!skuData || skuData.length === 0) {
//       alert('No data available to download.');
//       return;
//     }

//     const dataToExport = skuData.map((row) => {
//       const newRow = { ...row } as SkuRow;
//       if (editedPrices[row.product_name] !== undefined) {
//         newRow.price = editedPrices[row.product_name];
//       }
//       return newRow;
//     });

//     const exportData = dataToExport.map((row) => {
//       const filtered: Record<string, any> = {};
//       visibleColumns.forEach((col) => {
//         if (col.startsWith('gross_margin_')) {
//           const c = col.replace('gross_margin_', '');
//           const gm = calculateGrossMargin(
//             editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price,
//             row.currency,
//             c,
//             row.product_name
//           );
//           filtered[String(getColumnDisplayName(col))] = gm !== 'N/A' ? `${gm}%` : 'N/A';
//         } else if (col === 'price') {
//           const priceValue =
//             editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price;
//           const symbol = getCurrencySymbol(row.currency);
//           filtered[String(getColumnDisplayName(col))] = `${symbol}${priceValue ?? ''}`;
//         } else if (col === 'month_year') {
//           filtered[String(getColumnDisplayName(col))] = getMonthYearDisplay(row);
//         } else {
//           filtered[String(getColumnDisplayName(col))] = (row as any)[col];
//         }
//       });
//       return filtered;
//     });

//     const worksheet = XLSX.utils.json_to_sheet(exportData);
//     const workbook = XLSX.utils.book_new();
//     XLSX.utils.book_append_sheet(workbook, worksheet, 'SKU Price Data');

//     const fileName = `SKU_Price_Data_${countryName?.toUpperCase() || 'EXPORT'}.xlsx`;
//     XLSX.writeFile(workbook, fileName);
//   };

//   const tableData: TableRow[] = useMemo(() => {
//     return skuData.map((row, index) => {
//       const item: TableRow = {
//         id: `${row.product_name}-${index}`,
//         s_no: row.s_no ?? index + 1,
//         product_name: row.product_name ?? '—',
//         sku_uk: row.sku_uk ?? '—',
//         sku_us: row.sku_us ?? '—',
//         sku_canada: row.sku_canada ?? '—',
//         asin: row.asin ?? '—',
//         product_barcode: row.product_barcode ?? '—',
//         month_year: getMonthYearDisplay(row),
//         price: row.price ?? '',
//       };

//       visibleColumns.forEach((col) => {
//         if (col.startsWith('gross_margin_')) {
//           item[col] = '';
//         } else if (col in row) {
//           item[col] = row[col] ?? '—';
//         }
//       });

//       return item;
//     });
//   }, [skuData, visibleColumns]);

//   const columns: ColumnDef<TableRow>[] = useMemo(() => {
//     return visibleColumns.map((column) => {
//       const col: ColumnDef<TableRow> = {
//         key: column,
//         header: getColumnDisplayName(column),
//       };

//       if (column === 's_no') {
//         col.width = '70px';
//       }

//       if (column === 'product_name') {
//         col.width = '220px';
//         col.cellClassName = 'text-left';
//         col.render = (tableRow) => {
//           return <span>{tableRow.product_name}</span>;
//         };
//       }

//       if (column === 'asin') {
//         col.width = '140px';
//       }

//       if (column === 'product_barcode') {
//         col.width = '160px';
//       }

//       if (column === 'month_year') {
//         col.width = '140px';
//       }

//       if (column === 'price') {
//         col.width = '160px';
//         col.render = (tableRow) => {
//           const originalRow = skuData.find((r) => r.product_name === String(tableRow.product_name));
//           if (!originalRow) return '—';

//           return isEditing ? (
//             <div className="flex items-center justify-center gap-1">
//               <span>{getCurrencySymbol(originalRow.currency)}</span>
//               <input
//                 type="number"
//                 className="border border-gray-300 rounded px-2 py-1 w-[90px] text-center"
//                 value={
//                   editedPrices[originalRow.product_name] !== undefined
//                     ? editedPrices[originalRow.product_name]
//                     : originalRow.price ?? ''
//                 }
//                 onChange={(e) => handlePriceChange(originalRow.product_name, e.target.value)}
//               />
//             </div>
//           ) : (
//             <span>
//               {getCurrencySymbol(originalRow.currency)} {originalRow.price ?? '—'}
//             </span>
//           );
//         };
//       }

//       if (column.startsWith('gross_margin_')) {
//         col.width = '150px';
//         col.render = (tableRow) => {
//           const originalRow = skuData.find((r) => r.product_name === String(tableRow.product_name));
//           if (!originalRow) return 'N/A';
//           return renderGrossMarginCell(originalRow, column);
//         };
//       }

//       return col;
//     });
//   }, [visibleColumns, skuData, isEditing, editedPrices]);

//   const tabOptions = useMemo(
//     () => [
//       { value: 'sku-info' as const, label: 'Sku Info' },
//       { value: 'extra' as const, label: 'Upload Warehouse Data' },
//     ],
//     []
//   );

//   // 3) KEEP warehouse tab ordered exactly like your Excel
//   const getOrderedWarehouseColumns = (cols: string[]) => {
//     const preferredOrder = [
//       's_no',
//       'sku_us',
//       'sku_uk',
//       'local_stock',
//       'in_transit_units',
//       'month',
//       'year',
//     ];

//     const preferred = preferredOrder.filter((c) => cols.includes(c));
//     const remaining = cols.filter((c) => !preferredOrder.includes(c));

//     return [...preferred, ...remaining];
//   };

//   // 4) KEEP warehouse headers matching Excel labels
//   const getWarehouseHeaderLabel = (col: string) => {
//     switch (col) {
//       case 's_no':
//         return 'S No';
//       case 'sku_us':
//         return 'SKU_US';
//       case 'sku_uk':
//         return 'SKU_UK';
//       case 'local_stock':
//         return 'Local Stock';
//       case 'in_transit_units':
//         return 'In Transit Units';
//       case 'month':
//         return 'Month';
//       case 'year':
//         return 'Year';
//       default:
//         return col.replaceAll('_', ' ').replace(/\b\w/g, (c) => c.toUpperCase());
//     }
//   };

//   const warehouseTableColumns: ColumnDef<Record<string, any>>[] = useMemo(() => {
//     return warehouseColumns.map((col) => ({
//       key: col,
//       header: getWarehouseHeaderLabel(col),
//       width:
//         col === 's_no'
//           ? '70px'
//           : col === 'sku_us' || col === 'sku_uk'
//             ? '120px'
//             : col === 'local_stock' || col === 'in_transit_units'
//               ? '150px'
//               : col === 'month' || col === 'year'
//                 ? '110px'
//                 : '140px',
//       cellClassName: 'text-center',
//       render: (row) => {
//         const value = row[col];
//         return value === null || value === undefined || value === '' ? '—' : String(value);
//       },
//     }));
//   }, [warehouseColumns]);
//   if (loading) return <div>Loading...</div>;
//   if (error) return <div>Error: {error}</div>;


//   const UploadModalShell = ({
//     open,
//     onClose,
//     children,
//   }: {
//     open: boolean;
//     onClose: () => void;
//     children: React.ReactNode;
//   }) => {
//     if (!open) return null;

//     return (
//       <div
//         onClick={onClose}
//         className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
//       >
//         <div
//           onClick={(e) => e.stopPropagation()}
//           className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
//         >
//           <button
//             onClick={onClose}
//             type="button"
//             className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
//           >
//             &times;
//           </button>

//           <div className="relative w-full rounded-xl bg-white p-4 no-scrollbar lg:p-9">
//             {children}
//           </div>
//         </div>
//       </div>
//     );
//   };

//   const PreviewLockedSection = ({
//     enabled,
//     children,
//     title,
//     description,
//     buttonText,
//     onAction,
//   }: {
//     enabled: boolean;
//     children: React.ReactNode;
//     title?: string;
//     description?: string;
//     buttonText?: string;
//     onAction?: () => void;
//   }) => {
//     return (
//       <div className="relative w-full">
//         <div
//           className={
//             enabled
//               ? "pointer-events-none select-none opacity-45 transition-all duration-300"
//               : "opacity-100 transition-all duration-300"
//           }
//         >
//           {children}
//         </div>

//         {enabled && (
//           <>
//             <div className="absolute inset-0 z-10 rounded-xl bg-white/45" />

//             <div className="absolute inset-0 z-20 pointer-events-none">
//               <div className="sticky top-[18vh] sm:top-[20vh] lg:top-[22vh] 2xl:top-[24vh] flex justify-center px-4">
//                 <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl p-6 text-center">
//                   <div className="mb-4 flex justify-center">
//                     <div className="flex h-16 w-16 items-center justify-center rounded-full bg-[#37455F]">
//                       <IoMdLock className="text-3xl text-[#F8EDCE]" />
//                     </div>
//                   </div>

//                   <h3 className="text-lg font-semibold text-[#414042]">
//                     {title}
//                   </h3>

//                   <p className="mt-2 text-sm text-gray-600 leading-6">
//                     {description}
//                   </p>

//                   <button
//                     onClick={onAction}
//                     className="mt-4 rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90 transition"
//                   >
//                     {buttonText}
//                   </button>

//                   <p className="mt-3 text-xs text-gray-500">
//                     Demo data is shown for preview only.
//                   </p>
//                 </div>
//               </div>
//             </div>
//           </>
//         )}
//       </div>
//     );
//   };

//   const handleConnectAmazonPreview = () => {
//     const connectCountry = countryName === 'global' ? 'uk' : countryName;
//     router.push(`/profile/${connectCountry}/NA/NA`);
//   };

//   return (
//     <div>
//       <style>{`
//         div { font-family: 'Lato', sans-serif; }
//         .gross-margin-positive { color: #28a745; font-weight: bold; }
//         .gross-margin-negative { color: #dc3545; font-weight: bold; }
//         .gross-margin-na { color: #6c757d; font-style: italic; }
//       `}</style>

//       <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
//         <div className="min-w-0">
//           <div className="flex flex-wrap items-baseline gap-2 justify-start">
//             <PageBreadcrumb
//               pageTitle="Input Cost –"
//               variant="page"
//               align="left"
//               className=""
//             />
//             <span className="text-green-500 font-bold text-base sm:text-xl lg:text-lg 2xl:text-2xl">
//               Amazon {countryName?.toLowerCase() === "global"
//                 ? "Global"
//                 : countryName?.toUpperCase()}
//             </span>
//           </div>

//           <div className="mt-3">
//             <SegmentedToggle
//               value={activeTab}
//               options={tabOptions}
//               onChange={(val) => setActiveTab(val as InputCostTab)}
//               compact
//               textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
//             />
//           </div>
//         </div>

//         <div className="flex flex-wrap items-center gap-3 md:justify-end">
//           {activeTab === 'sku-info' ? (
//             <>
//               {isEditing && (
//                 <button className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]" onClick={saveChanges}>
//                   Save Changes
//                 </button>
//               )}

//               {/* <button
//                 className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
//                 onClick={() => setShowMultiuseCountry(true)}
//               >
//                 Re-Upload file
//               </button>

//               <DownloadIconButton onClick={handleDownloadXLSX} size="md" /> */}

//               <button
//                 className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
//                 onClick={() => setShowMultiuseCountry(true)}
//                 disabled={isNA}
//               >
//                 Re-Upload file
//               </button>

//               <DownloadIconButton onClick={handleDownloadXLSX} size="md" disabled={isNA} />
//             </>
//           ) : (
//             // <button
//             //   className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
//             //   onClick={() => setShowWarehouseUpload(true)}
//             // >
//             //   Upload Warehouse File
//             // </button>

//             <button
//               className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
//               onClick={() => setShowWarehouseUpload(true)}
//               disabled={isNA}
//             >
//               Upload Warehouse File
//             </button>
//           )}
//         </div>
//       </div>


//       <PreviewLockedSection
//         enabled={isNA}
//         title="Preview mode"
//         description="You're not seeing your real data yet.Connect your Amazon account now to unlock complete visibility into your business performance."
//         buttonText="Connect Amazon"
//         onAction={handleConnectAmazonPreview}
//       >
//         <>
//           {activeTab === 'sku-info' && (
//             <>
//               {skuData.length > 0 ? (
//                 <div className="mt-5">
//                   <DataTable<TableRow>
//                     columns={columns}
//                     data={tableData}
//                     loading={loading}
//                     paginate={true}
//                     pageSize={10}
//                     stickyHeader={true}
//                     zebra={true}
//                     scrollY={false}
//                     maxHeight="auto"
//                     emptyMessage="No data available"
//                     tableClassName="text-sm"
//                     className="rounded-xl"
//                   />
//                 </div>
//               ) : (
//                 <p className="mt-5">No data available</p>
//               )}
//             </>
//           )}

//           {activeTab === 'extra' && (
//             <div className="mt-5">
//               {/* <div className="mb-2 text-sm font-semibold text-[#414042]">
//                 Warehouse Data
//               </div> */}

//               {warehouseLoading ? (
//                 <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-slate-500">
//                   Uploading...
//                 </div>
//               ) : warehouseData.length > 0 ? (
//                 <DataTable<Record<string, any>>
//                   columns={warehouseTableColumns}
//                   data={warehouseData}
//                   paginate={true}
//                   pageSize={10}
//                   stickyHeader
//                   scrollY={false}
//                   maxHeight="auto"
//                   emptyMessage="No warehouse data available"
//                   tableClassName="text-sm"
//                   className="rounded-xl"
//                 />
//               ) : (
//                 <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-slate-500">
//                   Upload a warehouse file to view data here.
//                 </div>
//               )}
//             </div>
//           )}
//         </>
//       </PreviewLockedSection>


//       {showWarehouseUpload && (
//         <div
//           onClick={() => {
//             setSelectedWarehouseFile(null);
//             setShowWarehouseUpload(false);
//           }}
//           className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
//         >
//           <div
//             onClick={(e) => e.stopPropagation()}
//             className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
//           >
//             <button
//               onClick={() => {
//                 setSelectedWarehouseFile(null);
//                 setShowWarehouseUpload(false);
//               }}
//               type="button"
//               className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
//             >
//               &times;
//             </button>

//             <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar lg:p-9">
//               <div className="w-full max-w-[520px] mx-auto flex flex-col gap-3">
//                 <PageBreadcrumb
//                   pageTitle="Upload Warehouse Data"
//                   variant="table"
//                   align="center2"
//                 />


//                 <div className="rounded-2xl p-3">
//                   <div className="flex items-center gap-2 rounded-md border border-gray-300 bg-white px-2 py-1.5">
//                     <label
//                       htmlFor="warehouse-file"
//                       className="shrink-0 cursor-pointer rounded-md bg-gray-100 px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-200"
//                     >
//                       Upload File
//                     </label>

//                     <input
//                       id="warehouse-file"
//                       type="file"
//                       accept=".xlsx,.xls"
//                       className="hidden"
//                       onChange={(e) => {
//                         const file = e.target.files?.[0] || null;
//                         setSelectedWarehouseFile(file);
//                       }}
//                     />

//                     <span className="block w-full truncate px-2 text-xs text-gray-500">
//                       {selectedWarehouseFile?.name || "No File Chosen"}
//                     </span>
//                   </div>

//                   <button
//                     type="button"
//                     onClick={() => {
//                       const link = document.createElement("a");
//                       link.href = "/warehouse-information-template.xlsx";
//                       link.download = "Warehouse Information.xlsx";
//                       document.body.appendChild(link);
//                       link.click();
//                       document.body.removeChild(link);
//                     }}
//                     className="mx-auto mt-6 flex items-center gap-1 text-[13px] font-medium text-[#5EA68E] hover:text-[#4a907a]"
//                   >
//                     Download format here
//                   </button>
//                 </div>

//                 <div className="mt-2 flex justify-center gap-3">
//                   <button
//                     type="button"
//                     onClick={() => {
//                       setSelectedWarehouseFile(null);
//                       setShowWarehouseUpload(false);
//                     }}
//                     className="rounded-md border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
//                   >
//                     Cancel
//                   </button>

//                   <button
//                     type="button"
//                     onClick={() => {
//                       if (!selectedWarehouseFile) {
//                         setModalMessage("Please select a file first");
//                         setShowModal(true);
//                         return;
//                       }
//                       void handleWarehouseUpload(selectedWarehouseFile);
//                     }}
//                     className="rounded-md bg-[#5EA68E] px-4 py-2 text-sm font-semibold text-yellow-100 hover:opacity-95 disabled:opacity-60"
//                     disabled={warehouseLoading}
//                   >
//                     {warehouseLoading ? "Uploading..." : "Upload File"}
//                   </button>
//                 </div>
//               </div>
//             </div>
//           </div>
//         </div>
//       )}

//       {showMultiuseCountry && (
//         <div
//           onClick={() => setShowMultiuseCountry(false)}
//           className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
//         >
//           <div
//             onClick={(e) => e.stopPropagation()}
//             className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
//           >
//             <button
//               onClick={() => setShowMultiuseCountry(false)}
//               type="button"
//               className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
//             >
//               &times;
//             </button>

//             <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar lg:p-9">
//               <SkuMultiuseCountryUpload
//                 onClose={() => setShowMultiuseCountry(false)}
//                 onComplete={() => {
//                   setShowMultiuseCountry(false);

//                   // optional: refresh SKU table after upload
//                   // window.location.reload();
//                   // or call your fetchSkuData() if you expose it outside useEffect
//                 }}
//               />
//             </div>
//           </div>
//         </div>
//       )}
//       <Modalmsg
//         show={showModal}
//         message={modalMessage}
//         onClose={() => setShowModal(false)}
//         onCancel={() => setShowModal(false)}
//       />
//     </div>
//   );
// }

























































'use client';

import React, { use, useEffect, useMemo, useState } from 'react';
import * as XLSX from 'xlsx';
import '@/app/(admin)/pnlforecast/[countryName]/[month]/[year]/Styles.css';
import Modalmsg from '@/components/ui/modal/Modalmsg';
import SkuMultiuseCountryUpload from '@/components/ui/modal/SkuMultiCountryUpload';
import DataTable, { ColumnDef } from '@/components/ui/table/DataTable';
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import SegmentedToggle from '@/components/ui/SegmentedToggle';
import DownloadIconButton from '@/components/ui/button/DownloadButton';
import { useRouter } from 'next/navigation';
import { IoMdLock } from "react-icons/io";
import WarehouseMultiCountryUpload from "@/components/ui/modal/WarehouseMultiCountryUpload";

// =========================
// Warehouse upload format
// =========================
const WAREHOUSE_FIXED_COLUMNS = [
  's_no',
  'sku_us',
  'sku_uk',
  'local_stock',
  'in_transit_units',
  'month',
  'year',
];

const normalizeWarehouseHeader = (key: string) => {
  const raw = key.trim().toLowerCase().replace(/\s+/g, '_');

  const map: Record<string, string> = {
    's.no': 's_no',
    's.no.': 's_no',
    's._no': 's_no',
    's._no.': 's_no',
    's_no': 's_no',
    'serial_no': 's_no',
    'serial_number': 's_no',

    'sku_us': 'sku_us',
    'sku_(us)': 'sku_us',
    'sku-us': 'sku_us',
    'sku us': 'sku_us',

    'sku_uk': 'sku_uk',
    'sku_(uk)': 'sku_uk',
    'sku-uk': 'sku_uk',
    'sku uk': 'sku_uk',

    'stock': 'local_stock',
    'local_stock': 'local_stock',
    'local stock': 'local_stock',

    'in_transit': 'in_transit_units',
    'transit_units': 'in_transit_units',
    'in_transit_units': 'in_transit_units',
    'in transit units': 'in_transit_units',

    'month': 'month',
    'year': 'year',
  };

  return map[raw] || raw;
};

const validateWarehouseHeaders = (row: Record<string, any>) => {
  const normalized = Object.keys(row).map(normalizeWarehouseHeader);

  const missing = WAREHOUSE_FIXED_COLUMNS.filter(
    (col) => !normalized.includes(col)
  );

  if (missing.length) {
    return 'Invalid file format. Please upload a file using the provided template.';
  }

  return '';
};

// Types
interface Params {
  params: Promise<{
    countryName: string;
    month: string;
    year: string;
  }>;
}

interface SkuRow {
  s_no: number;
  product_name: string;
  sku_uk?: string;
  sku_us?: string;
  sku_canada?: string;
  asin?: string;
  product_barcode?: string;
  price?: number;
  currency?: string;
  month?: string;
  year?: string | number;
  local_stock?: number;
  in_transit_units?: number;
  [key: string]: any;
}

type TableRow = {
  id: string;
  s_no: React.ReactNode;
  product_name: React.ReactNode;
  sku_uk?: React.ReactNode;
  sku_us?: React.ReactNode;
  sku_canada?: React.ReactNode;
  asin?: React.ReactNode;
  product_barcode?: React.ReactNode;
  month_year?: React.ReactNode;
  price?: React.ReactNode;
  gross_margin_uk?: React.ReactNode;
  gross_margin_us?: React.ReactNode;
  gross_margin_canada?: React.ReactNode;
  gross_margin_eu?: React.ReactNode;
  gross_margin_europe?: React.ReactNode;
  gross_margin_global?: React.ReactNode;
  [key: string]: React.ReactNode;
};

const getCurrencySymbol = (country: string | undefined): string => {
  switch (country) {
    case 'GBP':
      return '£';
    case 'INR':
      return '₹';
    case 'USD':
      return '$';
    case 'europe':
    case 'eu':
    case 'EUR':
      return '€';
    case 'CAD':
      return '$';
    case 'global':
      return '$';
    default:
      return '$';
  }
};

function getCurrencyForCountry(country: string): string {
  switch (country.toLowerCase()) {
    case 'uk':
      return 'GBP';
    case 'us':
      return 'USD';
    case 'canada':
      return 'CAD';
    case 'eu':
    case 'europe':
      return 'EUR';
    default:
      return 'USD';
  }
}

const DUMMY_SKU_DATA: SkuRow[] = [
  {
    s_no: 1,
    product_name: 'Sample Product A',
    sku_uk: 'UK-SKU-001',
    sku_us: 'US-SKU-001',
    sku_canada: 'CA-SKU-001',
    asin: 'B0DUMMY001',
    product_barcode: '1234567890123',
    price: 12.5,
    currency: 'GBP',
    month: 'January',
    year: '2026',
  },
  {
    s_no: 2,
    product_name: 'Sample Product B',
    sku_uk: 'UK-SKU-002',
    sku_us: 'US-SKU-002',
    sku_canada: 'CA-SKU-002',
    asin: 'B0DUMMY002',
    product_barcode: '2234567890123',
    price: 18.75,
    currency: 'GBP',
    month: 'January',
    year: '2026',
  },
  {
    s_no: 3,
    product_name: 'Sample Product C',
    sku_uk: 'UK-SKU-003',
    sku_us: 'US-SKU-003',
    sku_canada: 'CA-SKU-003',
    asin: 'B0DUMMY003',
    product_barcode: '3234567890123',
    price: 9.99,
    currency: 'GBP',
    month: 'January',
    year: '2026',
  },
];

const DUMMY_ASP_DATA: Record<string, number> = {
  'Sample Product A': 25,
  'Sample Product B': 34,
  'Sample Product C': 16,
  'Sample Product A_uk': 25,
  'Sample Product B_uk': 34,
  'Sample Product C_uk': 16,
  'Sample Product A_us': 31,
  'Sample Product B_us': 42,
  'Sample Product C_us': 20,
  'Sample Product A_canada': 38,
  'Sample Product B_canada': 49,
  'Sample Product C_canada': 24,
};

const DUMMY_CURRENCY_RATES: Record<string, number> = {
  GBP: 1,
  USD: 1,
  CAD: 1,
  EUR: 1,
  INR: 1,
  gbp: 1,
  usd: 1,
  cad: 1,
  eur: 1,
  inr: 1,
  GBP_uk: 1,
  USD_us: 1,
  CAD_canada: 1,
  EUR_europe: 1,
  EUR_eu: 1,
  USD_global: 1,
};

const DUMMY_WAREHOUSE_DATA = [
  {
    s_no: 1,
    sku_us: 'US-SKU-001',
    sku_uk: 'UK-SKU-001',
    local_stock: 120,
    in_transit_units: 15,
    month: 'January',
    year: '2026',
  },
  {
    s_no: 2,
    sku_us: 'US-SKU-002',
    sku_uk: 'UK-SKU-002',
    local_stock: 85,
    in_transit_units: 9,
    month: 'January',
    year: '2026',
  },
];

export default function InputCostPage({ params }: Params) {
  const { countryName: countryNameRaw, month: monthRaw, year: yearRaw } = use(params);
  const countryName = decodeURIComponent(countryNameRaw ?? '').toLowerCase();
  const monthParam = decodeURIComponent(monthRaw ?? '');
  const yearParam = decodeURIComponent(yearRaw ?? '');

  const [skuData, setSkuData] = useState<SkuRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [isEditing, setIsEditing] = useState(false);
  const [editedPrices, setEditedPrices] = useState<Record<string, number>>({});
  const [showModal, setShowModal] = useState(false);
  const [modalMessage, setModalMessage] = useState('');
  const [visibleColumns, setVisibleColumns] = useState<string[]>([]);
  const [currencyRates, setCurrencyRates] = useState<Record<string, number>>({});
  const [aspData, setAspData] = useState<Record<string, number>>({});
  const [showMultiuseCountry, setShowMultiuseCountry] = useState(false);
  const [warehouseData, setWarehouseData] = useState<Record<string, any>[]>([]);
  const [warehouseColumns, setWarehouseColumns] = useState<string[]>([]);
  const [warehouseLoading, setWarehouseLoading] = useState(false);
  const [showWarehouseUpload, setShowWarehouseUpload] = useState(false);
  const [selectedWarehouseFile, setSelectedWarehouseFile] = useState<File | null>(null);
  const [warehouseUploadError, setWarehouseUploadError] = useState('');

  const router = useRouter();

  const isNA =
    monthParam?.toLowerCase() === 'na' ||
    yearParam?.toLowerCase() === 'na';

  type InputCostTab = 'sku-info' | 'extra';
  const [activeTab, setActiveTab] = useState<InputCostTab>('sku-info');

  const isColumnEmpty = (data: SkuRow[], columnName: string) => {
    return data.every((row) => {
      const value = row[columnName];
      return (
        value === null ||
        value === undefined ||
        value === '' ||
        (typeof value === 'string' && value.trim() === '')
      );
    });
  };

  // Remove local_stock and in_transit_units from SKU tab
  const getVisibleColumns = (data: SkuRow[]) => {
    if (!data || data.length === 0) return [] as string[];

    const baseColumns: string[] = ['s_no', 'product_name'];
    let skuColumns: string[] = [];
    let grossMarginColumns: string[] = [];

    if (countryName === 'global') {
      const potentialSkuColumns = ['sku_uk', 'sku_us', 'sku_canada'];
      skuColumns = potentialSkuColumns.filter((col) => !isColumnEmpty(data, col));
      skuColumns.forEach((skuCol) => {
        const c = skuCol.replace('sku_', '');
        grossMarginColumns.push(`gross_margin_${c}`);
      });
    } else {
      const skuColumn = `sku_${countryName}`;
      if (!isColumnEmpty(data, skuColumn)) {
        skuColumns.push(skuColumn);
      }
      grossMarginColumns.push(`gross_margin_${countryName}`);
    }

    const otherColumns = [
      'asin',
      'product_barcode',
      'month_year',
      'price',
    ];

    const visibleOtherColumns = otherColumns.filter((col) => {
      if (col === 'month_year') {
        return data.some((row) => row.month || row.year);
      }
      return !isColumnEmpty(data, col);
    });

    return [...baseColumns, ...skuColumns, ...visibleOtherColumns, ...grossMarginColumns];
  };

  const getColumnDisplayName = (column: string): React.ReactNode => {
    switch (column) {
      case 's_no':
        return 'S.No.';
      case 'product_name':
        return 'Product Name';
      case 'sku_uk':
        return 'SKU (UK)';
      case 'sku_us':
        return 'SKU (US)';
      case 'sku_canada':
        return 'SKU (CANADA)';
      case 'asin':
        return 'ASIN';
      case 'product_barcode':
        return 'Product Barcode';
      case 'month_year':
        return 'Month / Year';
      case 'price':
        return 'Landing Cost';
      default:
        if (column.startsWith('sku_')) {
          const c = column.replace('sku_', '').toUpperCase();
          return `SKU (${c})`;
        }
        if (column.startsWith('gross_margin_')) {
          const c = column.replace('gross_margin_', '').toUpperCase();
          return (
            <>
              {`Gross Margin (%) ${c} `}
              <span
                style={{ position: 'relative', cursor: 'pointer' }}
                title="*Gross Margin calculation is based on previous month’s ASP"
              >
                &nbsp;<i className="fa-solid fa-circle-info" style={{ color: '#f8edcf' }}></i>
              </span>
            </>
          );
        }
        return column.charAt(0).toUpperCase() + column.slice(1);
    }
  };

  const getCurrencyRate = (currency: string | undefined, country: string) => {
    if (!currency || !currencyRates || Object.keys(currencyRates).length === 0) return 1;
    const possibleKeys = [
      `${currency}_${country}`,
      `${currency.toLowerCase()}_${country.toLowerCase()}`,
      `${currency.toUpperCase()}_${country.toLowerCase()}`,
      currency,
      currency.toLowerCase(),
      currency.toUpperCase(),
    ];
    for (const key of possibleKeys) {
      if (currencyRates[key] !== undefined) return currencyRates[key];
    }
    return 1;
  };

  const getAspForProduct = (productName: string, targetCountry: string | null = null) => {
    if (!aspData || Object.keys(aspData).length === 0) return null;

    if (targetCountry && countryName === 'global') {
      const countrySpecificKey = `${productName}_${targetCountry}`;
      if (aspData[countrySpecificKey] !== undefined) return aspData[countrySpecificKey];
      for (const key in aspData) {
        if (key.includes(`_${targetCountry}`) && key.includes(productName)) return aspData[key];
      }
    }

    if (countryName === 'global') {
      if (aspData[productName] !== undefined) return aspData[productName];
      for (const key in aspData) {
        if (key.includes(productName) || productName.includes(key)) return aspData[key];
      }
    } else {
      return aspData[productName] ?? null;
    }

    return null;
  };

  const calculateGrossMargin = (
    price: number | undefined,
    sourceCurrency: string | undefined,
    targetCountry: string,
    productName: string
  ): string => {
    try {
      const asp = getAspForProduct(productName, targetCountry);
      if (!price || !asp || asp === 0) return 'N/A';

      let convertedPrice: number;
      if (countryName === 'global') {
        const targetCurrency = getCurrencyForCountry(targetCountry);
        if (sourceCurrency === targetCurrency) {
          convertedPrice = price;
        } else {
          const sourceToUsdRate = getCurrencyRate(sourceCurrency, 'global') || 1;
          const usdToTargetRate = getCurrencyRate(targetCurrency, targetCountry) || 1;
          convertedPrice = price * sourceToUsdRate * usdToTargetRate;
        }
      } else {
        const currencyRate = getCurrencyRate(sourceCurrency, targetCountry);
        convertedPrice = price * currencyRate;
      }

      const grossMargin = ((asp - convertedPrice) / asp) * 100;
      return grossMargin.toFixed(2);
    } catch {
      return 'N/A';
    }
  };

  const fetchCurrencyRates = async () => {
    if (isNA) {
      setCurrencyRates(DUMMY_CURRENCY_RATES);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) return;

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/currency-rates`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });
      if (response.ok) {
        const rates: Array<{ user_currency: string; country: string; conversion_rate: number }> =
          await response.json();
        const map: Record<string, number> = {};
        rates.forEach((rate) => {
          const keys = [
            `${rate.user_currency}_${rate.country}`,
            `${rate.user_currency.toLowerCase()}_${rate.country.toLowerCase()}`,
            `${rate.user_currency.toUpperCase()}_${rate.country.toLowerCase()}`,
            rate.user_currency,
            rate.user_currency.toLowerCase(),
            rate.user_currency.toUpperCase(),
          ];
          keys.forEach((k) => (map[k] = rate.conversion_rate));
        });
        setCurrencyRates(map);
      }
    } catch (e) {
      console.error('Error fetching currency rates', e);
    }
  };

  const fetchAspData = async () => {
    if (isNA) {
      setAspData(DUMMY_ASP_DATA);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) return;

    const monthNames = [
      'january', 'february', 'march', 'april', 'may', 'june',
      'july', 'august', 'september', 'october', 'november', 'december',
    ];

    const normalizedMonth = (() => {
      const m = monthParam.toLowerCase();
      if (/^\d+$/.test(m)) {
        const idx = Math.min(Math.max(parseInt(m, 10) - 1, 0), 11);
        return monthNames[idx];
      }
      return monthNames.includes(m) ? m : monthNames[new Date().getMonth()];
    })();

    const normalizedYear = (() => {
      const y = parseInt(yearParam, 10);
      if (!isNaN(y) && y > 2000 && y < 2100) return y;
      return new Date().getFullYear();
    })();

    try {
      let currentMonthIndex = monthNames.indexOf(normalizedMonth);
      let currentYear = normalizedYear;

      for (let attempt = 0; attempt < 12; attempt++) {
        const monthName = monthNames[currentMonthIndex];
        try {
          const response = await fetch(
            `${process.env.NEXT_PUBLIC_API_BASE_URL}/asp-data?country=${countryName}&month=${monthName}&year=${currentYear}`,
            {
              method: 'GET',
              headers: { Authorization: `Bearer ${token}` },
            }
          );
          if (response.ok) {
            const aspArray: Array<{ product_name: string; asp: number; source_country?: string }> =
              await response.json();
            const map: Record<string, number> = {};
            aspArray.forEach((item) => {
              map[item.product_name] = item.asp;
            });
            setAspData(map);
            return;
          }
        } catch {
          // continue
        }
        currentMonthIndex--;
        if (currentMonthIndex < 0) {
          currentMonthIndex = 11;
          currentYear--;
        }
      }
      setAspData({});
    } catch (e) {
      console.error('Error in fetchAspData', e);
      setAspData({});
    }
  };

  const fetchSkuData = async () => {
    if (isNA) {
      const previewCurrency =
        countryName === 'uk'
          ? 'GBP'
          : countryName === 'us'
            ? 'USD'
            : countryName === 'canada'
              ? 'CAD'
              : countryName === 'eu' || countryName === 'europe'
                ? 'EUR'
                : 'USD';

      const previewRows = DUMMY_SKU_DATA.map((row) => ({
        ...row,
        currency: countryName === 'global' ? row.currency : previewCurrency,
        month: 'January',
        year: '2026',
      }));

      const sorted = [...previewRows].sort((a, b) => (a.s_no ?? 0) - (b.s_no ?? 0));
      setSkuData(sorted);
      setVisibleColumns(getVisibleColumns(sorted));
      setLoading(false);
      setError(null);
      return;
    }

    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      setError('Authorization token is missing');
      setLoading(false);
      return;
    }

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/skuprice`, {
        method: 'GET',
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!response.ok) throw new Error('Failed to fetch data');
      const data: SkuRow[] = await response.json();
      const sorted = [...data].sort((a, b) => (a.s_no ?? 0) - (b.s_no ?? 0));
      setSkuData(sorted);
      setVisibleColumns(getVisibleColumns(sorted));
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSkuData();
    fetchCurrencyRates();
    fetchAspData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [countryName, monthParam, yearParam, isNA]);

  const handlePriceChange = (productName: string, value: string) => {
    setEditedPrices((prev) => ({
      ...prev,
      [productName]: value === '' ? NaN : parseFloat(value),
    }));
  };

  const saveChanges = async () => {
    const token = typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;
    if (!token) {
      alert('Authorization token is missing');
      return;
    }
    if (Object.keys(editedPrices).length === 0) {
      alert('No changes to save.');
      return;
    }
    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/updatePrices`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ prices: editedPrices }),
      });
      if (response.ok) {
        const result = await response.json();
        setModalMessage('Prices updated successfully');
        setShowModal(true);
        setIsEditing(false);
        setEditedPrices({});
        if (result.data) {
          const sortedData: SkuRow[] = result.data.sort((a: SkuRow, b: SkuRow) => (a.s_no ?? 0) - (b.s_no ?? 0));
          setSkuData(sortedData);
          setVisibleColumns(getVisibleColumns(sortedData));
        } else {
          window.location.reload();
        }
      } else {
        const errorData = await response.json();
        throw new Error(errorData.error || 'Failed to update prices');
      }
    } catch (e: any) {
      alert(`Error: ${e.message}`);
      console.error('Update prices error:', e);
    }
  };

  const getOrderedWarehouseColumns = (cols: string[]) => {
    const preferredOrder = [
      's_no',
      'sku_us',
      'sku_uk',
      'local_stock',
      'in_transit_units',
      'month',
      'year',
    ];

    const preferred = preferredOrder.filter((c) => cols.includes(c));
    const remaining = cols.filter((c) => !preferredOrder.includes(c));

    return [...preferred, ...remaining];
  };

  const fetchWarehouseData = async () => {
    if (isNA) {
      setWarehouseData(DUMMY_WAREHOUSE_DATA);
      setWarehouseColumns(getOrderedWarehouseColumns(Object.keys(DUMMY_WAREHOUSE_DATA[0] || {})));
      setWarehouseLoading(false);
      return;
    }

    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    const skuFallbackRows = skuData
      .filter((row) => row.local_stock !== undefined || row.in_transit_units !== undefined)
      .map((row, index) => ({
        s_no: row.s_no ?? index + 1,
        sku_us: row.sku_us ?? '',
        sku_uk: row.sku_uk ?? '',
        local_stock: row.local_stock ?? '',
        in_transit_units: row.in_transit_units ?? '',
        month: row.month ?? '',
        year: row.year ?? '',
      }));

    if (!token) {
      setWarehouseData(skuFallbackRows);
      setWarehouseColumns(
        skuFallbackRows.length > 0
          ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
          : []
      );
      return;
    }

    try {
      setWarehouseLoading(true);

      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData?country=${countryName}`,
        {
          method: 'GET',
          headers: {
            Authorization: `Bearer ${token}`,
          },
        }
      );

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        setWarehouseData(skuFallbackRows);
        setWarehouseColumns(
          skuFallbackRows.length > 0
            ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
            : []
        );
        return;
      }

      const rows = Array.isArray(result?.data) ? result.data : [];

      if (rows.length > 0) {
        const cols = Array.isArray(result?.columns)
          ? result.columns
          : Object.keys(rows[0]);

        setWarehouseData(rows);
        setWarehouseColumns(getOrderedWarehouseColumns(cols));
      } else {
        setWarehouseData(skuFallbackRows);
        setWarehouseColumns(
          skuFallbackRows.length > 0
            ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
            : []
        );
      }
    } catch (e) {
      console.error('Failed to fetch warehouse data', e);

      setWarehouseData(skuFallbackRows);
      setWarehouseColumns(
        skuFallbackRows.length > 0
          ? getOrderedWarehouseColumns(Object.keys(skuFallbackRows[0]))
          : []
      );
    } finally {
      setWarehouseLoading(false);
    }
  };

  const uploadWarehouseToServer = async (file: File) => {
    if (isNA) {
      setModalMessage('Preview mode only. Connect your account to upload warehouse data.');
      setShowModal(true);
      return;
    }

    const token =
      typeof window !== 'undefined' ? localStorage.getItem('jwtToken') : null;

    if (!token) {
      setModalMessage('Authorization token is missing');
      setShowModal(true);
      return;
    }

    try {
      setWarehouseLoading(true);

      const formData = new FormData();
      formData.append('file', file);
      formData.append('country', countryName);

      const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/uploadWarehouseData`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
        },
        body: formData,
      });

      const result = await response.json().catch(() => ({}));

      if (!response.ok) {
        throw new Error(result?.error || result?.message || 'Failed to upload warehouse file');
      }

      const rows = Array.isArray(result?.data) ? result.data : [];
      setWarehouseData(rows);

      const cols = Array.isArray(result?.columns)
        ? result.columns
        : rows.length > 0
          ? Object.keys(rows[0])
          : [];

      setWarehouseColumns(getOrderedWarehouseColumns(cols));

      setSelectedWarehouseFile(null);
      setShowWarehouseUpload(false);
      setModalMessage(result?.message || 'Warehouse file uploaded successfully');
      setShowModal(true);
    } catch (e: any) {
      setModalMessage(e?.message || 'Failed to upload warehouse file');
      setShowModal(true);
    } finally {
      setWarehouseLoading(false);
    }
  };

  const handleWarehouseUpload = async (file: File) => {
    try {
      const reader = new FileReader();

      reader.onload = async (e) => {
        try {
          const wb = XLSX.read(e.target?.result as ArrayBuffer, {
            type: 'array',
          });

          const firstSheetName = wb.SheetNames[0];
          if (!firstSheetName) {
            setModalMessage('The uploaded Excel file has no sheets.');
            setShowModal(true);
            return;
          }

          const sheet = wb.Sheets[firstSheetName];
          const json = XLSX.utils.sheet_to_json(sheet);

          if (!json.length) {
            setModalMessage('The uploaded file is empty.');
            setShowModal(true);
            return;
          }

          const headerError = validateWarehouseHeaders(json[0] as Record<string, any>);
          if (headerError) {
            setModalMessage(headerError);
            setShowModal(true);
            return;
          }

          await uploadWarehouseToServer(file);
        } catch (err) {
          console.error(err);
          setModalMessage('Invalid warehouse file format.');
          setShowModal(true);
        }
      };

      reader.onerror = () => {
        setModalMessage('Failed to read the selected file.');
        setShowModal(true);
      };

      reader.readAsArrayBuffer(file);
    } catch {
      setModalMessage('Invalid warehouse file');
      setShowModal(true);
    }
  };

  useEffect(() => {
    if (activeTab === 'extra') {
      void fetchWarehouseData();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeTab, countryName, skuData]);

  const renderGrossMarginCell = (row: SkuRow, column: string) => {
    const targetCountry = column.replace('gross_margin_', '');
    const currentPrice =
      editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price;
    const currency = row.currency;
    const grossMargin = calculateGrossMargin(currentPrice, currency, targetCountry, row.product_name);

    if (grossMargin === 'N/A') return <span className="gross-margin-na">N/A</span>;
    const marginValue = parseFloat(grossMargin);
    const className = marginValue >= 0 ? 'gross-margin-positive' : 'gross-margin-negative';
    return <span className={className}>{grossMargin}%</span>;
  };

  const getMonthYearDisplay = (row: SkuRow) => {
    const month = row.month ?? '';
    const year = row.year ?? '';
    if (month && year) return `${month} ${year}`;
    if (month) return String(month);
    if (year) return String(year);
    return '—';
  };

  const handleDownloadXLSX = () => {
    if (!skuData || skuData.length === 0) {
      alert('No data available to download.');
      return;
    }

    const dataToExport = skuData.map((row) => {
      const newRow = { ...row } as SkuRow;
      if (editedPrices[row.product_name] !== undefined) {
        newRow.price = editedPrices[row.product_name];
      }
      return newRow;
    });

    const exportData = dataToExport.map((row) => {
      const filtered: Record<string, any> = {};
      visibleColumns.forEach((col) => {
        if (col.startsWith('gross_margin_')) {
          const c = col.replace('gross_margin_', '');
          const gm = calculateGrossMargin(
            editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price,
            row.currency,
            c,
            row.product_name
          );
          filtered[String(getColumnDisplayName(col))] = gm !== 'N/A' ? `${gm}%` : 'N/A';
        } else if (col === 'price') {
          const priceValue =
            editedPrices[row.product_name] !== undefined ? editedPrices[row.product_name] : row.price;
          const symbol = getCurrencySymbol(row.currency);
          filtered[String(getColumnDisplayName(col))] = `${symbol}${priceValue ?? ''}`;
        } else if (col === 'month_year') {
          filtered[String(getColumnDisplayName(col))] = getMonthYearDisplay(row);
        } else {
          filtered[String(getColumnDisplayName(col))] = (row as any)[col];
        }
      });
      return filtered;
    });

    const worksheet = XLSX.utils.json_to_sheet(exportData);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, 'SKU Price Data');

    const fileName = `SKU_Price_Data_${countryName?.toUpperCase() || 'EXPORT'}.xlsx`;
    XLSX.writeFile(workbook, fileName);
  };

  const tableData: TableRow[] = useMemo(() => {
    return skuData.map((row, index) => {
      const item: TableRow = {
        id: `${row.product_name}-${index}`,
        s_no: row.s_no ?? index + 1,
        product_name: row.product_name ?? '—',
        sku_uk: row.sku_uk ?? '—',
        sku_us: row.sku_us ?? '—',
        sku_canada: row.sku_canada ?? '—',
        asin: row.asin ?? '—',
        product_barcode: row.product_barcode ?? '—',
        month_year: getMonthYearDisplay(row),
        price: row.price ?? '',
      };

      visibleColumns.forEach((col) => {
        if (col.startsWith('gross_margin_')) {
          item[col] = '';
        } else if (col in row) {
          item[col] = row[col] ?? '—';
        }
      });

      return item;
    });
  }, [skuData, visibleColumns]);

  const columns: ColumnDef<TableRow>[] = useMemo(() => {
    return visibleColumns.map((column) => {
      const col: ColumnDef<TableRow> = {
        key: column,
        header: getColumnDisplayName(column),
      };

      if (column === 's_no') col.width = '70px';

      if (column === 'product_name') {
        col.width = '220px';
        col.cellClassName = 'text-left';
        col.render = (tableRow) => <span>{tableRow.product_name}</span>;
      }

      if (column === 'asin') col.width = '140px';
      if (column === 'product_barcode') col.width = '160px';
      if (column === 'month_year') col.width = '140px';

      if (column === 'price') {
        col.width = '160px';
        col.render = (tableRow) => {
          const originalRow = skuData.find((r) => r.product_name === String(tableRow.product_name));
          if (!originalRow) return '—';

          return isEditing ? (
            <div className="flex items-center justify-center gap-1">
              <span>{getCurrencySymbol(originalRow.currency)}</span>
              <input
                type="number"
                className="border border-gray-300 rounded px-2 py-1 w-[90px] text-center"
                value={
                  editedPrices[originalRow.product_name] !== undefined
                    ? editedPrices[originalRow.product_name]
                    : originalRow.price ?? ''
                }
                onChange={(e) => handlePriceChange(originalRow.product_name, e.target.value)}
              />
            </div>
          ) : (
            <span>
              {getCurrencySymbol(originalRow.currency)} {originalRow.price ?? '—'}
            </span>
          );
        };
      }

      if (column.startsWith('gross_margin_')) {
        col.width = '150px';
        col.render = (tableRow) => {
          const originalRow = skuData.find((r) => r.product_name === String(tableRow.product_name));
          if (!originalRow) return 'N/A';
          return renderGrossMarginCell(originalRow, column);
        };
      }

      return col;
    });
  }, [visibleColumns, skuData, isEditing, editedPrices]);

  const tabOptions = useMemo(
    () => [
      { value: 'sku-info' as const, label: 'SKU Info' },
      { value: 'extra' as const, label: 'Upload Warehouse Data' },
    ],
    []
  );

  const getWarehouseHeaderLabel = (col: string) => {
    switch (col) {
      case 's_no':
        return 'S. No.';
      case 'sku_us':
        return 'SKU_US';
      case 'sku_uk':
        return 'SKU_UK';
      case 'local_stock':
        return 'Local Stock';
      case 'in_transit_units':
        return 'In Transit Units';
      case 'month':
        return 'Month';
      case 'year':
        return 'Year';
      default:
        return col.replaceAll('_', ' ').replace(/\b\w/g, (c) => c.toUpperCase());
    }
  };

  const warehouseTableColumns: ColumnDef<Record<string, any>>[] = useMemo(() => {
    return warehouseColumns.map((col) => ({
      key: col,
      header: getWarehouseHeaderLabel(col),
      width:
        col === 's_no'
          ? '70px'
          : col === 'sku_us' || col === 'sku_uk'
            ? '120px'
            : col === 'local_stock' || col === 'in_transit_units'
              ? '150px'
              : col === 'month' || col === 'year'
                ? '110px'
                : '140px',
      cellClassName: 'text-center',
      render: (row) => {
        const value = row[col];

        if (col === "month" && typeof value === "string") {
          return value.charAt(0).toUpperCase() + value.slice(1).toLowerCase();
        }

        return value === null || value === undefined || value === '' ? '—' : String(value);
      }
    }));
  }, [warehouseColumns]);

  if (loading) return <div>Loading...</div>;
  if (error) return <div>Error: {error}</div>;

  const PreviewLockedSection = ({
    enabled,
    children,
    title,
    description,
    buttonText,
    onAction,
  }: {
    enabled: boolean;
    children: React.ReactNode;
    title?: string;
    description?: string;
    buttonText?: string;
    onAction?: () => void;
  }) => {
    return (
      <div className="relative w-full">
        <div
          className={
            enabled
              ? "pointer-events-none select-none opacity-45 transition-all duration-300"
              : "opacity-100 transition-all duration-300"
          }
        >
          {children}
        </div>

        {enabled && (
          <>
            <div className="absolute inset-0 z-10 rounded-xl bg-white/45" />

            <div className="absolute inset-0 z-20 pointer-events-none">
              <div className="sticky top-[18vh] sm:top-[20vh] lg:top-[22vh] 2xl:top-[24vh] flex justify-center px-4">
                <div className="pointer-events-auto w-full max-w-md rounded-2xl bg-white shadow-2xl p-6 text-center">
                  <div className="mb-4 flex justify-center">
                    <div className="flex h-16 w-16 items-center justify-center rounded-full bg-[#37455F]">
                      <IoMdLock className="text-3xl text-[#F8EDCE]" />
                    </div>
                  </div>

                  <h3 className="text-lg font-semibold text-[#414042]">
                    {title}
                  </h3>

                  <p className="mt-2 text-sm text-gray-600 leading-6">
                    {description}
                  </p>

                  <button
                    onClick={onAction}
                    className="mt-4 rounded-md bg-[#37455F] px-4 py-2 text-sm text-[#F8EDCE] hover:opacity-90 transition"
                  >
                    {buttonText}
                  </button>

                  <p className="mt-3 text-xs text-gray-500">
                    Demo data is shown for preview only.
                  </p>
                </div>
              </div>
            </div>
          </>
        )}
      </div>
    );
  };

  const handleConnectAmazonPreview = () => {
    const connectCountry = countryName === 'global' ? 'uk' : countryName;
    router.push(`/profile/${connectCountry}/NA/NA`);
  };

  return (
    <div>
      <style>{`
        div { font-family: 'Lato', sans-serif; }
        .gross-margin-positive { color: #28a745; font-weight: bold; }
        .gross-margin-negative { color: #dc3545; font-weight: bold; }
        .gross-margin-na { color: #6c757d; font-style: italic; }
      `}</style>

      <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
        <div className="min-w-0">
          <div className="flex flex-wrap items-baseline gap-2 justify-start">
            <PageBreadcrumb
              pageTitle="Input Cost –"
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

          <div className="mt-3">
            <SegmentedToggle
              value={activeTab}
              options={tabOptions}
              onChange={(val) => setActiveTab(val as InputCostTab)}
              compact
              textSizeClass="text-[10px] sm:text-xs 2xl:text-sm"
            />
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-3 md:justify-end">
          {activeTab === 'sku-info' ? (
            <>
              {isEditing && (
                <button
                  className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
                  onClick={saveChanges}
                >
                  Save Changes
                </button>
              )}

              <button
                className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
                onClick={() => setShowMultiuseCountry(true)}
                disabled={isNA}
              >
                Re-Upload file
              </button>

              <DownloadIconButton onClick={handleDownloadXLSX} size="md" disabled={isNA} />
            </>
          ) : (
            <button
              className="ml-auto cursor-pointer rounded-[5px] bg-[#2c3e50] px-4 py-2 font-['Lato'] text-[clamp(12px,0.729vw,16px)] font-bold text-[#f8edcf] hover:bg-[#34495e]"
              onClick={() => setShowWarehouseUpload(true)}
              disabled={isNA}
            >
              Upload Warehouse File
            </button>
          )}
        </div>
      </div>

      <PreviewLockedSection
        enabled={isNA}
        title="Preview mode"
        description="You're not seeing your real data yet.Connect your Amazon account now to unlock complete visibility into your business performance."
        buttonText="Connect Amazon"
        onAction={handleConnectAmazonPreview}
      >
        <>
          {activeTab === 'sku-info' && (
            <>
              {skuData.length > 0 ? (
                <div className="mt-5">
                  <DataTable<TableRow>
                    columns={columns}
                    data={tableData}
                    loading={loading}
                    paginate={true}
                    pageSize={10}
                    stickyHeader={true}
                    zebra={true}
                    scrollY={false}
                    maxHeight="auto"
                    emptyMessage="No data available"
                    tableClassName="text-sm"
                    className="rounded-xl"
                  />
                </div>
              ) : (
                <p className="mt-5">No data available</p>
              )}
            </>
          )}

          {activeTab === 'extra' && (
            <div className="mt-5">
              {warehouseLoading ? (
                <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-slate-500">
                  Uploading...
                </div>
              ) : warehouseData.length > 0 ? (
                <DataTable<Record<string, any>>
                  columns={warehouseTableColumns}
                  data={warehouseData}
                  paginate={true}
                  pageSize={10}
                  stickyHeader
                  scrollY={false}
                  maxHeight="auto"
                  emptyMessage="No warehouse data available"
                  tableClassName="text-sm"
                  className="rounded-xl"
                />
              ) : (
                <div className="rounded-xl border border-slate-200 bg-white p-6 text-sm text-slate-500">
                  Upload a warehouse file to view data here.
                </div>
              )}
            </div>
          )}
        </>
      </PreviewLockedSection>

      {showWarehouseUpload && (
        <div
          onClick={() => setShowWarehouseUpload(false)}
          className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
          >
            <button
              onClick={() => setShowWarehouseUpload(false)}
              type="button"
              className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
            >
              &times;
            </button>

            <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar lg:p-9">
              <WarehouseMultiCountryUpload
                countryName={countryName}
                onClose={() => setShowWarehouseUpload(false)}
                onComplete={() => {
                  setShowWarehouseUpload(false);
                  void fetchWarehouseData();
                  setModalMessage("Warehouse file uploaded successfully");
                  setShowModal(true);
                }}
              />
            </div>
          </div>
        </div>
      )}

      {showMultiuseCountry && (
        <div
          onClick={() => setShowMultiuseCountry(false)}
          className="fixed inset-0 z-[10001] flex items-center justify-center bg-black/50 p-4"
        >
          <div
            onClick={(e) => e.stopPropagation()}
            className="relative m-4 w-full max-w-[500px] rounded-xl border border-[#D9D9D9] bg-white shadow-[6px_6px_7px_0px_#00000026]"
          >
            <button
              onClick={() => setShowMultiuseCountry(false)}
              type="button"
              className="absolute right-4 top-3 z-10 text-2xl leading-none text-neutral-500 hover:text-neutral-800"
            >
              &times;
            </button>

            <div className="relative w-full rounded-xl bg-white/30 p-4 no-scrollbar lg:p-9">
              <SkuMultiuseCountryUpload
                onClose={() => setShowMultiuseCountry(false)}
                onComplete={() => {
                  setShowMultiuseCountry(false);
                  void fetchSkuData();
                }}
              />
            </div>
          </div>
        </div>
      )}

      <Modalmsg
        show={showModal}
        message={modalMessage}
        onClose={() => setShowModal(false)}
        onCancel={() => setShowModal(false)}
      />
    </div>
  );
}