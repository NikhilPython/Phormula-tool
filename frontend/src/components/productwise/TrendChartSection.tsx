// // // // components/productwise/TrendChartSection.tsx
// // // "use client";

// // // import React, { useMemo, useRef, useState } from "react";
// // // import dynamic from "next/dynamic";
// // // import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
// // // import {
// // //   CountryKey,
// // //   formatCountryLabel,
// // //   getCountryColor,
// // // } from "./productwiseHelpers";

// // // import ExcelJS from "exceljs";
// // // import { saveAs } from "file-saver";
// // // import SegmentedToggle from "../ui/SegmentedToggle";

// // // const Line = dynamic(() => import("react-chartjs-2").then((m) => m.Line), {
// // //   ssr: false,
// // // });

// // // type TrendTab = "sales_cm1" | "units";

// // // interface TrendChartSectionProps {
// // //   productname: string;
// // //   title: string; // e.g. Classic (Year'25)
// // //   chartDataList: any[];
// // //   chartOptions: any;
// // //   nonEmptyCountriesFromApi: CountryKey[];
// // //   selectedCountries: Record<CountryKey, boolean>;
// // //   onToggleCountry: (country: CountryKey) => void;
// // // }

// // // const TrendChartSection: React.FC<TrendChartSectionProps> = ({
// // //   productname,
// // //   title,
// // //   chartDataList,
// // //   chartOptions,
// // //   nonEmptyCountriesFromApi,
// // //   selectedCountries,
// // //   onToggleCountry,
// // // }) => {
// // //   const [activeTab, setActiveTab] = useState<TrendTab>("sales_cm1");

// // //   // ref to the chart instance
// // //   const chartRef = useRef<any>(null);

// // //   // ---------- Build chart data based on active tab ----------
// // //   const processedChartData = useMemo(() => {
// // //     if (!chartDataList) return null;

// // //     const styleDatasetsByLabel = (datasets: any[] = []) =>
// // //       datasets.map((ds: any) => {
// // //         const label = (ds.label || "").toString().toLowerCase();
// // //         const isCm1OrProfit =
// // //           label.includes("cm1") ||
// // //           label.includes("cm 1") ||
// // //           label.includes("cm-1") ||
// // //           label.includes("profit");

// // //         return {
// // //           ...ds,
// // //           fill: false,
// // //           borderDash: isCm1OrProfit ? [6, 6] : [], // dotted for CM1/Profit
// // //         };
// // //       });

// // //     // TAB 1: Net Sales + CM1
// // //     if (activeTab === "sales_cm1") {
// // //       const netSalesData = chartDataList[0]; // Net Sales
// // //       const cm1Data = chartDataList[2]; // CM1 Profit

// // //       if (!netSalesData) return null;

// // //       if (!cm1Data) {
// // //         return {
// // //           ...netSalesData,
// // //           datasets: styleDatasetsByLabel(netSalesData.datasets || []),
// // //         };
// // //       }

// // //       const labels = netSalesData.labels;

// // //       const netSalesDatasets = (netSalesData.datasets || []).map((ds: any) => {
// // //         const label = (ds.label || "").toString().toLowerCase();
// // //         const isCm1OrProfit =
// // //           label.includes("cm1") ||
// // //           label.includes("cm 1") ||
// // //           label.includes("cm-1") ||
// // //           label.includes("profit");

// // //         return {
// // //           ...ds,
// // //           fill: false,
// // //           borderDash: isCm1OrProfit ? [6, 6] : [], // usually solid for pure Net Sales
// // //         };
// // //       });

// // //       const cm1Datasets = (cm1Data.datasets || []).map((ds: any) => ({
// // //         ...ds,
// // //         fill: false,
// // //         borderDash: [6, 6],
// // //       }));

// // //       return {
// // //         ...netSalesData,
// // //         labels,
// // //         datasets: [...netSalesDatasets, ...cm1Datasets],
// // //       };
// // //     }

// // //     // TAB 2: Units
// // //     if (activeTab === "units") {
// // //       const unitsData = chartDataList[1];
// // //       if (!unitsData) return null;

// // //       return {
// // //         ...unitsData,
// // //         datasets: styleDatasetsByLabel(unitsData.datasets || []),
// // //       };
// // //     }

// // //     return null;
// // //   }, [chartDataList, activeTab]);

// // //   const processedChartOptions = useMemo(() => chartOptions, [chartOptions]);

// // //   const getTitleByTab = () =>
// // //     activeTab === "sales_cm1"
// // //       ? "Net Sales + CM1 Profit Trend"
// // //       : "Units Trend";

// // //   // ---------- DOWNLOAD: Excel + chart image ----------
// // //   const handleDownload = async () => {
// // //     try {
// // //       if (!processedChartData) return;

// // //       // 1) Get chart image as data URL
// // //       let imageDataUrl: string | undefined;
// // //       const chart = chartRef.current;

// // //       if (chart) {
// // //         if (typeof chart.toBase64Image === "function") {
// // //           imageDataUrl = chart.toBase64Image();
// // //         } else if (chart.canvas && chart.canvas.toDataURL) {
// // //           imageDataUrl = chart.canvas.toDataURL("image/png");
// // //         }
// // //       }

// // //       // 2) Create Excel workbook
// // //       const workbook = new ExcelJS.Workbook();
// // //       const sheet = workbook.addWorksheet("Performance");

// // //       // Optional: title row
// // //       sheet.mergeCells("A1", "E1");
// // //       const titleCell = sheet.getCell("A1");
// // //       titleCell.value = `${getTitleByTab()} `;
// // //       titleCell.font = { bold: true, size: 14 };
// // //       titleCell.alignment = { vertical: "middle", horizontal: "center" };
// // //       sheet.getRow(1).height = 24;

// // //       let currentRow = 3;

// // //       // 3) Add chart image if we have it
// // //       if (imageDataUrl) {
// // //         const base64 = imageDataUrl.replace(
// // //           /^data:image\/(png|jpe?g);base64,/,
// // //           ""
// // //         );

// // //         const imgId = workbook.addImage({
// // //           base64,
// // //           extension: "png",
// // //         });

// // //         // Place image in the sheet
// // //         sheet.addImage(imgId, {
// // //           tl: { col: 0, row: currentRow - 1 }, // row is 0-based here
// // //           ext: { width: 900, height: 400 },
// // //         });

// // //         currentRow += 22; // leave space below image
// // //       }

// // //       // 4) Dump data table under the image
// // //       const labels: string[] = (processedChartData as any).labels || [];
// // //       const datasets: any[] = (processedChartData as any).datasets || [];

// // //       if (labels.length && datasets.length) {
// // //         // Header row
// // //         const headerRowValues = ["Month", ...datasets.map((d) => d.label)];
// // //         const headerRow = sheet.getRow(currentRow);
// // //         headerRow.values = headerRowValues;
// // //         headerRow.font = { bold: true };
// // //         headerRow.alignment = { horizontal: "center" };
// // //         currentRow += 1;

// // //         // Data rows
// // //         labels.forEach((label, idx) => {
// // //           const row = sheet.getRow(currentRow);
// // //           const values = [
// // //             label,
// // //             ...datasets.map((d) => d.data?.[idx] ?? null),
// // //           ];
// // //           row.values = values;
// // //           currentRow += 1;
// // //         });

// // //         // Make columns a bit wider
// // //         sheet.columns.forEach((col) => {
// // //           if (!col.width || col.width < 12) col.width = 12;
// // //         });
// // //       }

// // //       // 5) Save workbook
// // //       const buffer = await workbook.xlsx.writeBuffer();
// // //       const blob = new Blob([buffer], {
// // //         type:
// // //           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
// // //       });
// // //       const filename = `${productname}-${getTitleByTab()}.xlsx`;
// // //       saveAs(blob, filename);
// // //     } catch (err) {
// // //       console.error("Failed to export Excel + chart image", err);
// // //     }
// // //   };

// // //   return (
// // //     <div className="w-full rounded-md border border-charcoal-500 bg-[#D9D9D933] p-4 sm:p-5 shadow-sm">
// // //       <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
// // //         {/* LEFT: Title + country toggles */}
// // //         <div className="flex-1">
// // //           <h3 className="m-0 text-xl font-bold text-[#414042]">
// // //             {getTitleByTab()} -{" "}
// // //             <b className="text-green-500 capitalize">
// // //               {productname} ({title})
// // //             </b>
// // //           </h3>

// // //           <p className="mt-1 text-xs sm:text-sm text-gray-500">
// // //             Yearly performance comparison across regions
// // //           </p>

// // //           <div className="my-4 flex flex-wrap items-center gap-3">
// // //             {["global", ...nonEmptyCountriesFromApi].map((country) => {
// // //               const color = getCountryColor(country as CountryKey);
// // //               const isChecked = selectedCountries[country as CountryKey] ?? true;
// // //               const label = formatCountryLabel(country as CountryKey);

// // //               return (
// // //                 <label
// // //                   key={country}
// // //                   className={[
// // //                     "shrink-0",
// // //                     "flex items-center gap-1 sm:gap-1.5",
// // //                     "font-semibold select-none whitespace-nowrap",
// // //                     "text-[9px] sm:text-[10px] md:text-[11px] lg:text-xs xl:text-sm",
// // //                     "text-charcoal-500",
// // //                     isChecked ? "opacity-100" : "opacity-40",
// // //                     "cursor-pointer",
// // //                   ].join(" ")}
// // //                   onClick={() => onToggleCountry(country as CountryKey)}
// // //                 >
// // //                   <span
// // //                     className="
// // //                       flex items-center justify-center
// // //                       h-3 w-3 sm:h-3.5 sm:w-3.5
// // //                       rounded-sm border transition
// // //                     "
// // //                     style={{
// // //                       borderColor: color,
// // //                       backgroundColor: isChecked ? color : "white",
// // //                     }}
// // //                   >
// // //                     {isChecked && (
// // //                       <svg
// // //                         viewBox="0 0 24 24"
// // //                         width="14"
// // //                         height="14"
// // //                         className="text-white"
// // //                       >
// // //                         <path
// // //                           fill="currentColor"
// // //                           d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
// // //                         />
// // //                       </svg>
// // //                     )}
// // //                   </span>

// // //                   <span className="text-charcoal-500">{label}</span>
// // //                 </label>
// // //               );
// // //             })}
// // //           </div>
// // //         </div>

// // //         {/* RIGHT: Segmented toggle + Download button */}
// // //         <div className="flex items-center gap-3">
// // //           <SegmentedToggle<TrendTab>
// // //             value={activeTab}
// // //             onChange={setActiveTab}
// // //             textSizeClass="text-xs sm:text-sm"
// // //             options={[
// // //               { value: "sales_cm1", label: "Sales & CM1 Profit" },
// // //               { value: "units", label: "Units" },
// // //             ]}
// // //           />

// // //           <DownloadIconButton onClick={handleDownload} />
// // //         </div>
// // //       </div>

// // //       {/* CHART AREA */}
// // //       <div className="mt-4 h-[260px] sm:h-[300px] md:h-[340px] lg:h-[380px] xl:h-[420px]">
// // //         {processedChartData ? (
// // //           <Line
// // //             ref={chartRef}
// // //             data={processedChartData as any}
// // //             options={processedChartOptions as any}
// // //             style={{ width: "100%", height: "100%" }}
// // //           />
// // //         ) : (
// // //           <p className="flex h-full items-center justify-center">
// // //             No chart data available.
// // //           </p>
// // //         )}
// // //       </div>
// // //     </div>
// // //   );
// // // };

// // // export default TrendChartSection;













































// // // components/productwise/TrendChartSection.tsx
// // "use client";

// // import React, { useMemo, useRef, useState } from "react";
// // import dynamic from "next/dynamic";
// // import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
// // import {
// //   CountryKey,
// //   formatCountryLabel,
// //   getCountryColor,
// // } from "./productwiseHelpers";

// // import ExcelJS from "exceljs";
// // import { saveAs } from "file-saver";
// // import SegmentedToggle from "../ui/SegmentedToggle";

// // const Line = dynamic(() => import("react-chartjs-2").then((m) => m.Line), {
// //   ssr: false,
// // });

// // type TrendTab = "sales_cm1" | "units";

// // interface TrendChartSectionProps {
// //   productname: string;
// //   title: string; // e.g. Classic (Year'25)
// //   chartDataList: any[];
// //   chartOptions: any;
// //   nonEmptyCountriesFromApi: CountryKey[];
// //   selectedCountries: Record<CountryKey, boolean>;
// //   onToggleCountry: (country: CountryKey) => void;
// // }

// // const TrendChartSection: React.FC<TrendChartSectionProps> = ({
// //   productname,
// //   title,
// //   chartDataList,
// //   chartOptions,
// //   nonEmptyCountriesFromApi,
// //   selectedCountries,
// //   onToggleCountry,
// // }) => {
// //   const [activeTab, setActiveTab] = useState<TrendTab>("sales_cm1");

// //   // ref to the chart instance
// //   const chartRef = useRef<any>(null);

// //   // ---------- Build chart data based on active tab ----------
// //   const processedChartData = useMemo(() => {
// //     if (!chartDataList) return null;

// //     const styleDatasetsByLabel = (datasets: any[] = []) =>
// //       datasets.map((ds: any) => {
// //         const label = (ds.label || "").toString().toLowerCase();
// //         const isCm1OrProfit =
// //           label.includes("cm1") ||
// //           label.includes("cm 1") ||
// //           label.includes("cm-1") ||
// //           label.includes("profit");

// //         return {
// //           ...ds,
// //           fill: false,
// //           borderDash: isCm1OrProfit ? [6, 6] : [], // dotted for CM1/Profit
// //         };
// //       });

// //     // TAB 1: Net Sales + CM1
// //     if (activeTab === "sales_cm1") {
// //       const netSalesData = chartDataList[0]; // Net Sales
// //       const cm1Data = chartDataList[2]; // CM1 Profit

// //       if (!netSalesData) return null;

// //       if (!cm1Data) {
// //         return {
// //           ...netSalesData,
// //           datasets: styleDatasetsByLabel(netSalesData.datasets || []),
// //         };
// //       }

// //       const labels = netSalesData.labels;

// //       const netSalesDatasets = (netSalesData.datasets || []).map((ds: any) => {
// //         const label = (ds.label || "").toString().toLowerCase();
// //         const isCm1OrProfit =
// //           label.includes("cm1") ||
// //           label.includes("cm 1") ||
// //           label.includes("cm-1") ||
// //           label.includes("profit");

// //         return {
// //           ...ds,
// //           fill: false,
// //           borderDash: isCm1OrProfit ? [6, 6] : [], // usually solid for pure Net Sales
// //         };
// //       });

// //       const cm1Datasets = (cm1Data.datasets || []).map((ds: any) => ({
// //         ...ds,
// //         fill: false,
// //         borderDash: [6, 6],
// //       }));

// //       return {
// //         ...netSalesData,
// //         labels,
// //         datasets: [...netSalesDatasets, ...cm1Datasets],
// //       };
// //     }

// //     // TAB 2: Units
// //     if (activeTab === "units") {
// //       const unitsData = chartDataList[1];
// //       if (!unitsData) return null;

// //       return {
// //         ...unitsData,
// //         datasets: styleDatasetsByLabel(unitsData.datasets || []),
// //       };
// //     }

// //     return null;
// //   }, [chartDataList, activeTab]);

// //   const processedChartOptions = useMemo(() => chartOptions, [chartOptions]);

// //   const getTitleByTab = () =>
// //     activeTab === "sales_cm1"
// //       ? "Net Sales + CM1 Profit Trend"
// //       : "Units Trend";

// //   // ---------- DOWNLOAD: Excel + chart image ----------
// //   const handleDownload = async () => {
// //     try {
// //       if (!processedChartData) return;

// //       // 1) Get chart image as data URL
// //       let imageDataUrl: string | undefined;
// //       const chart = chartRef.current;

// //       if (chart) {
// //         if (typeof chart.toBase64Image === "function") {
// //           imageDataUrl = chart.toBase64Image();
// //         } else if (chart.canvas && chart.canvas.toDataURL) {
// //           imageDataUrl = chart.canvas.toDataURL("image/png");
// //         }
// //       }

// //       // 2) Create Excel workbook
// //       const workbook = new ExcelJS.Workbook();
// //       const sheet = workbook.addWorksheet("Performance");

// //       // Optional: title row
// //       sheet.mergeCells("A1", "E1");
// //       const titleCell = sheet.getCell("A1");
// //       titleCell.value = `${getTitleByTab()} `;
// //       titleCell.font = { bold: true, size: 14 };
// //       titleCell.alignment = { vertical: "middle", horizontal: "center" };
// //       sheet.getRow(1).height = 24;

// //       let currentRow = 3;

// //       // 3) Add chart image if we have it
// //       if (imageDataUrl) {
// //         const base64 = imageDataUrl.replace(
// //           /^data:image\/(png|jpe?g);base64,/,
// //           ""
// //         );

// //         const imgId = workbook.addImage({
// //           base64,
// //           extension: "png",
// //         });

// //         // Place image in the sheet
// //         sheet.addImage(imgId, {
// //           tl: { col: 0, row: currentRow - 1 }, // row is 0-based here
// //           ext: { width: 900, height: 400 },
// //         });

// //         currentRow += 22; // leave space below image
// //       }

// //       // 4) Dump data table under the image
// //       const labels: string[] = (processedChartData as any).labels || [];
// //       const datasets: any[] = (processedChartData as any).datasets || [];

// //       if (labels.length && datasets.length) {
// //         // Header row
// //         const headerRowValues = ["Month", ...datasets.map((d) => d.label)];
// //         const headerRow = sheet.getRow(currentRow);
// //         headerRow.values = headerRowValues;
// //         headerRow.font = { bold: true };
// //         headerRow.alignment = { horizontal: "center" };
// //         currentRow += 1;

// //         // Data rows
// //         labels.forEach((label, idx) => {
// //           const row = sheet.getRow(currentRow);
// //           const values = [
// //             label,
// //             ...datasets.map((d) => d.data?.[idx] ?? null),
// //           ];
// //           row.values = values;
// //           currentRow += 1;
// //         });

// //         // Make columns a bit wider
// //         sheet.columns.forEach((col) => {
// //           if (!col.width || col.width < 12) col.width = 12;
// //         });
// //       }

// //       // 5) Save workbook
// //       const buffer = await workbook.xlsx.writeBuffer();
// //       const blob = new Blob([buffer], {
// //         type:
// //           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
// //       });
// //       const filename = `${productname}-${getTitleByTab()}.xlsx`;
// //       saveAs(blob, filename);
// //     } catch (err) {
// //       console.error("Failed to export Excel + chart image", err);
// //     }
// //   };

// //   return (
// //     <div className="w-full rounded-md border border-charcoal-500 bg-[#D9D9D933] p-4 sm:p-5 shadow-sm">
// //       <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
// //         {/* LEFT: Title + country toggles */}
// //         <div className="flex-1">
// //           <h3 className="m-0 text-xl font-bold text-[#414042]">
// //             {getTitleByTab()} -{" "}
// //             <b className="text-green-500 capitalize">
// //               {productname} ({title})
// //             </b>
// //           </h3>

// //           <p className="mt-1 text-xs sm:text-sm text-gray-500">
// //             Yearly performance comparison across regions
// //           </p>

// //           <div className="my-4 flex flex-wrap items-center gap-3">
// //             {["global", ...nonEmptyCountriesFromApi].map((country) => {
// //               const color = getCountryColor(country as CountryKey);
// //               const isChecked =
// //                 selectedCountries[country as CountryKey] ?? true;
// //               const label = formatCountryLabel(country as CountryKey);

// //               return (
// //                 <label
// //                   key={country}
// //                   className={[
// //                     "shrink-0",
// //                     "flex items-center gap-1 sm:gap-1.5",
// //                     "font-semibold select-none whitespace-nowrap",
// //                     "text-[9px] sm:text-[10px] md:text-[11px] lg:text-xs xl:text-sm",
// //                     "text-charcoal-500",
// //                     isChecked ? "opacity-100" : "opacity-40",
// //                     "cursor-pointer",
// //                   ].join(" ")}
// //                   onClick={() => onToggleCountry(country as CountryKey)}
// //                 >
// //                   <span
// //                     className="
// //                       flex items-center justify-center
// //                       h-3 w-3 sm:h-3.5 sm:w-3.5
// //                       rounded-sm border transition
// //                     "
// //                     style={{
// //                       borderColor: color,
// //                       backgroundColor: isChecked ? color : "white",
// //                     }}
// //                   >
// //                     {isChecked && (
// //                       <svg
// //                         viewBox="0 0 24 24"
// //                         width="14"
// //                         height="14"
// //                         className="text-white"
// //                       >
// //                         <path
// //                           fill="currentColor"
// //                           d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
// //                         />
// //                       </svg>
// //                     )}
// //                   </span>

// //                   <span className="text-charcoal-500">{label}</span>
// //                 </label>
// //               );
// //             })}
// //           </div>
// //         </div>

// //         {/* RIGHT: Segmented toggle (download moved to bottom) */}
// //         <div className="flex items-center gap-3">
// //           <SegmentedToggle<TrendTab>
// //             value={activeTab}
// //             onChange={setActiveTab}
// //             textSizeClass="text-xs sm:text-sm"
// //             options={[
// //               { value: "sales_cm1", label: "Sales & CM1 Profit" },
// //               { value: "units", label: "Units" },
// //             ]}
// //           />
// //         </div>
// //       </div>

// //       {/* CHART AREA */}
// //       <div className="mt-4 h-[260px] sm:h-[300px] md:h-[340px] lg:h-[380px] xl:h-[420px]">
// //         {processedChartData ? (
// //           <Line
// //             ref={chartRef}
// //             data={processedChartData as any}
// //             options={processedChartOptions as any}
// //             style={{ width: "100%", height: "100%" }}
// //           />
// //         ) : (
// //           <p className="flex h-full items-center justify-center">
// //             No chart data available.
// //           </p>
// //         )}
// //       </div>

// //       {/* BOTTOM: custom legend + download button */}
// //       <div className="mt-4 flex items-center justify-between">
// //         {/* Left: legend for line styles */}
// //         {activeTab === "sales_cm1" ? (
// //           <div className="flex flex-wrap items-center gap-4 text-xs sm:text-sm text-gray-700">
// //             <div className="flex items-center gap-2">
// //               <span className="h-[2px] w-8 bg-gray-800" />
// //               <span>Net Sales</span>
// //             </div>
// //             <div className="flex items-center gap-2">
// //               <span className="w-8 border-t border-dashed border-gray-800" />
// //               <span>CM1 Profit</span>
// //             </div>
// //           </div>
// //         ) : (
// //           <div className="flex items-center gap-2 text-xs sm:text-sm text-gray-700">
// //             <span className="h-[2px] w-8 bg-gray-800" />
// //             <span>Units</span>
// //           </div>
// //         )}

// //         {/* Right: download icon at the bottom */}
// //         <DownloadIconButton onClick={handleDownload} />
// //       </div>
// //     </div>
// //   );
// // };

// // export default TrendChartSection;





























// // components/productwise/TrendChartSection.tsx
// "use client";

// import React, { useMemo, useRef, useState, useEffect } from "react";
// import dynamic from "next/dynamic";
// import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
// import {
//   CountryKey,
//   formatCountryLabel,
//   getCountryColor,
// } from "./productwiseHelpers";

// import ExcelJS from "exceljs";
// import { saveAs } from "file-saver";
// import SegmentedToggle from "../ui/SegmentedToggle";

// const Line = dynamic(() => import("react-chartjs-2").then((m) => m.Line), {
//   ssr: false,
// });

// type TrendTab = "sales_cm1" | "units";

// interface TrendChartSectionProps {
//   productname: string;
//   title: string; // e.g. Classic (Year'25)
//   chartDataList: any[];
//   chartOptions: any;
//   nonEmptyCountriesFromApi: CountryKey[];
//   selectedCountries: Record<CountryKey, boolean>;
//   onToggleCountry: (country: CountryKey) => void;

//   // 🔎 for search
//   authToken?: string | null;
//   onProductSelect?: (productName: string) => void;
// }

// const TrendChartSection: React.FC<TrendChartSectionProps> = ({
//   productname,
//   title,
//   chartDataList,
//   chartOptions,
//   nonEmptyCountriesFromApi,
//   selectedCountries,
//   onToggleCountry,
//   authToken,
//   onProductSelect,
// }) => {
//   const [activeTab, setActiveTab] = useState<TrendTab>("sales_cm1");

//   // ref to the chart instance
//   const chartRef = useRef<any>(null);

//   // ---------- SEARCH BAR STATE ----------
//   const [searchTerm, setSearchTerm] = useState<string>(productname || "");
//   const [suggestions, setSuggestions] = useState<string[]>([]);
//   const [searchLoading, setSearchLoading] = useState(false);
//   const [showSuggestions, setShowSuggestions] = useState(false);

//   // Fetch suggestions when user types
//   useEffect(() => {
//     const q = searchTerm.trim();
//     if (!q || q.length < 2 || !authToken) {
//       setSuggestions([]);
//       return;
//     }

//     const controller = new AbortController();
//     const id = setTimeout(async () => {
//       try {
//         setSearchLoading(true);
//         const res = await fetch(
//           `http://localhost:5000/Product_search?query=${encodeURIComponent(
//             q
//           )}`,
//           {
//             method: "GET",
//             headers: {
//               Authorization: `Bearer ${authToken}`,
//             },
//             signal: controller.signal,
//           }
//         );

//         const json = await res.json();
//         if (res.ok && json?.products) {
//           setSuggestions(json.products.map((p: any) => p.product_name));
//           setShowSuggestions(true);
//         } else {
//           setSuggestions([]);
//           setShowSuggestions(false);
//         }
//       } catch {
//         if (!controller.signal.aborted) {
//           setSuggestions([]);
//           setShowSuggestions(false);
//         }
//       } finally {
//         setSearchLoading(false);
//       }
//     }, 300); // small debounce

//     return () => {
//       clearTimeout(id);
//       controller.abort();
//     };
//   }, [searchTerm, authToken]);

//   const handleSelectSuggestion = (name: string) => {
//     setSearchTerm(name);
//     setShowSuggestions(false);
//     onProductSelect?.(name);
//   };

//   const handleSearchSubmit = (e: React.FormEvent) => {
//     e.preventDefault();
//     const q = searchTerm.trim();
//     if (!q) return;
//     onProductSelect?.(q);
//     setShowSuggestions(false);
//   };

//   // ---------- Build chart data based on active tab ----------
//   const processedChartData = useMemo(() => {
//     if (!chartDataList) return null;

//     const styleDatasetsByLabel = (datasets: any[] = []) =>
//       datasets.map((ds: any) => {
//         const label = (ds.label || "").toString().toLowerCase();
//         const isCm1OrProfit =
//           label.includes("cm1") ||
//           label.includes("cm 1") ||
//           label.includes("cm-1") ||
//           label.includes("profit");

//         return {
//           ...ds,
//           fill: false,
//           borderDash: isCm1OrProfit ? [6, 6] : [], // dotted for CM1/Profit
//         };
//       });

//     // TAB 1: Net Sales + CM1
//     if (activeTab === "sales_cm1") {
//       const netSalesData = chartDataList[0]; // Net Sales
//       const cm1Data = chartDataList[2]; // CM1 Profit

//       if (!netSalesData) return null;

//       if (!cm1Data) {
//         return {
//           ...netSalesData,
//           datasets: styleDatasetsByLabel(netSalesData.datasets || []),
//         };
//       }

//       const labels = netSalesData.labels;

//       const netSalesDatasets = (netSalesData.datasets || []).map((ds: any) => {
//         const label = (ds.label || "").toString().toLowerCase();
//         const isCm1OrProfit =
//           label.includes("cm1") ||
//           label.includes("cm 1") ||
//           label.includes("cm-1") ||
//           label.includes("profit");

//         return {
//           ...ds,
//           fill: false,
//           borderDash: isCm1OrProfit ? [6, 6] : [], // keep Net Sales solid
//         };
//       });

//       const cm1Datasets = (cm1Data.datasets || []).map((ds: any) => ({
//         ...ds,
//         fill: false,
//         borderDash: [6, 6],
//       }));

//       return {
//         ...netSalesData,
//         labels,
//         datasets: [...netSalesDatasets, ...cm1Datasets],
//       };
//     }

//     // TAB 2: Units
//     if (activeTab === "units") {
//       const unitsData = chartDataList[1];
//       if (!unitsData) return null;

//       return {
//         ...unitsData,
//         datasets: styleDatasetsByLabel(unitsData.datasets || []),
//       };
//     }

//     return null;
//   }, [chartDataList, activeTab]);

//   const processedChartOptions = useMemo(() => chartOptions, [chartOptions]);

//   const getTitleByTab = () =>
//     activeTab === "sales_cm1"
//       ? "Net Sales + CM1 Profit Trend"
//       : "Units Trend";

//   // ---------- DOWNLOAD: Excel + chart image ----------
//   const handleDownload = async () => {
//     try {
//       if (!processedChartData) return;

//       // 1) Get chart image as data URL
//       let imageDataUrl: string | undefined;
//       const chart = chartRef.current;

//       if (chart) {
//         if (typeof chart.toBase64Image === "function") {
//           imageDataUrl = chart.toBase64Image();
//         } else if (chart.canvas && chart.canvas.toDataURL) {
//           imageDataUrl = chart.canvas.toDataURL("image/png");
//         }
//       }

//       // 2) Create Excel workbook
//       const workbook = new ExcelJS.Workbook();
//       const sheet = workbook.addWorksheet("Performance");

//       // Optional: title row
//       sheet.mergeCells("A1", "E1");
//       const titleCell = sheet.getCell("A1");
//       titleCell.value = `${getTitleByTab()} `;
//       titleCell.font = { bold: true, size: 14 };
//       titleCell.alignment = { vertical: "middle", horizontal: "center" };
//       sheet.getRow(1).height = 24;

//       let currentRow = 3;

//       // 3) Add chart image if we have it
//       if (imageDataUrl) {
//         const base64 = imageDataUrl.replace(
//           /^data:image\/(png|jpe?g);base64,/,
//           ""
//         );

//         const imgId = workbook.addImage({
//           base64,
//           extension: "png",
//         });

//         // Place image in the sheet
//         sheet.addImage(imgId, {
//           tl: { col: 0, row: currentRow - 1 }, // row is 0-based here
//           ext: { width: 900, height: 400 },
//         });

//         currentRow += 22; // leave space below image
//       }

//       // 4) Dump data table under the image
//       const labels: string[] = (processedChartData as any).labels || [];
//       const datasets: any[] = (processedChartData as any).datasets || [];

//       if (labels.length && datasets.length) {
//         // Header row
//         const headerRowValues = ["Month", ...datasets.map((d) => d.label)];
//         const headerRow = sheet.getRow(currentRow);
//         headerRow.values = headerRowValues;
//         headerRow.font = { bold: true };
//         headerRow.alignment = { horizontal: "center" };
//         currentRow += 1;

//         // Data rows
//         labels.forEach((label, idx) => {
//           const row = sheet.getRow(currentRow);
//           const values = [
//             label,
//             ...datasets.map((d) => d.data?.[idx] ?? null),
//           ];
//           row.values = values;
//           currentRow += 1;
//         });

//         // Make columns a bit wider
//         sheet.columns.forEach((col) => {
//           if (!col.width || col.width < 12) col.width = 12;
//         });
//       }

//       // 5) Save workbook
//       const buffer = await workbook.xlsx.writeBuffer();
//       const blob = new Blob([buffer], {
//         type:
//           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
//       });
//       const filename = `${productname}-${getTitleByTab()}.xlsx`;
//       saveAs(blob, filename);
//     } catch (err) {
//       console.error("Failed to export Excel + chart image", err);
//     }
//   };

//   return (
//     <div className="w-full rounded-md border border-charcoal-500 bg-[#D9D9D933] p-4 sm:p-5 shadow-sm">
//       <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
//         {/* LEFT: Title + country toggles */}
//         <div className="flex-1">
//           <h3 className="m-0 text-xl font-bold text-[#414042]">
//             Product Name -{" "}
//             <b className="text-green-500 capitalize">{productname}</b>
//           </h3>

//           <p className="mt-1 text-xs sm:text-sm text-gray-500">
//             SKU Performance Analysis ({title}) – yearly performance comparison
//             across regions
//           </p>

//           <div className="my-4 flex flex-wrap items-center gap-3">
//             {["global", ...nonEmptyCountriesFromApi].map((country) => {
//               const color = getCountryColor(country as CountryKey);
//               const isChecked =
//                 selectedCountries[country as CountryKey] ?? true;
//               const label = formatCountryLabel(country as CountryKey);

//               return (
//                 <label
//                   key={country}
//                   className={[
//                     "shrink-0",
//                     "flex items-center gap-1 sm:gap-1.5",
//                     "font-semibold select-none whitespace-nowrap",
//                     "text-[9px] sm:text-[10px] md:text-[11px] lg:text-xs xl:text-sm",
//                     "text-charcoal-500",
//                     isChecked ? "opacity-100" : "opacity-40",
//                     "cursor-pointer",
//                   ].join(" ")}
//                   onClick={() => onToggleCountry(country as CountryKey)}
//                 >
//                   <span
//                     className="
//                       flex items-center justify-center
//                       h-3 w-3 sm:h-3.5 sm:w-3.5
//                       rounded-sm border transition
//                     "
//                     style={{
//                       borderColor: color,
//                       backgroundColor: isChecked ? color : "white",
//                     }}
//                   >
//                     {isChecked && (
//                       <svg
//                         viewBox="0 0 24 24"
//                         width="14"
//                         height="14"
//                         className="text-white"
//                       >
//                         <path
//                           fill="currentColor"
//                           d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
//                         />
//                       </svg>
//                     )}
//                   </span>

//                   <span className="text-charcoal-500">{label}</span>
//                 </label>
//               );
//             })}
//           </div>
//         </div>

//         {/* RIGHT: Search + Segmented toggle */}
//         <div className="flex flex-col items-stretch gap-2 sm:flex-row sm:items-center sm:gap-3">
//           {/* SEARCH BAR */}
//           <form
//             onSubmit={handleSearchSubmit}
//             className="relative w-full sm:w-64 md:w-72"
//           >
//             <input
//               type="text"
//               value={searchTerm}
//               onChange={(e) => setSearchTerm(e.target.value)}
//               onFocus={() => suggestions.length && setShowSuggestions(true)}
//               placeholder="Search products, SKUs..."
//               className="w-full rounded-md border border-gray-300 bg-white px-3 py-1.5 pr-8 text-xs sm:text-sm focus:outline-none focus:ring-1 focus:ring-[#5EA68E]"
//             />
//             <span className="pointer-events-none absolute right-2 top-1/2 -translate-y-1/2 text-gray-400">
//               {searchLoading ? (
//                 <i className="fa-solid fa-spinner fa-spin" />
//               ) : (
//                 <i className="fa-solid fa-magnifying-glass" />
//               )}
//             </span>

//             {/* Suggestions dropdown */}
//             {showSuggestions && suggestions.length > 0 && (
//               <div className="absolute z-20 mt-1 max-h-56 w-full overflow-auto rounded-md border border-gray-200 bg-white shadow-lg">
//                 {suggestions.map((s) => (
//                   <button
//                     key={s}
//                     type="button"
//                     onClick={() => handleSelectSuggestion(s)}
//                     className="block w-full px-3 py-1.5 text-left text-xs sm:text-sm hover:bg-gray-100"
//                   >
//                     {s}
//                   </button>
//                 ))}
//               </div>
//             )}
//           </form>

//           {/* TOGGLE */}
//           <SegmentedToggle<TrendTab>
//             value={activeTab}
//             onChange={setActiveTab}
//             textSizeClass="text-xs sm:text-sm"
//             options={[
//               { value: "sales_cm1", label: "Sales & CM1 Profit" },
//               { value: "units", label: "Units" },
//             ]}
//           />
//         </div>
//       </div>

//       {/* CHART AREA */}
//       <div className="mt-4 h-[260px] sm:h-[300px] md:h-[340px] lg:h-[380px] xl:h-[420px]">
//         {processedChartData ? (
//           <Line
//             ref={chartRef}
//             data={processedChartData as any}
//             options={processedChartOptions as any}
//             style={{ width: "100%", height: "100%" }}
//           />
//         ) : (
//           <p className="flex h-full items-center justify-center">
//             No chart data available.
//           </p>
//         )}
//       </div>

//       {/* BOTTOM: centered legend + download button */}
//       <div className="mt-4 flex items-center">
//         {/* Centered legend */}
//         <div className="flex-1 flex justify-center">
//           {activeTab === "sales_cm1" ? (
//             <div className="flex flex-wrap items-center gap-4 text-xs sm:text-sm text-gray-700">
//               <div className="flex items-center gap-2">
//                 <span className="h-[2px] w-10 bg-gray-800" />
//                 <span>Net Sales</span>
//               </div>
//               <div className="flex items-center gap-2">
//                 <span className="w-10 border-t border-dashed border-gray-800" />
//                 <span>CM1 Profit</span>
//               </div>
//             </div>
//           ) : (
//             <div className="flex items-center gap-2 text-xs sm:text-sm text-gray-700">
//               <span className="h-[2px] w-10 bg-gray-800" />
//               <span>Units</span>
//             </div>
//           )}
//         </div>

//         {/* Download icon bottom-right */}
//         <DownloadIconButton onClick={handleDownload} />
//       </div>
//     </div>
//   );
// };

// export default TrendChartSection;
















// components/productwise/TrendChartSection.tsx
"use client";

import React, { useMemo, useRef, useState } from "react";
import dynamic from "next/dynamic";
import DownloadIconButton from "@/components/ui/button/DownloadIconButton";
import {
  CountryKey,
  formatCountryLabel,
  getCountryColor,
} from "./productwiseHelpers";

import ExcelJS from "exceljs";
import { saveAs } from "file-saver";
import SegmentedToggle from "../ui/SegmentedToggle";
import ProductSearchDropdown from "@/components/products/ProductSearchDropdown";

const Line = dynamic(() => import("react-chartjs-2").then((m) => m.Line), {
  ssr: false,
});

type TrendTab = "sales_cm1" | "units";

interface TrendChartSectionProps {
  productname: string;
  title: string; // e.g. Year'25, Q1'25, etc.
  chartDataList: any[];
  chartOptions: any;
  nonEmptyCountriesFromApi: CountryKey[];
  selectedCountries: Record<CountryKey, boolean>;
  onToggleCountry: (country: CountryKey) => void;

  // for search dropdown
  authToken: string | null;
  onProductSelect: (productName: string) => void;
}

const TrendChartSection: React.FC<TrendChartSectionProps> = ({
  productname,
  title,
  chartDataList,
  chartOptions,
  nonEmptyCountriesFromApi,
  selectedCountries,
  onToggleCountry,
  authToken,
  onProductSelect,
}) => {
  const [activeTab, setActiveTab] = useState<TrendTab>("sales_cm1");

  // ref to the chart instance
  const chartRef = useRef<any>(null);

  // ---------- Build chart data based on active tab ----------
  const processedChartData = useMemo(() => {
    if (!chartDataList) return null;

    const styleDatasetsByLabel = (datasets: any[] = []) =>
      datasets.map((ds: any) => {
        const label = (ds.label || "").toString().toLowerCase();
        const isCm1OrProfit =
          label.includes("cm1") ||
          label.includes("cm 1") ||
          label.includes("cm-1") ||
          label.includes("profit");

        return {
          ...ds,
          fill: false,
          borderDash: isCm1OrProfit ? [6, 6] : [], // dotted for CM1/Profit
        };
      });

    // TAB 1: Net Sales + CM1
    if (activeTab === "sales_cm1") {
      const netSalesData = chartDataList[0]; // Net Sales
      const cm1Data = chartDataList[2]; // CM1 Profit

      if (!netSalesData) return null;

      if (!cm1Data) {
        return {
          ...netSalesData,
          datasets: styleDatasetsByLabel(netSalesData.datasets || []),
        };
      }

      const labels = netSalesData.labels;

      const netSalesDatasets = (netSalesData.datasets || []).map((ds: any) => {
        const label = (ds.label || "").toString().toLowerCase();
        const isCm1OrProfit =
          label.includes("cm1") ||
          label.includes("cm 1") ||
          label.includes("cm-1") ||
          label.includes("profit");

        return {
          ...ds,
          fill: false,
          borderDash: isCm1OrProfit ? [6, 6] : [], // keep Net Sales solid
        };
      });

      const cm1Datasets = (cm1Data.datasets || []).map((ds: any) => ({
        ...ds,
        fill: false,
        borderDash: [6, 6],
      }));

      return {
        ...netSalesData,
        labels,
        datasets: [...netSalesDatasets, ...cm1Datasets],
      };
    }

    // TAB 2: Units
    if (activeTab === "units") {
      const unitsData = chartDataList[1];
      if (!unitsData) return null;

      return {
        ...unitsData,
        datasets: styleDatasetsByLabel(unitsData.datasets || []),
      };
    }

    return null;
  }, [chartDataList, activeTab]);

  const processedChartOptions = useMemo(() => chartOptions, [chartOptions]);

  const getTitleByTab = () =>
    activeTab === "sales_cm1"
      ? "Net Sales + CM1 Profit Trend"
      : "Units Trend";

  // ---------- DOWNLOAD: Excel + chart image ----------
  const handleDownload = async () => {
    try {
      if (!processedChartData) return;

      // 1) Get chart image as data URL
      let imageDataUrl: string | undefined;
      const chart = chartRef.current;

      if (chart) {
        if (typeof chart.toBase64Image === "function") {
          imageDataUrl = chart.toBase64Image();
        } else if (chart.canvas && chart.canvas.toDataURL) {
          imageDataUrl = chart.canvas.toDataURL("image/png");
        }
      }

      // 2) Create Excel workbook
      const workbook = new ExcelJS.Workbook();
      const sheet = workbook.addWorksheet("Performance");

      // Optional: title row
      sheet.mergeCells("A1", "E1");
      const titleCell = sheet.getCell("A1");
      titleCell.value = `${getTitleByTab()} `;
      titleCell.font = { bold: true, size: 14 };
      titleCell.alignment = { vertical: "middle", horizontal: "center" };
      sheet.getRow(1).height = 24;

      let currentRow = 3;

      // 3) Add chart image if we have it
      if (imageDataUrl) {
        const base64 = imageDataUrl.replace(
          /^data:image\/(png|jpe?g);base64,/,
          ""
        );

        const imgId = workbook.addImage({
          base64,
          extension: "png",
        });

        // Place image in the sheet
        sheet.addImage(imgId, {
          tl: { col: 0, row: currentRow - 1 }, // row is 0-based here
          ext: { width: 900, height: 400 },
        });

        currentRow += 22; // leave space below image
      }

      // 4) Dump data table under the image
      const labels: string[] = (processedChartData as any).labels || [];
      const datasets: any[] = (processedChartData as any).datasets || [];

      if (labels.length && datasets.length) {
        // Header row
        const headerRowValues = ["Month", ...datasets.map((d) => d.label)];
        const headerRow = sheet.getRow(currentRow);
        headerRow.values = headerRowValues;
        headerRow.font = { bold: true };
        headerRow.alignment = { horizontal: "center" };
        currentRow += 1;

        // Data rows
        labels.forEach((label, idx) => {
          const row = sheet.getRow(currentRow);
          const values = [
            label,
            ...datasets.map((d) => d.data?.[idx] ?? null),
          ];
          row.values = values;
          currentRow += 1;
        });

        // Make columns a bit wider
        sheet.columns.forEach((col) => {
          if (!col.width || col.width < 12) col.width = 12;
        });
      }

      // 5) Save workbook
      const buffer = await workbook.xlsx.writeBuffer();
      const blob = new Blob([buffer], {
        type:
          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      });
      const filename = `${productname}-${getTitleByTab()}.xlsx`;
      saveAs(blob, filename);
    } catch (err) {
      console.error("Failed to export Excel + chart image", err);
    }
  };

  return (
    <div className="w-full rounded-md border border-charcoal-500 bg-[#D9D9D933] p-4 sm:p-5 shadow-sm">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        {/* LEFT: Title + country toggles */}
        <div className="flex-1">
          <h3 className="m-0 text-xl font-bold text-[#414042]">
            Product Name -{" "}
            <b className="text-green-500 capitalize">{productname}</b>
          </h3>

          {/* <p className="mt-1 text-xs sm:text-sm text-gray-500">
            SKU Performance Analysis ({title}) – yearly performance comparison
            across regions
          </p> */}

          <div className="my-4 flex flex-wrap items-center gap-3">
            {["global", ...nonEmptyCountriesFromApi].map((country) => {
              const color = getCountryColor(country as CountryKey);
              const isChecked =
                selectedCountries[country as CountryKey] ?? true;
              const label = formatCountryLabel(country as CountryKey);

              return (
                <label
                  key={country}
                  className={[
                    "shrink-0",
                    "flex items-center gap-1 sm:gap-1.5",
                    "font-semibold select-none whitespace-nowrap",
                    "text-[9px] sm:text-[10px] md:text-[11px] lg:text-xs xl:text-sm",
                    "text-charcoal-500",
                    isChecked ? "opacity-100" : "opacity-40",
                    "cursor-pointer",
                  ].join(" ")}
                  onClick={() => onToggleCountry(country as CountryKey)}
                >
                  <span
                    className="
                      flex items-center justify-center
                      h-3 w-3 sm:h-3.5 sm:w-3.5
                      rounded-sm border transition
                    "
                    style={{
                      borderColor: color,
                      backgroundColor: isChecked ? color : "white",
                    }}
                  >
                    {isChecked && (
                      <svg
                        viewBox="0 0 24 24"
                        width="14"
                        height="14"
                        className="text-white"
                      >
                        <path
                          fill="currentColor"
                          d="M20.285 6.709a1 1 0 0 0-1.414-1.414L9 15.168l-3.879-3.88a1 1 0 0 0-1.414 1.415l4.586 4.586a1 1 0 0 0 1.414 0l10-10Z"
                        />
                      </svg>
                    )}
                  </span>

                  <span className="text-charcoal-500">{label}</span>
                </label>
              );
            })}
          </div>
        </div>

        {/* RIGHT: Search dropdown + Segmented toggle */}
        <div className="flex flex-col items-stretch gap-2 sm:flex-row sm:items-center sm:gap-3">
          <div className="w-full sm:w-64 md:w-72">
            <ProductSearchDropdown
              authToken={authToken}
              onProductSelect={onProductSelect}
            />
          </div>

          <SegmentedToggle<TrendTab>
            value={activeTab}
            onChange={setActiveTab}
            textSizeClass="text-xs sm:text-sm"
            options={[
              { value: "sales_cm1", label: "Sales & CM1 Profit" },
              { value: "units", label: "Units" },
            ]}
          />
        </div>
      </div>

      {/* CHART AREA */}
      <div className="mt-4 h-[260px] sm:h-[300px] md:h-[340px] lg:h-[380px] xl:h-[420px]">
        {processedChartData ? (
          <Line
            ref={chartRef}
            data={processedChartData as any}
            options={processedChartOptions as any}
            style={{ width: "100%", height: "100%" }}
          />
        ) : (
          <p className="flex h-full items-center justify-center">
            No chart data available.
          </p>
        )}
      </div>

      {/* BOTTOM: centered legend + download button */}
      <div className="mt-4 flex items-center">
        {/* Centered legend */}
        <div className="flex-1 flex justify-center">
          {activeTab === "sales_cm1" ? (
            <div className="flex flex-wrap items-center gap-4 text-xs sm:text-sm text-gray-700">
              <div className="flex items-center gap-2">
                <span className="h-[2px] w-10 bg-gray-800" />
                <span>Net Sales</span>
              </div>
              <div className="flex items-center gap-2">
                <span className="w-10 border-t border-dashed border-gray-800" />
                <span>CM1 Profit</span>
              </div>
            </div>
          ) : (
            <div className="flex items-center gap-2 text-xs sm:text-sm text-gray-700">
              <span className="h-[2px] w-10 bg-gray-800" />
              <span>Units</span>
            </div>
          )}
        </div>

        {/* Download icon bottom-right */}
        <DownloadIconButton onClick={handleDownload} />
      </div>
    </div>
  );
};

export default TrendChartSection;
