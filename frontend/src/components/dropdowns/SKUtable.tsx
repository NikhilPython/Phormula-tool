"use client";

import React, { useEffect, useMemo, useState } from "react";
import * as XLSX from "xlsx";
import { jwtDecode } from "jwt-decode";
import SkuMultiCountryUpload from "../ui/modal/SkuMultiCountryUpload";
import Productinfoinpopup from "./Productinfoinpopup";
import Button from "../ui/button/Button";
import PageBreadcrumb from "../common/PageBreadCrumb";
import { FiDownload } from "react-icons/fi";
import DownloadIconButton from "../ui/button/DownloadIconButton";
import { FaCaretLeft, FaCaretRight } from "react-icons/fa";
import Tooltip from "./Tooltip";

/* ---------- Types ---------- */

type RangeType = "monthly" | "quarterly" | "yearly";

type SKUtableProps = {
    range: RangeType;
    month?: string;
    quarter?: string;
    year: string | number;
    countryName: string;
    /** 👇 NEW, only used when countryName === 'global' */
    homeCurrency?: string;
};



type TableRow = {
    product_name?: string;
    sku?: string;
    quantity?: number;
    asp?: number;
    ASP?: number; // sometimes keys differ in casing
    product_sales?: number;
    net_sales?: number;
    cost_of_unit_sold?: number;
    amazon_fee?: number;
    selling_fees?: number;
    fba_fees?: number;
    net_credits?: number;
    net_taxes?: number;
    profit?: number;
    profit_percentage?: number;
    unit_wise_profitability?: number;

    // totals row optional fields
    platform_fee?: number;
    rembursement_fee?: number;
    advertising_total?: number;
    shipment_charges?: number;
    reimbursement_vs_sales?: number;
    cm2_profit?: number;
    cm2_margins?: number;
    acos?: number;
    rembursment_vs_cm2_margins?: number;

    // sometimes API uses these
    Profit?: number;
    Net_Sales?: number;

    // mix values used for top/bottom
    profit_mix?: number;
    sales_mix?: number;
};

type Totals = {
    platform_fee: number;
    rembursement_fee: number;
    advertising_total: number;
    shipment_charges: number;
    reimbursement_vs_sales: number;
    cm2_profit: number;
    cm2_margins: number;
    acos: number;
    rembursment_vs_cm2_margins: number;
    profit: number;
    net_sales: number;
};

type JwtPayload = {
    user_id?: string | number;
    [k: string]: unknown;
};

/* ---------- Helpers ---------- */

const getCurrencySymbol = (codeOrCountry: string) => {
    switch (codeOrCountry.toLowerCase()) {
        case "uk":
        case "gb":
        case "gbp":
            return "£";
        case "india":
        case "in":
        case "inr":
            return "₹";
        case "us":
        case "usa":
        case "usd":
            return "$";
        case "europe":
        case "eu":
        case "eur":
            return "€";
        default:
            return "¤";
    }
};


const capitalizeFirstLetter = (str: string) =>
    str.charAt(0).toUpperCase() + str.slice(1).toLowerCase();

const convertToAbbreviatedMonth = (m?: string) =>
    m ? capitalizeFirstLetter(m).slice(0, 3) : "";

/* ---------- Component ---------- */
const getDisplayProductNameFromRow = (row: TableRow): string => {
    const name = row.product_name;

    // Treat "0" / 0 / empty as "no name"
    const hasName =
        name !== undefined &&
        name !== null &&
        name !== "" &&
        name !== "0";

    if (hasName) return String(name);

    if (row.sku !== undefined && row.sku !== null) {
        return String(row.sku);
    }

    return "";
};

const SKUtable: React.FC<SKUtableProps> = ({
    range,
    month = "",
    quarter = "",
    year,
    countryName,
    homeCurrency,
}) => {
    const [selectedProduct, setSelectedProduct] = useState<string | null>(null);
    const [showModal, setShowModal] = useState(false);
    const [noDataFound, setNoDataFound] = useState(false);
    const [showModal2, setShowModal2] = useState(false);

    const [tableData, setTableData] = useState<TableRow[]>([]);
    const [totals, setTotals] = useState<Totals>({
        platform_fee: 0,
        rembursement_fee: 0,
        advertising_total: 0,
        shipment_charges: 0,
        reimbursement_vs_sales: 0,
        cm2_profit: 0,
        cm2_margins: 0,
        acos: 0,
        rembursment_vs_cm2_margins: 0,
        profit: 0,
        net_sales: 0,
    });
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [showamazonfee, setshowamazonfee] = useState(false);
    const [showprofit, setshowprofit] = useState(false);
    const [userData, setUserData] = useState<{ brand_name?: string; company_name?: string } | null>(null);

    // const currencySymbol = getCurrencySymbol(countryName || "");

    const isGlobalPage = countryName.toLowerCase() === "global";

    const currencySymbol = isGlobalPage
        ? getCurrencySymbol(homeCurrency || "usd")   // GLOBAL → home currency
        : getCurrencySymbol(countryName || "");      // Country route → country currency

    const yearShort =
        typeof year === "string" ? year.toString().slice(-2) : String(year).slice(-2);

    const quarterMapping: Record<string, string> = {
        Q1: "quarter1",
        Q2: "quarter2",
        Q3: "quarter3",
        Q4: "quarter4",
    };

    // decode user id from JWT
    const token = typeof window !== "undefined" ? localStorage.getItem("jwtToken") : null;
    const userid = useMemo(() => {
        if (!token) return "";
        try {
            const decoded = jwtDecode<JwtPayload>(token);
            return decoded?.user_id ?? "";
        } catch {
            return "";
        }
    }, [token]);

    // dummy table in case API has no data
    const dummyTableData: TableRow[] = [
        {
            product_name: "Sample Product A",
            quantity: 100,
            asp: 12.5,
            product_sales: 1250,
            net_sales: 1250,
            cost_of_unit_sold: 500,
            amazon_fee: 100,
            selling_fees: 60,
            fba_fees: 40,
            net_credits: 100,
            net_taxes: 50,
            profit: 700,
            profit_percentage: 56,
            unit_wise_profitability: 7,
        },
        {
            product_name: "Sample Product B",
            quantity: 80,
            asp: 10,
            product_sales: 800,
            net_sales: 800,
            cost_of_unit_sold: 300,
            amazon_fee: 80,
            selling_fees: 50,
            fba_fees: 30,
            net_credits: 70,
            net_taxes: 40,
            profit: 450,
            profit_percentage: 56.25,
            unit_wise_profitability: 5.625,
        },
        {
            product_name: "Sample Product C",
            quantity: 80,
            asp: 8,
            product_sales: 800,
            net_sales: 400,
            cost_of_unit_sold: 400,
            amazon_fee: 50,
            selling_fees: 80,
            fba_fees: 30,
            net_credits: 70,
            net_taxes: 54,
            profit: 310,
            profit_percentage: 56.25,
            unit_wise_profitability: 5.625,
        },
        {
            product_name: "Total",
            quantity: 80,
            asp: 10,
            product_sales: 800,
            net_sales: 800,
            cost_of_unit_sold: 300,
            amazon_fee: 80,
            selling_fees: 50,
            fba_fees: 30,
            net_credits: 70,
            net_taxes: 40,
            profit: 450,
            profit_percentage: 56.25,
            unit_wise_profitability: 5.625,
        },
    ];

    const CustomModal: React.FC<
        React.PropsWithChildren<{ onClose: () => void }>
    > = ({ onClose, children }) => {
        return (
            <div
                onClick={onClose}
                className="fixed inset-0 z-[1000] flex items-center justify-center bg-black/50"
            >
                <div
                    onClick={(e) => e.stopPropagation()}
                    className="relative flex h-[30vh] w-[30vw] flex-col items-center justify-between overflow-y-auto rounded-lg bg-white p-4"
                >
                    <div className="flex flex-1 flex-col items-center justify-center">
                        {children}
                    </div>
                </div>
            </div>
        );
    };

    // find asp key (asp or ASP)
    const aspKey = useMemo(() => {
        const first = tableData[0] || {};
        const k = Object.keys(first).find((key) => key.toLowerCase() === "asp");
        return k as keyof TableRow | undefined;
    }, [tableData]);

    // columns visible (controlled by toggles)
    const columnsToDisplay = useMemo(() => {
        const cols: (keyof TableRow | string | false)[] = [
            "product_name",
            "quantity",
            aspKey || "asp",
            "product_sales",
            "net_sales",
            "cost_of_unit_sold",
            "amazon_fee",
            showamazonfee && "selling_fees",
            showamazonfee && "fba_fees",
            "net_credits",
            "net_taxes",
            "profit",
            showprofit && "profit_percentage",
            showprofit && "unit_wise_profitability",
        ];
        return cols.filter(Boolean) as (keyof TableRow | string)[];
    }, [aspKey, showamazonfee, showprofit]);

    const totalTableColumns = 1 + columnsToDisplay.length;
    const summaryLabelColSpan = totalTableColumns - 1;

    /* --------- Fetch user data (brand/company names) --------- */
    useEffect(() => {
        const fetchUserData = async () => {
            if (!token) {
                setError("No token found. Please log in.");
                return;
            }
            try {
                const res = await fetch("http://127.0.0.1:5000/get_user_data", {
                    method: "GET",
                    headers: { Authorization: `Bearer ${token}` },
                    cache: "no-store",
                });
                if (!res.ok) {
                    const data = await res.json().catch(() => ({}));
                    setError(data?.error || "Something went wrong.");
                    return;
                }
                const data = (await res.json()) as { brand_name?: string; company_name?: string };
                setUserData(data);
            } catch {
                setError("Error fetching user data");
            }
        };
        fetchUserData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    /* --------- Fetch table data --------- */
    useEffect(() => {
        const fetchData = async () => {
            setLoading(true);
            setError(null);

            try {
                let response: Response | null = null;

                if (range === "monthly") {
                    const skuwiseFileName =
                        countryName.toLowerCase() === "global"
                            ? `skuwisemonthly_${userid}_${countryName}_${(month || "").toLowerCase()}${year}_table`
                            : `skuwisemonthly_${userid}_${countryName.toLowerCase()}_${(month || "").toLowerCase()}${year}`;

                    const url = new URL(`http://127.0.0.1:5000/skutableprofit/${skuwiseFileName}`);

                    // ✅ send these always (backend uses them if present)
                    url.searchParams.set("country", countryName);
                    url.searchParams.set("month", (month || "").toLowerCase());
                    url.searchParams.set("year", String(year));

                    // ✅ GLOBAL only
                    if (isGlobalPage && homeCurrency) {
                        url.searchParams.set("homeCurrency", homeCurrency.toLowerCase());
                    }

                    response = await fetch(url.toString(), {
                        method: "GET",
                        headers: token ? { Authorization: `Bearer ${token}` } : {},
                        cache: "no-store",
                    });
                }
                else if (range === "quarterly") {
                    const backendQuarter = quarterMapping[quarter] || "";

                    const url = new URL("http://127.0.0.1:5000/quarterlyskutable");
                    url.searchParams.set("quarter", backendQuarter);
                    url.searchParams.set("country", countryName);
                    url.searchParams.set("year", String(year));
                    url.searchParams.set("userid", String(userid)); // not used by backend, but harmless

                    // ✅ GLOBAL only
                    if (isGlobalPage && homeCurrency) {
                        url.searchParams.set("homeCurrency", homeCurrency.toLowerCase());
                    }

                    response = await fetch(url.toString(), {
                        method: "GET",
                        headers: token ? { Authorization: `Bearer ${token}` } : {},
                        cache: "no-store",
                    });
                }
                else if (range === "yearly") {
                    const url = new URL("http://127.0.0.1:5000/YearlySKU");
                    url.searchParams.set("country", countryName);
                    url.searchParams.set("year", String(year));

                    // ✅ GLOBAL only
                    if (isGlobalPage && homeCurrency) {
                        url.searchParams.set("homeCurrency", homeCurrency.toLowerCase());
                    }

                    response = await fetch(url.toString(), {
                        method: "GET",
                        headers: token ? { Authorization: `Bearer ${token}` } : {},
                        cache: "no-store",
                    });
                }

                if (!response || !response.ok) {
                    console.warn("Fetch failed. Showing dummy data.");
                    setNoDataFound(true);
                    setTableData(dummyTableData);
                    return;
                }

                const data = (await response.json()) as TableRow[] | unknown;

                if (Array.isArray(data) && data.length === 0) {
                    setNoDataFound(true);
                    setTableData(dummyTableData);
                    return;
                }

                if (Array.isArray(data)) {
                    setTableData(data);
                    setNoDataFound(false);

                    const lastRow = data[data.length - 1] || {};
                    const lastRowProfit = (lastRow.Profit as number) ?? (lastRow.profit as number) ?? 0;
                    const lastRowSales = (lastRow.Net_Sales as number) ?? (lastRow.net_sales as number) ?? 0;

                    setTotals({
                        platform_fee: parseFloat(String(lastRow.platform_fee ?? 0)),
                        rembursement_fee: parseFloat(String(lastRow.rembursement_fee ?? 0)),
                        advertising_total: parseFloat(String(lastRow.advertising_total ?? 0)),
                        shipment_charges: parseFloat(String(lastRow.shipment_charges ?? 0)),
                        reimbursement_vs_sales: parseFloat(
                            String(lastRow.reimbursement_vs_sales ?? 0)
                        ),
                        cm2_profit: parseFloat(String(lastRow.cm2_profit ?? 0)),
                        cm2_margins: parseFloat(String(lastRow.cm2_margins ?? 0)),
                        acos: parseFloat(String(lastRow.acos ?? 0)),
                        rembursment_vs_cm2_margins: parseFloat(
                            String(lastRow.rembursment_vs_cm2_margins ?? 0)
                        ),
                        profit: parseFloat(String(lastRowProfit ?? 0)),
                        net_sales: parseFloat(String(lastRowSales ?? 0)),
                    });
                } else {
                    // unexpected payload
                    setNoDataFound(true);
                    setTableData(dummyTableData);
                }
            } catch (err) {
                console.error("Error during fetch:", err);
                setNoDataFound(true);
                setTableData(dummyTableData);
            } finally {
                setLoading(false);
            }
        };

        if (countryName) fetchData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [countryName, range, month, quarter, year, token, userid]);

    /* --------- UI Handlers --------- */
    const handleProductClick = (product: string) => {
        setSelectedProduct(product);
        setShowModal(true);
    };
    const handleAmazonFeeClick = () => setshowamazonfee((p) => !p);
    const handleprofitClick = () => setshowprofit((p) => !p);

    /* --------- Top/Bottom helpers --------- */
    function getTop5Profitable(data: TableRow[]) {
        const rows = data.slice(0, -1);
        const top5 = [...rows]
            .sort((a, b) => (b.profit || 0) - (a.profit || 0))
            .slice(0, 5);

        const totalProfit = top5.reduce((s, r) => s + (r.profit || 0), 0);
        const totalProfitMix = top5.reduce((s, r) => s + (r.profit_mix || 0), 0);
        const totalSalesMix = top5.reduce((s, r) => s + (r.sales_mix || 0), 0);
        const totalUnitWise = top5.reduce(
            (s, r) => s + (r.unit_wise_profitability || 0),
            0
        );

        const formatted = top5.map((item) => ({
            product_name: getDisplayProductNameFromRow(item),
            profit: (item.profit || 0).toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            }),
            profitMix: (item.profit_mix || 0).toFixed(2),
            salesMix: (item.sales_mix || 0).toFixed(2),
            unit_wise_profitability: (item.unit_wise_profitability || 0).toFixed(2),
            cost_of_unit_sold: item.cost_of_unit_sold,
            sku: item.sku,
        }));

        return {
            rows: formatted,
            totals: {
                profit: totalProfit.toLocaleString(undefined, {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                }),
                profitMix: totalProfitMix.toFixed(2),
                salesMix: totalSalesMix.toFixed(2),
                unit_wise_profitability: totalUnitWise.toFixed(2),
            },
        };
    }


    function getBottom5Profitable(data: TableRow[]) {
        const rows = data.slice(0, -1);
        const bottom5 = [...rows]
            .sort((a, b) => (a.profit || 0) - (b.profit || 0))
            .slice(0, 5);

        const totalProfit = bottom5.reduce((s, r) => s + (r.profit || 0), 0);
        const totalProfitMix = bottom5.reduce((s, r) => s + (r.profit_mix || 0), 0);
        const totalSalesMix = bottom5.reduce((s, r) => s + (r.sales_mix || 0), 0);
        const totalUnitWise = bottom5.reduce(
            (s, r) => s + (r.unit_wise_profitability || 0),
            0
        );

        const formatted = bottom5.map((item) => ({
            product_name: getDisplayProductNameFromRow(item),
            profit: (item.profit || 0).toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            }),
            profitMix: (item.profit_mix || 0).toFixed(2),
            salesMix: (item.sales_mix || 0).toFixed(2),
            unit_wise_profitability: (item.unit_wise_profitability || 0).toFixed(2),
            cost_of_unit_sold: item.cost_of_unit_sold,
            sku: item.sku,
        }));

        return {
            rows: formatted,
            totals: {
                profit: totalProfit.toLocaleString(undefined, {
                    minimumFractionDigits: 2,
                    maximumFractionDigits: 2,
                }),
                profitMix: totalProfitMix.toFixed(2),
                salesMix: totalSalesMix.toFixed(2),
                unit_wise_profitability: totalUnitWise.toFixed(2),
            },
        };
    }

    const topData = useMemo(() => getTop5Profitable(tableData), [tableData]);
    const bottomData = useMemo(() => getBottom5Profitable(tableData), [tableData]);

    /* --------- Formatting helpers --------- */
    const formatValue = (value: unknown, key: string) => {
        if (key === "quantity") return value as number;

        if (typeof value === "number") {
            const noAbsKeys = ["net_taxes", "profit"];
            const base = noAbsKeys.includes(key) ? value : Math.abs(value);
            const formatted = base.toLocaleString(undefined, {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            });
            if (key === "profit_percentage") return `${formatted}%`;
            return formatted;
        }
        return value as string;
    };

    const getTitle = () => {
        if (range === "monthly") {
            return `Profit Breakup (SKU Level)`;
        } else if (range === "quarterly") {
            return `Profit Breakup (SKU Level)`;
        } else {
            return `Profit Breakup (SKU Level)`;
        }
    };

    const getTitle2 = () => {
        if (range === "monthly") {
            return `Profit Breakup (SKU Level)`;
        } else if (range === "quarterly") {
            return `Profit Breakup (SKU Level)`;
        } else {
            return `Profit Breakup (SKU Level)`;
        }
    };

    const getExtraRows = () => {
        const formattedCountry =
            countryName?.toLowerCase() === "global" ? "GLOBAL" : countryName?.toUpperCase();
        return [
            [`${userData?.brand_name || "N/A"}`],
            [`${userData?.company_name || "N/A"}`],
            [getTitle2()],
            [`Currency:  ${currencySymbol}`],
            [`Country: ${formattedCountry}`],
            [`Platform: Amazon`],
        ];
    };

    /* --------- Excel Download --------- */
    const handleDownloadExcel = () => {
        const wb = XLSX.utils.book_new();

        const columnsToDisplay2 = [
            "product_name",
            "quantity",
            "asp",
            "product_sales",
            "net_sales",
            "cost_of_unit_sold",
            "amazon_fee",
            "selling_fees",
            "fba_fees",
            "net_credits",
            "net_taxes",
            "profit",
            "profit_percentage",
            "unit_wise_profitability",
        ] as const;

        const percentageSummaryLabels = [
            "CM2 Margins",
            "TACoS (Total Advertising Cost of Sale)",
            "Reimbursement vs CM2 Margins",
            "Reimbursement vs Sales",
        ];

        const noAbsKeys = ["net_taxes", "profit"];

        const tableDataForExcel = tableData.map((row) => {
            const rowData: Record<string, string | number> = {};

            columnsToDisplay2.forEach((column) => {
                let value: any = row[column];

                // Fallback: if product_name missing, use sku
                if (column === "product_name") {
                    value = row["product_name"];
                    if (!value || value === "0" || value === 0) {
                        value = row["sku"];
                    }
                }

                if (typeof value === "number") {
                    if (!noAbsKeys.includes(column)) value = Math.abs(value);
                    if (Math.abs(value) < 1e-10) value = 0;
                    if (column !== "product_name" && column !== "quantity") {
                        value = Number(value.toFixed(2));
                    }
                }

                if (column === "asp" && typeof value === "number") value = Number(value.toFixed(2));
                if (column === "unit_wise_profitability" && typeof value === "number")
                    value = Number(value.toFixed(2));
                if (column === "profit_percentage" && typeof value === "number")
                    value = Number(value) / 100;

                rowData[column] = isNaN(value) ? value : Number(value);
            });

            return rowData;
        });

        const summaryRows: Record<string, string | number>[] = [
            { [columnsToDisplay2[0]]: "Cost of Advertisement (-)", [columnsToDisplay2[10]]: Math.abs(Number(totals.advertising_total)) },
            ...((countryName === "us" || countryName === "global")
                ? [{
                    [columnsToDisplay2[0]]: "Shipment Charges (-)",
                    [columnsToDisplay2[10]]: Math.abs(Number(totals.shipment_charges)),
                }]
                : []),
            { [columnsToDisplay2[0]]: "Platform Fees (-)", [columnsToDisplay2[10]]: Math.abs(Number(totals.platform_fee)) },
            { [columnsToDisplay2[0]]: "CM2 Profit/Loss", [columnsToDisplay2[10]]: Math.abs(Number(totals.cm2_profit)) },
            { [columnsToDisplay2[0]]: "CM2 Margins", [columnsToDisplay2[10]]: Number(totals.cm2_margins) / 100 },
            { [columnsToDisplay2[0]]: "TACoS (Total Advertising Cost of Sale)", [columnsToDisplay2[10]]: Number(totals.acos) / 100 },
            { [columnsToDisplay2[0]]: "Net Reimbursement during the month", [columnsToDisplay2[10]]: Math.abs(Number(totals.rembursement_fee)) },
            { [columnsToDisplay2[0]]: "Reimbursement vs CM2 Margins", [columnsToDisplay2[10]]: Number(totals.rembursment_vs_cm2_margins) / 100 },
            { [columnsToDisplay2[0]]: "Reimbursement vs Sales", [columnsToDisplay2[10]]: Number(totals.reimbursement_vs_sales) / 100 },
        ];

        const headerRow = {
            [columnsToDisplay2[0]]: "Product Name",
            [columnsToDisplay2[1]]: "Quantity Sold",
            [columnsToDisplay2[2]]: "ASP",
            [columnsToDisplay2[3]]: "Gross Sales",             // 👈 changed
            [columnsToDisplay2[4]]: "Net Sales",               // 👈 changed
            [columnsToDisplay2[5]]: "Cost of Goods Sold",
            [columnsToDisplay2[6]]: "Amazon Fees",
            [columnsToDisplay2[7]]: "Selling Fees",
            [columnsToDisplay2[8]]: "FBA fees",
            [columnsToDisplay2[9]]: "Net Credits",
            [columnsToDisplay2[10]]: "Net Taxes",
            [columnsToDisplay2[11]]: "CM1 Profit",
            [columnsToDisplay2[12]]: "CM1 Profit (%)",
            [columnsToDisplay2[13]]: "CM1 Profit per Unit",
        };


        const signageRow = {
            [columnsToDisplay2[2]]: "",          // ASP
            [columnsToDisplay2[3]]: "",       // Gross Sales
            [columnsToDisplay2[4]]: "(+)",       // Net Sales
            [columnsToDisplay2[5]]: "(-)",       // COGS
            [columnsToDisplay2[6]]: "(-)",       // Amazon Fees
            [columnsToDisplay2[7]]: "(-)",       // Selling Fees
            [columnsToDisplay2[8]]: "(-)",       // FBA Fees
            [columnsToDisplay2[9]]: "(+)",       // Net Credits
            [columnsToDisplay2[10]]: "",         // Net Taxes
            [columnsToDisplay2[11]]: "",         // CM1 Profit
            [columnsToDisplay2[12]]: "",         // CM1 Profit (%)
            [columnsToDisplay2[13]]: "",         // CM1 Profit per Unit
        };


        const fullData = [
            ...getExtraRows().map((row) => ({ [columnsToDisplay2[0]]: row[0] })),
            {},
            headerRow,
            signageRow,
            ...tableDataForExcel,
            ...summaryRows,
        ];

        const finalWs = XLSX.utils.json_to_sheet(fullData, { skipHeader: true });

        if (finalWs["!ref"]) {
            const rng = XLSX.utils.decode_range(finalWs["!ref"]);
            for (let r = 6; r <= rng.e.r; r++) {
                for (let c = 1; c < columnsToDisplay2.length; c++) {
                    const cellAddress = XLSX.utils.encode_cell({ r, c });
                    const colKey = columnsToDisplay2[c] as string;

                    if (finalWs[cellAddress] && typeof finalWs[cellAddress].v === "number") {
                        finalWs[cellAddress].t = "n";

                        const rowHeaderVal =
                            finalWs[XLSX.utils.encode_cell({ r, c: 0 })]?.v as string | undefined;
                        const isPct =
                            colKey === "profit_percentage" ||
                            (rowHeaderVal ? percentageSummaryLabels.includes(rowHeaderVal) : false);

                        if (isPct) {
                            finalWs[cellAddress].z = "#,##0.00%";
                        } else if (colKey === "quantity") {
                            finalWs[cellAddress].z = "#,##0";
                        } else {
                            finalWs[cellAddress].z = "#,##0.00";
                        }
                    }
                }
            }
        }

        XLSX.utils.book_append_sheet(wb, finalWs, "SKU Profitability");

        const filename =
            range === "monthly"
                ? `SKU-wise Profitability-${convertToAbbreviatedMonth(month)}'${yearShort}.xlsx`
                : range === "quarterly"
                    ? `SKU-wise Profitability-${quarter}'${yearShort}.xlsx`
                    : `SKU-wise Profitability-Year'${yearShort}.xlsx`;

        XLSX.writeFile(wb, filename);
    };

    /* --------- Render --------- */

    if (loading) return <div>Loading...</div>;
    if (error) return <div className="text-red-600">Error: {error}</div>;

    return (
        <>
            {/* <div className="mt-6">
        <h2 className="inline-flex items-center rounded-md bg-white text-[18px] font-bold text-[#414042]">
          <span dangerouslySetInnerHTML={{ __html: getTitle() }} />
          <span className="text-[#5EA68E]">&nbsp;({currencySymbol})</span>
        </h2>
      </div> */}

            {/* <div className="flex gap-2 mt-6">
                <PageBreadcrumb
                    pageTitle={getTitle()}
                    variant="page"
                    align="left"
                    textSize="2xl"
                />
                <span className="text-[#5EA68E] text-2xl font-semibold">
                    ({currencySymbol})
                </span>
            </div> */}

            <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-1 sm:p-2">
                <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                    <div className="flex flex-wrap items-baseline gap-2 justify-center sm:justify-start">
                        <PageBreadcrumb
                            pageTitle={getTitle()}
                            variant="page"
                            align="left"
                            textSize="2xl"
                        />
                        <span className="text-[#5EA68E] text-lg sm:text-2xl md:text-2xl font-bold">
                            ({currencySymbol})
                        </span>
                    </div>

                    <div className="flex justify-center sm:justify-end">
                        <DownloadIconButton onClick={handleDownloadExcel} />
                    </div>
                </div>




                <div className={`transition-opacity ${noDataFound ? "opacity-30" : "opacity-100"}`}>
                    {showModal2 && (
                        <CustomModal onClose={() => setShowModal2(false)}>
                            <SkuMultiCountryUpload
                                onClose={() => setShowModal2(false)}
                                onComplete={() => setShowModal2(false)}
                            />
                        </CustomModal>
                    )}

                    {/* Main table */}

                    <div className="w-full overflow-x-auto rounded-xl border border-gray-300">
                        <div className="min-w-full">
                            <table className="min-w-[800px] w-full table-auto border-collapse text-[#414042]">
                                <thead className="sticky top-0 z-10 font-bold text-[#f8edcf]">
                                    <tr className="bg-[#5EA68E]">
                                        <th className="w-[60px] whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            Sno.
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]">
                                            Product Name
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            Quantity Sold
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            ASP
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            Gross Sales
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            Net Sales
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            COGS
                                        </th>
                                        <th
                                            onClick={handleAmazonFeeClick}
                                            className="relative cursor-pointer select-none whitespace-nowrap border border-gray-300 bg-[#4a8773] px-6 py-2 text-center text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            {/* Left Icon */}
                                            <span
                                                className={`absolute left-2 top-1/2 caret-pulse`}
                                            >
                                                {showamazonfee ? <FaCaretRight /> : <FaCaretLeft />}
                                            </span>

                                            {/* Center Text */}
                                            <span>Amazon Fees </span>

                                            {/* Right Icon */}
                                            <span
                                                className={`absolute right-2 top-1/2 caret-pulse`}
                                            >
                                                {showamazonfee ? <FaCaretLeft /> : <FaCaretRight />}
                                            </span>
                                        </th>

                                        {showamazonfee && (
                                            <>
                                                <th className="whitespace-nowrap border border-gray-300 bg-[#4a8773] px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    Selling Fees
                                                </th>
                                                <th className="whitespace-nowrap border border-gray-300 bg-[#4a8773] px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    FBA fees
                                                </th>
                                            </>
                                        )}
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <div className="flex items-center justify-center gap-1">
                                                Net Credits
                                                <Tooltip text="Net Credits Formula: Postage Credits + Gift Wrap Credits" />
                                            </div>
                                        </th>

                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <div className="flex items-center justify-center gap-1">
                                                Net Taxes
                                                <Tooltip text="Net Taxes Formula: Net Taxes = Shipping Credits Tax + Giftwrap Credits Tax + Promotional Rebates tax + Marketplace Withheld Tax" />
                                            </div>
                                        </th>

                                        <th
                                            onClick={handleprofitClick}
                                            className="relative cursor-pointer select-none whitespace-nowrap border border-gray-300 bg-[#4a8773] px-6 py-2 text-center text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            {/* Left Icon */}
                                            <span className="absolute left-2 top-1/2 caret-pulse">
                                                {showprofit ? <FaCaretRight /> : <FaCaretLeft />}
                                            </span>

                                            {/* Center Text */}
                                            <span>CM1 Profit </span>

                                            {/* Right Icon */}
                                            <span className="absolute right-2 top-1/2 caret-pulse">
                                                {showprofit ? <FaCaretLeft /> : <FaCaretRight />}
                                            </span>
                                        </th>


                                        {showprofit && (
                                            <>
                                                <th className="whitespace-nowrap border border-gray-300 bg-[#4a8773] px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    CM1 Profit (%)
                                                </th>
                                                <th className="whitespace-nowrap border border-gray-300 bg-[#4a8773] px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    CM1 Profit per Unit
                                                </th>
                                            </>
                                        )}
                                    </tr>

                                    <tr className="font-bold text-center">
                                        <td className="border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                        <td className="border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                        <td className="border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                        <td className="border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                        <td className="border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)] text-green-700">
                                            (+)
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)] text-[#ff5c5c]">
                                            (-)
                                        </td>
                                        <td
                                            onClick={handleAmazonFeeClick}
                                            className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)] text-[#ff5c5c]"
                                        >
                                            (-)
                                        </td>
                                        {showamazonfee && (
                                            <>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]">
                                                    (-)
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)] ">
                                                    (-)
                                                </td>
                                            </>
                                        )}
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)] text-green-700">
                                            (+)
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                        {showprofit && (
                                            <>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)]" />
                                            </>
                                        )}
                                    </tr>
                                </thead>

                                <tbody>
                                    {tableData.length > 0 ? (
                                        tableData.map((row, index) => {
                                            const isLastRow = index === tableData.length - 1;
                                            const isCostZero = (row["cost_of_unit_sold"] || 0) === 0;

                                            return (
                                                <tr
                                                    key={index}
                                                    className={`${index % 2 === 0 ? "bg-white" : "bg-gray-50"} ${isLastRow ? "bg-gray-200 font-semibold" : ""
                                                        } ${isCostZero ? "" : ""}`}
                                                >
                                                    <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                        {isLastRow ? "" : index + 1}
                                                    </td>

                                                    {columnsToDisplay.map((column, idx) => {
                                                        const col = column as keyof TableRow;
                                                        const isProductName = col === "product_name";
                                                        const raw = row[col];

                                                        // base formatted value
                                                        let cellContent: string | number = formatValue(raw as any, col as string);

                                                        // --- special handling for product name ---
                                                        let isNameMissing = false;
                                                        if (isProductName) {
                                                            const name = row.product_name;
                                                            isNameMissing =
                                                                name === undefined ||
                                                                name === null ||
                                                                name === "" ||
                                                                name === "0" ||
                                                                (name as any) === 0;

                                                            // if name is missing / 0, show SKU instead (for monthly, quarterly, yearly)
                                                            if (isNameMissing && row.sku) {
                                                                cellContent = String(row.sku);
                                                            }
                                                        }

                                                        return (
                                                            <td
                                                                key={idx}
                                                                className={`whitespace-nowrap border border-gray-300 px-2 py-2 text-[clamp(12px,0.729vw,16px)] ${isProductName ? "text-left" : "text-center"
                                                                    }`}
                                                            >
                                                                {isProductName && !isLastRow ? (
                                                                    <span
                                                                        onClick={() => handleProductClick(String(cellContent || ""))}
                                                                        className="inline-block max-w-[220px] cursor-pointer truncate align-middle text-[#60a68e] no-underline"
                                                                    >
                                                                        {String(cellContent || "")}

                                                                        {(isCostZero || isNameMissing) && (
                                                                            <span className="ml-1">
                                                                                {/* only show SKU text next to name if it's DIFFERENT
                  from what we're already displaying, so no duplicates */}
                                                                                {row.sku &&
                                                                                    row.sku !== cellContent && (
                                                                                        <strong title="Product name is not available & COGS is zero because You need to Upload SKU data file.">
                                                                                            {row.sku}
                                                                                        </strong>
                                                                                    )}
                                                                                <i
                                                                                    className="fa-solid fa-circle-info ml-1 cursor-pointer"
                                                                                    title="Product name is not available & COGS is zero because You need to Upload SKU data file."
                                                                                    onClick={(e) => {
                                                                                        e.stopPropagation();
                                                                                        setShowModal2(true);
                                                                                    }}
                                                                                />
                                                                            </span>
                                                                        )}
                                                                    </span>
                                                                ) : (
                                                                    <span className="inline-block max-w-[220px] truncate">
                                                                        {cellContent as React.ReactNode}
                                                                    </span>
                                                                )}
                                                            </td>
                                                        );
                                                    })}

                                                </tr>
                                            );
                                        })
                                    ) : (
                                        <tr>
                                            <td
                                                className="border border-gray-300 px-2 py-3 text-center text-[clamp(12px,0.729vw,16px)]"
                                                colSpan={columnsToDisplay.length + 1}
                                            >
                                                No data available
                                            </td>
                                        </tr>
                                    )}

                                    {/* Summary rows */}

                                    {/* Summary rows */}
                                    <tr>
                                        <td
                                            colSpan={summaryLabelColSpan}
                                            className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            Cost of Advertisement &nbsp;
                                            <strong className="text-[#ff5c5c]">(-)</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            {formatValue(totals.advertising_total, "advertising_total")}
                                        </td>
                                    </tr>

                                    {(countryName === "us" || countryName === "global") && (
                                        <tr>
                                            <td
                                                colSpan={summaryLabelColSpan}
                                                className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                            >
                                                Shipment Charges &nbsp;
                                                <strong className="text-[#ff5c5c]">(-)</strong>
                                            </td>
                                            <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                {formatValue(totals.shipment_charges, "shipment_charges")}
                                            </td>
                                        </tr>
                                    )}

                                    <tr>
                                        <td
                                            colSpan={summaryLabelColSpan}
                                            className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            Platform Fees &nbsp;
                                            <strong className="text-[#ff5c5c]">(-)</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            {formatValue(totals.platform_fee, "platform_fee")}
                                        </td>
                                    </tr>

                                    <tr>
                                        <td
                                            colSpan={summaryLabelColSpan}
                                            className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            CM2 Profit/Loss
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            {formatValue(totals.cm2_profit, "cm2_profit")}
                                        </td>
                                    </tr>

                                    <tr>
                                        <td
                                            colSpan={summaryLabelColSpan}
                                            className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            CM2 Margins
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            {formatValue(totals.cm2_margins, "cm2_margins")}%
                                        </td>
                                    </tr>

                                    <tr>
                                        <td
                                            colSpan={summaryLabelColSpan}
                                            className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            TACoS (Total Advertising Cost of Sale)
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            {formatValue(totals.acos, "acos")}%
                                        </td>
                                    </tr>

                                    <tr>
                                        <td
                                            colSpan={summaryLabelColSpan}
                                            className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            Net Reimbursement
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            {formatValue(totals.rembursement_fee, "rembursement_fee")}
                                        </td>
                                    </tr>

                                    <tr>
                                        <td
                                            colSpan={summaryLabelColSpan}
                                            className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            Reimbursement vs CM2 Margins
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            {formatValue(
                                                totals.rembursment_vs_cm2_margins,
                                                "rembursment_vs_cm2_margins"
                                            )}
                                            %
                                        </td>
                                    </tr>

                                    <tr>
                                        <td
                                            colSpan={summaryLabelColSpan}
                                            className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]"
                                        >
                                            Reimbursement vs Sales
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            {formatValue(
                                                totals.reimbursement_vs_sales,
                                                "reimbursement_vs_sales"
                                            )}
                                            %
                                        </td>
                                    </tr>

                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>




            {/* Top & Bottom tables */}
            <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-1 sm:p-2 
            
            
            
            
            
            ">
                <div className=" flex flex-col justify-between gap-7 md:gap-3 text-[#414042] md:flex-row">
                    <div className="flex-1">
                        <div className="flex gap-2 text-lg sm:text-2xl md:text-2xl mb-2 md:mb-4 font-bold">
                            <PageBreadcrumb pageTitle="Most 5 Profitable Products" variant="page" align="left" textSize="2xl" />
                            {/* <span className="text-green-500 ">&nbsp;({currencySymbol})</span> */}
                        </div>

                        <div className="overflow-x-auto rounded-xl border border-gray-300">
                            <table className="w-full table-auto border-collapse">
                                <thead>
                                    <tr className="bg-green-500 font-bold text-[#f8edcf]">
                                        <th className="w-[160px] whitespace-nowrap border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]">
                                            Product Name
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            CM1 Profit {' '}({currencySymbol})
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            Profit Mix (%)
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            Sales Mix (%)
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            CM1 Profit per Unit {' '}({currencySymbol})
                                        </th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {topData.rows.map((item, index) => {
                                        const isCostZero = (item as any).cost_of_unit_sold === 0;
                                        const isProductMissing = !item.product_name;

                                        return (
                                            <tr
                                                key={index}
                                                className={`${index % 2 === 0 ? "bg-white" : "bg-gray-50"} ${isCostZero ? "text-[#ff5c5c]" : ""
                                                    }`}
                                            >
                                                <td className="w-[160px] whitespace-nowrap border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]">
                                                    <span
                                                        className="flex max-w-[220px] items-center truncate"
                                                    // title={item.product_name || (item as any).sku}
                                                    >
                                                        {item.product_name || (
                                                            <span className="text-[#ff5c5c]">Missing Product</span>
                                                        )}
                                                        {(isCostZero || isProductMissing) && (
                                                            <span className="ml-1 flex items-center text-[#ff5c5c]">
                                                                {(item as any).sku && (
                                                                    <strong title="Product name is not available & COGS is zero because You need to Upload SKU data file.">
                                                                        {(item as any).sku}
                                                                    </strong>
                                                                )}
                                                                <i
                                                                    className="fa-solid fa-circle-info ml-1 cursor-pointer"
                                                                    title="Product name is not available & COGS is zero because You need to Upload SKU data file."
                                                                    onClick={() => setShowModal2(true)}
                                                                />
                                                            </span>
                                                        )}
                                                    </span>
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    {item.profit}
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    {item.profitMix}%
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    {item.salesMix}%
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    {item.unit_wise_profitability}
                                                </td>
                                            </tr>
                                        );
                                    })}

                                    <tr className="bg-gray-200 font-semibold">
                                        <td className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]">
                                            <strong>Total</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <strong>{topData.totals.profit}</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <strong>{topData.totals.profitMix}%</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <strong>{topData.totals.salesMix}%</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <strong>{topData.totals.unit_wise_profitability}</strong>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>

                    <div className="flex-1">
                        <div className="flex gap-2 text-lg sm:text-2xl md:text-2xl mb-2 md:mb-4 font-bold">
                            <PageBreadcrumb pageTitle="Least 5 Profitable Products" variant="page" align="left" textSize="2xl" />
                            {/* <span className="text-[#5EA68E]">&nbsp;({currencySymbol})</span> */}
                        </div>
                        <div className="overflow-x-auto rounded-xl border border-gray-300">
                            <table className="w-full table-auto border-collapse">
                                <thead>
                                    <tr className="bg-[#ff5c5c] font-bold text-white">
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]">
                                            Product Name
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            CM1 Profit {' '}({currencySymbol})
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            Profit Mix (%)
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            Sales Mix (%)
                                        </th>
                                        <th className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            CM1 Profit per Unit {' '}({currencySymbol})
                                        </th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {bottomData.rows.map((item, index) => {
                                        const isCostZero = (item as any).cost_of_unit_sold === 0;
                                        const isProductMissing = !item.product_name;

                                        return (
                                            <tr
                                                key={index}
                                                className={`${index % 2 === 0 ? "bg-white" : "bg-gray-50"} ${isCostZero ? "text-[#ff5c5c]" : ""
                                                    }`}
                                            >
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]">
                                                    <span
                                                        className="inline-flex max-w-[220px] items-center truncate"
                                                    // title={item.product_name || (item as any).sku}
                                                    >
                                                        {item.product_name}
                                                        {(isCostZero || isProductMissing) && (
                                                            <span className="ml-1 inline-flex items-center text-[#ff5c5c]">
                                                                {(item as any).sku && (
                                                                    <strong title="Product name is not available & COGS is zero because You need to Upload SKU data file.">
                                                                        {(item as any).sku}
                                                                    </strong>
                                                                )}
                                                                <i
                                                                    className="fa-solid fa-circle-info ml-1 cursor-pointer"
                                                                    title="Product name is not available & COGS is zero because You need to Upload SKU data file."
                                                                    onClick={() => setShowModal2(true)}
                                                                />
                                                            </span>
                                                        )}
                                                    </span>
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    {item.profit}
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    {item.profitMix}%
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    {item.salesMix}%
                                                </td>
                                                <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                                    {item.unit_wise_profitability}
                                                </td>
                                            </tr>
                                        );
                                    })}

                                    <tr className="bg-gray-200 font-semibold">
                                        <td className="border border-gray-300 px-2 py-2 text-left text-[clamp(12px,0.729vw,16px)]">
                                            <strong>Total</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <strong>{bottomData.totals.profit}</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <strong>{bottomData.totals.profitMix}%</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <strong>{bottomData.totals.salesMix}%</strong>
                                        </td>
                                        <td className="whitespace-nowrap border border-gray-300 px-2 py-2 text-center text-[clamp(12px,0.729vw,16px)]">
                                            <strong>{bottomData.totals.unit_wise_profitability}</strong>
                                        </td>
                                    </tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>

            {showModal && selectedProduct && (
                <Productinfoinpopup
                    productname={selectedProduct}
                    countryName={countryName}
                    month={month}
                    year={year}
                    onClose={() => setShowModal(false)}
                />
            )}
        </>
    );
};

export default SKUtable;