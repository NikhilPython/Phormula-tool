import * as XLSX from "xlsx-js-style";
import { saveAs } from "file-saver";

type Row = Record<string, any>;
const money = (n: number) => Math.round((n + Number.EPSILON) * 100) / 100;
const optionalNumber = (v: any): number | null =>
  v == null || String(v).trim() === "" || !Number.isFinite(Number(v)) ? null : Number(v);

/** API charged amounts are positive expenses. Never substitute them for estimates. */
export function buildFbaReport(rows: Row[]) {
  const detail = rows.filter(r => r.order_id && r.sku && r.record_type !== "summary"
    && r.fba_estimate_status !== "not_fulfillment_sale").map(r => {
    const refund = r.transaction_type === "Refund" || r.fba_estimate_status === "refund_credit_not_comparable";
    const expected = optionalNumber(r.fbaanswer);
    const charged = optionalNumber(r.fba_fees);
    const difference = expected === null || charged === null ? null : money(charged - expected);
    return {
      order: String(r.order_id), sku: String(r.sku), product: String(r.product_name || r.sku),
      units: Number(r.fba_quantity ?? r.quantity ?? r.total_quantity ?? 0), expected, charged, difference,
      refund,
      status: refund ? "Refund / credit" : difference === null ? "Needs data" : difference === 0 ? "Accurately Charged" : difference > 0 ? "Overcharged" : "Undercharged",
      basis: ({
        amazon_measurements_2026_09_01_assumed_for_september: "Amazon explanation — September assumption",
        amazon_fee_preview_measurements_historical_estimate: "Amazon Fee Preview estimate",
        amazon_package_attributes_historical_estimate: "Amazon package attribute estimate",
        estimated_standard_fulfillment: "Catalog package estimate",
        refund_credit_not_comparable: "Transaction refund / credit",
      } as Record<string, string>)[r.fba_estimate_status] || String(r.fba_estimate_status || "Package estimate"),
    };
  });
  const summarize = (items: typeof detail) => ({
    lines: items.length, units: items.reduce((s, r) => s + r.units, 0),
    missing: items.filter(r => !r.refund && (r.expected === null || r.charged === null)).length,
    expected: items.some(r => !r.refund && r.expected === null) ? null : money(items.reduce((s, r) => s + (r.expected ?? 0), 0)),
    charged: items.some(r => r.charged === null) ? null : money(items.reduce((s, r) => s + r.charged!, 0)),
    comparableVariance: money(items.reduce((s, r) => s + (r.difference ?? 0), 0)),
  });
  const groups = new Map<string, typeof detail>();
  detail.forEach(r => groups.set(r.sku, [...(groups.get(r.sku) || []), r]));
  return {
    detail, total: summarize(detail),
    summary: ["Accurately Charged", "Overcharged", "Undercharged", "Needs data", "Refund / credit"].map(status => ({status, ...summarize(detail.filter(r => r.status === status))})),
    products: [...groups].map(([sku, items]) => ({sku, product: items[0].product, ...summarize(items)})),
  };
}

export function exportFbaFeesExcel(options: {rows: Row[]; period: string; country: string; currency: string; company?: string}) {
  const report = buildFbaReport(options.rows);
  const wb = XLSX.utils.book_new();
  const cell = (v: number | null) => v === null ? "Unavailable" : v;
  const totals = (r: typeof report.total) => [r.lines, r.units, cell(r.expected), cell(r.charged), r.comparableVariance, r.missing];
  const add = (name: string, headers: string[], data: any[][]) => {
    const ws = XLSX.utils.aoa_to_sheet([
      [`Amazon ${options.country.toUpperCase()} | FBA Fees | ${options.period}`],
      [options.company || "", "Phormula", options.currency],
      ["Expected uses shipped units. Net units = shipped units − returns. Charged FBA includes refund credits."],
      ["Estimates exclude storage, inventory surcharges and program discounts. Verify variances before claiming."],
      ["Refunds are shown separately and are not fee variances. Refresh and download after updating product data."],
      headers, ...data,
    ]);
    ws["!cols"] = headers.map((h, i) => ({wch: i === 0 ? 25 : h.includes("Product") || h.toLowerCase().includes("basis") ? 48 : 21}));
    ws["!merges"] = [0, 2, 3, 4].map(r => ({s:{r,c:0},e:{r,c:headers.length-1}}));
    ws["!rows"] = [{hpt:28},{hpt:22},{hpt:30},{hpt:30},{hpt:30},{hpt:34}];
    ws["!autofilter"] = {ref:XLSX.utils.encode_range({s:{r:5,c:0},e:{r:5+data.length,c:headers.length-1}})};
    Object.keys(ws).filter(k => !k.startsWith("!")).forEach(k => {
      const p = XLSX.utils.decode_cell(k), c = ws[k];
      c.s = {font:{name:"Calibri",sz:11,color:{rgb:p.r === 0 || p.r === 5 ? "FFFFFF" : "243746"}},
        fill:{fgColor:{rgb:p.r === 0 || p.r === 5 ? "17365D" : p.r % 2 === 0 ? "F0F5FA" : "FFFFFF"}},
        alignment:{vertical:"center",wrapText:p.r < 6 || headers[p.c]?.toLowerCase().includes("basis")}};
      if (c.t === "n") c.z = "#,##0.00;[Red](#,##0.00);–";
    });
    XLSX.utils.book_append_sheet(wb, ws, name);
  };
  add("Summary", ["FBA classification", "Order/SKU lines", "Net units", "Expected shipment FBA", "Net charged FBA", "Comparable variance", "Needs data lines"],
    [...report.summary.map(r => [r.status, ...totals(r)]), ["Total", ...totals(report.total)]]);
  add("Product-wise Breakdown", ["SKU", "Product", "Order/SKU lines", "Net units", "Expected shipment FBA", "Net charged FBA", "Comparable variance", "Needs data lines"],
    report.products.map(r => [r.sku, r.product, ...totals(r)]));
  const headers = ["Order ID", "SKU", "Product", "Units", "Expected FBA", "Charged FBA", "Difference", "FBA classification", "Estimate basis"];
  const detailRows = (accurate: boolean) => report.detail.filter(r => (r.status === "Accurately Charged") === accurate)
    .map(r => [r.order,r.sku,r.product,r.units,r.refund ? "Not applicable" : cell(r.expected),cell(r.charged),r.refund ? "Not applicable" : cell(r.difference),r.status,r.basis]);
  add("Fee Variances", headers, detailRows(false));
  add("Accurately Charged", headers, detailRows(true));
  saveAs(new Blob([XLSX.write(wb, {bookType:"xlsx",type:"array"})], {type:"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"}),
    `FBA Fees ${options.country.toUpperCase()} ${options.period}.xlsx`);
}
