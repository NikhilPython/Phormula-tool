// src/app/(tabs)/dashboard.tsx
import React, { useCallback, useMemo, useState } from "react";
import {
    View,
    Text,
    StyleSheet,
    TouchableOpacity,
    ScrollView,
    RefreshControl,
    ActivityIndicator,
    Dimensions,
} from "react-native";
import Svg, {
    Path,
    Line,
    Circle,
    Text as SvgText,
    G,
} from "react-native-svg";
import { useAuth } from "../../services/auth/AuthContext";
import { useRouter } from "expo-router";
import { COLORS } from "../../constants/colors"; // <-- IMPORT COLORS

/* =========================================================
   TYPES
   ========================================================= */
type MtdView = "global" | "uk" | "us";
type CurrencyCode = "GBP" | "USD" | "CAD" | "INR";

type DailyPoint = { date: string; net_sales?: number };
type BiSeries = { current_mtd: DailyPoint[]; previous: DailyPoint[] };

type StatCardData = {
    label: string;
    current: number;
    previous: number;
    deltaPct?: number | null;
    inverseDelta?: boolean;
    borderColor: string;
    currentPerUnit?: number;
    previousPerUnit?: number;
    isPercent?: boolean;
    secondaryPct?: number;
    secondaryPrevPct?: number;
    secondaryIsAbsolute?: boolean;
};

type TargetData = {
    today: number;
    mtd: number;
    lastMonth: number;
    lastMonthToDate: number;
    target: number;
    salesTrendPct?: number | null;
    targetTrendPct?: number | null;
    reimbursementCurrent: number;
    reimbursementPrevious: number;
};

/* =========================================================
   MOCK DATA (Updated to use COLORS)
   ========================================================= */
const MOCK_SERIES: BiSeries = {
    current_mtd: [
        { date: "2025-11-01", net_sales: 4200 },
        { date: "2025-11-02", net_sales: 5100 },
        { date: "2025-11-03", net_sales: 4700 },
        { date: "2025-11-04", net_sales: 6300 },
        { date: "2025-11-05", net_sales: 5900 },
        { date: "2025-11-06", net_sales: 7200 },
        { date: "2025-11-07", net_sales: 6800 },
        { date: "2025-11-08", net_sales: 8100 },
        { date: "2025-11-09", net_sales: 7600 },
        { date: "2025-11-10", net_sales: 8900 },
        { date: "2025-11-11", net_sales: 9500 },
        { date: "2025-11-12", net_sales: 8700 },
    ],
    previous: [
        { date: "2025-10-01", net_sales: 3800 },
        { date: "2025-10-02", net_sales: 4400 },
        { date: "2025-10-03", net_sales: 4200 },
        { date: "2025-10-04", net_sales: 5100 },
        { date: "2025-10-05", net_sales: 4800 },
        { date: "2025-10-06", net_sales: 6100 },
        { date: "2025-10-07", net_sales: 5500 },
        { date: "2025-10-08", net_sales: 6400 },
        { date: "2025-10-09", net_sales: 6100 },
        { date: "2025-10-10", net_sales: 7300 },
        { date: "2025-10-11", net_sales: 7900 },
        { date: "2025-10-12", net_sales: 7100 },
    ],
};

const MOCK_CARDS_BY_VIEW: Record<MtdView, StatCardData[]> = {
    global: [
        { label: "Units", current: 4820, previous: 4380, deltaPct: 10.05, borderColor: COLORS.yellow },
        { label: "ASP", current: 18.45, previous: 17.9, deltaPct: 3.07, borderColor: COLORS.red },
        { label: "Gross Sales", current: 92300, previous: 84100, deltaPct: 9.75, borderColor: COLORS.orange,
          currentPerUnit: 19.15, previousPerUnit: 19.2 },
        { label: "Net Sales", current: 88940, previous: 78410, deltaPct: 13.43, borderColor: COLORS.blue,
          currentPerUnit: 18.45, previousPerUnit: 17.9 },
        { label: "Cost of Ads", current: 12450, previous: 13890, deltaPct: -10.37, inverseDelta: true,
          borderColor: COLORS.brown, currentPerUnit: 2.58, previousPerUnit: 3.17 },
        { label: "TACoS", current: 14.0, previous: 17.71, deltaPct: -20.95, inverseDelta: true, isPercent: true,
          borderColor: COLORS.teal },
        { label: "CM2 Profit", current: 21850, previous: 17600, deltaPct: 24.15, borderColor: COLORS.lightGreen,
          secondaryPct: 24.57, secondaryPrevPct: 22.45 },
        { label: "Promotions", current: 3980, previous: 4620, deltaPct: -13.85, inverseDelta: true,
          borderColor: COLORS.darkGreen, secondaryPct: 4.47, secondaryPrevPct: 5.89, secondaryIsAbsolute: true },
    ],
    uk: [
        { label: "Units", current: 2140, previous: 1980, deltaPct: 8.08, borderColor: COLORS.yellow },
        { label: "ASP", current: 16.2, previous: 15.8, deltaPct: 2.53, borderColor: COLORS.red },
        { label: "Gross Sales", current: 41200, previous: 37800, deltaPct: 8.99, borderColor: COLORS.orange,
          currentPerUnit: 19.25, previousPerUnit: 19.09 },
        { label: "Net Sales", current: 38950, previous: 35200, deltaPct: 10.65, borderColor: COLORS.blue,
          currentPerUnit: 18.2, previousPerUnit: 17.78 },
        { label: "Cost of Ads", current: 5820, previous: 6410, deltaPct: -9.2, inverseDelta: true,
          borderColor: COLORS.brown, currentPerUnit: 2.72, previousPerUnit: 3.24 },
        { label: "TACoS", current: 14.94, previous: 18.21, deltaPct: -17.96, inverseDelta: true, isPercent: true,
          borderColor: COLORS.teal },
        { label: "CM2 Profit", current: 9740, previous: 8120, deltaPct: 19.95, borderColor: COLORS.lightGreen,
          secondaryPct: 25.01, secondaryPrevPct: 23.07 },
        { label: "Promotions", current: 1640, previous: 2080, deltaPct: -21.15, inverseDelta: true,
          borderColor: COLORS.darkGreen, secondaryPct: 4.21, secondaryPrevPct: 5.91, secondaryIsAbsolute: true },
    ],
    us: [
        { label: "Units", current: 2680, previous: 2400, deltaPct: 11.67, borderColor: COLORS.yellow },
        { label: "ASP", current: 20.7, previous: 20.0, deltaPct: 3.5, borderColor: COLORS.red },
        { label: "Gross Sales", current: 51100, previous: 46300, deltaPct: 10.37, borderColor: COLORS.orange,
          currentPerUnit: 19.07, previousPerUnit: 19.29 },
        { label: "Net Sales", current: 49990, previous: 43210, deltaPct: 15.69, borderColor: COLORS.blue,
          currentPerUnit: 18.65, previousPerUnit: 18.0 },
        { label: "Cost of Ads", current: 6630, previous: 7480, deltaPct: -11.36, inverseDelta: true,
          borderColor: COLORS.brown, currentPerUnit: 2.47, previousPerUnit: 3.12 },
        { label: "TACoS", current: 13.26, previous: 17.31, deltaPct: -23.4, inverseDelta: true, isPercent: true,
          borderColor: COLORS.teal },
        { label: "CM2 Profit", current: 12110, previous: 9480, deltaPct: 27.74, borderColor: COLORS.lightGreen,
          secondaryPct: 24.22, secondaryPrevPct: 21.94 },
        { label: "Promotions", current: 2340, previous: 2540, deltaPct: -7.87, inverseDelta: true,
          borderColor: COLORS.darkGreen, secondaryPct: 4.68, secondaryPrevPct: 5.88, secondaryIsAbsolute: true },
    ],
};

const MOCK_TARGETS_BY_VIEW: Record<MtdView, TargetData> = {
    global: { today: 9500, mtd: 88940, lastMonth: 118400, lastMonthToDate: 78410, target: 130000,
        salesTrendPct: 13.43, targetTrendPct: 68.4, reimbursementCurrent: 1240, reimbursementPrevious: 980 },
    uk: { today: 4100, mtd: 38950, lastMonth: 51200, lastMonthToDate: 35200, target: 58000,
        salesTrendPct: 10.65, targetTrendPct: 67.2, reimbursementCurrent: 540, reimbursementPrevious: 420 },
    us: { today: 5400, mtd: 49990, lastMonth: 67200, lastMonthToDate: 43210, target: 72000,
        salesTrendPct: 15.69, targetTrendPct: 69.4, reimbursementCurrent: 700, reimbursementPrevious: 560 },
};

/* =========================================================
   FORMATTERS
   ========================================================= */
const CURRENCY_SYMBOL: Record<CurrencyCode, string> = {
    GBP: "£", USD: "$", CAD: "CA$", INR: "₹",
};

const formatMoney = (
    value: number | null | undefined,
    currency: CurrencyCode = "GBP",
    decimals = 2
) => {
    const n = Number(value ?? 0);
    const symbol = CURRENCY_SYMBOL[currency];
    return `${symbol}${n.toLocaleString("en-GB", {
        minimumFractionDigits: decimals,
        maximumFractionDigits: decimals,
    })}`;
};

const formatMoneyShort = (value: number, currency: CurrencyCode = "GBP") => {
    const symbol = CURRENCY_SYMBOL[currency];
    if (Math.abs(value) >= 1000) return `${symbol}${(value / 1000).toFixed(1)}k`;
    return `${symbol}${Math.round(value)}`;
};

const formatInt = (v: number | null | undefined) =>
    Math.round(Number(v ?? 0)).toLocaleString("en-GB");

const formatPct = (v: number | null | undefined, decimals = 2) =>
    `${Number(v ?? 0).toFixed(decimals)}%`;

/* =========================================================
   STAT CARD
   ========================================================= */
function MobileStatCard({
    card,
    currency,
    loading,
}: {
    card: StatCardData;
    currency: CurrencyCode;
    loading?: boolean;
}) {
    const delta = card.deltaPct;
    const isPositive = delta != null ? Number(delta) >= 0 : null;
    const isGood = isPositive == null
        ? null
        : card.inverseDelta
            ? !isPositive
            : isPositive;
            
    // Use COLORS for delta
    const deltaColor = isGood == null ? COLORS.mutedText : isGood ? COLORS.green : COLORS.red;
    const arrow = isPositive == null ? "" : isPositive ? "▲" : "▼";

    const formatMain = (v: number) =>
        card.isPercent ? formatPct(v) : formatMoney(v, currency);

    const formatPerUnit = (v: number) =>
        formatMoney(v, currency, 0);

    return (
        <View style={[styles.statCard, { borderTopColor: card.borderColor }]}>
            <Text style={styles.statLabel}>{card.label}</Text>

            {loading ? (
                <ActivityIndicator size="small" color={COLORS.green} style={{ marginTop: 12 }} />
            ) : (
                <>
                    <View style={styles.statCurrentRow}>
                        <Text style={styles.statCurrent} numberOfLines={1}>
                            {formatMain(card.current)}
                        </Text>
                        {card.secondaryPct != null && (
                            <Text style={styles.statSecondary}>
                                ({formatPct(card.secondaryPct)})
                            </Text>
                        )}
                    </View>

                    {card.currentPerUnit != null && (
                        <Text style={styles.statPerUnit}>
                            /unit {formatPerUnit(card.currentPerUnit)}
                        </Text>
                    )}

                    <View style={styles.statFooter}>
                        <Text style={styles.statPrev} numberOfLines={1}>
                            {formatMain(card.previous)}
                        </Text>
                        {delta != null && (
                            <Text style={[styles.statDelta, { color: deltaColor }]}>
                                {arrow} {Math.abs(Number(delta)).toFixed(2)}%
                            </Text>
                        )}
                    </View>
                </>
            )}
        </View>
    );
}

/* =========================================================
   SALES TARGET CARD
   ========================================================= */
function TargetCard({
    data,
    currency,
}: {
    data: TargetData;
    currency: CurrencyCode;
}) {
    const pct = data.target > 0 ? Math.min((data.mtd / data.target) * 100, 100) : 0;
    const salesTrend = data.salesTrendPct ?? 0;
    const salesTrendGood = salesTrend >= 0;

    return (
        <View style={styles.targetCard}>
            <View style={styles.targetHeader}>
                <Text style={styles.targetTitle}>Sales Target</Text>
                <Text style={styles.targetPct}>{pct.toFixed(1)}%</Text>
            </View>

            <View style={styles.targetAmountRow}>
                <Text style={styles.targetAmountCurrent}>
                    {formatMoney(data.mtd, currency, 0)}
                </Text>
                <Text style={styles.targetAmountTotal}>
                    / {formatMoney(data.target, currency, 0)}
                </Text>
            </View>

            <View style={styles.progressTrack}>
                <View style={[styles.progressFill, { width: `${pct}%` }]} />
            </View>

            <View style={styles.targetRows}>
                <TargetRow label="Today" value={formatMoney(data.today, currency, 0)} />
                <TargetRow
                    label="MTD"
                    value={formatMoney(data.mtd, currency, 0)}
                    right={
                        <Text style={[styles.targetTrend, { color: salesTrendGood ? COLORS.green : COLORS.red }]}>
                            {salesTrendGood ? "▲" : "▼"} {Math.abs(salesTrend).toFixed(2)}%
                        </Text>
                    }
                />
                <TargetRow label="Last Month" value={formatMoney(data.lastMonth, currency, 0)} />
                <TargetRow label="Target" value={formatMoney(data.target, currency, 0)} />
                <TargetRow
                    label="Reimbursement"
                    value={formatMoney(data.reimbursementCurrent, currency, 0)}
                    right={<Text style={styles.targetMuted}>prev {formatMoney(data.reimbursementPrevious, currency, 0)}</Text>}
                />
            </View>
        </View>
    );
}

function TargetRow({
    label,
    value,
    right,
}: {
    label: string;
    value: string;
    right?: React.ReactNode;
}) {
    return (
        <View style={styles.targetRow}>
            <Text style={styles.targetRowLabel}>{label}</Text>
            <View style={{ flexDirection: "row", alignItems: "center", gap: 8 }}>
                <Text style={styles.targetRowValue}>{value}</Text>
                {right}
            </View>
        </View>
    );
}

/* =========================================================
   DAILY SALES CHART
   ========================================================= */
function DailySalesChart({
    series,
    currency,
}: {
    series: BiSeries;
    currency: CurrencyCode;
}) {
    const width = Dimensions.get("window").width - 48;
    const height = 200;
    const padding = { top: 20, right: 12, bottom: 28, left: 48 };
    const innerW = width - padding.left - padding.right;
    const innerH = height - padding.top - padding.bottom;

    const all = [...series.current_mtd, ...series.previous];
    const maxVal = Math.max(...all.map((p) => Number(p.net_sales || 0)), 1);

    const xFor = (i: number, total: number) =>
        padding.left + (total <= 1 ? innerW / 2 : (i / (total - 1)) * innerW);
    const yFor = (v: number) =>
        padding.top + innerH - (Math.max(v, 0) / maxVal) * innerH;

    const toPath = (pts: DailyPoint[]) =>
        pts
            .map((p, i) => {
                const x = xFor(i, pts.length);
                const y = yFor(Number(p.net_sales || 0));
                return `${i === 0 ? "M" : "L"} ${x} ${y}`;
            })
            .join(" ");

    const currentPath = toPath(series.current_mtd);
    const prevPath = toPath(series.previous);

    const yTicks = [0, maxVal * 0.5, maxVal];

    return (
        <View style={styles.chartCard}>
            <View style={styles.chartHeader}>
                <Text style={styles.chartTitle}>Daily Net Sales</Text>
                <View style={{ flexDirection: "row", gap: 12 }}>
                    <LegendDot color={COLORS.orange} label="MTD" />
                    <LegendDot color={COLORS.chartPrevLine} label="Prev" dashed />
                </View>
            </View>

            <Svg width={width} height={height}>
                {yTicks.map((v, i) => (
                    <G key={`yt-${i}`}>
                        <Line
                            x1={padding.left}
                            y1={yFor(v)}
                            x2={width - padding.right}
                            y2={yFor(v)}
                            stroke={COLORS.borderLine}
                            strokeWidth={1}
                        />
                        <SvgText
                            x={padding.left - 8}
                            y={yFor(v) + 4}
                            fontSize={10}
                            fill={COLORS.mutedText}
                            textAnchor="end"
                        >
                            {formatMoneyShort(v, currency)}
                        </SvgText>
                    </G>
                ))}

                {prevPath ? (
                    <Path
                        d={prevPath}
                        stroke={COLORS.chartPrevLine}
                        strokeWidth={2}
                        strokeDasharray="6 4"
                        fill="none"
                    />
                ) : null}

                {currentPath ? (
                    <Path
                        d={currentPath}
                        stroke={COLORS.orange}
                        strokeWidth={2.5}
                        fill="none"
                    />
                ) : null}

                {series.current_mtd.map((p, i) => (
                    <Circle
                        key={`dot-${i}`}
                        cx={xFor(i, series.current_mtd.length)}
                        cy={yFor(Number(p.net_sales || 0))}
                        r={3}
                        fill={COLORS.white}
                        stroke={COLORS.orange}
                        strokeWidth={2}
                    />
                ))}

                {[0, Math.floor((series.current_mtd.length - 1) / 2), series.current_mtd.length - 1]
                    .filter((i, idx, arr) => i >= 0 && arr.indexOf(i) === idx)
                    .map((i) => {
                        const p = series.current_mtd[i];
                        const day = p?.date ? p.date.slice(8, 10) : "";
                        return (
                            <SvgText
                                key={`xl-${i}`}
                                x={xFor(i, series.current_mtd.length)}
                                y={height - 8}
                                fontSize={10}
                                fill={COLORS.mutedText}
                                textAnchor="middle"
                            >
                                {day}
                            </SvgText>
                        );
                    })}
            </Svg>
        </View>
    );
}

function LegendDot({
    color,
    label,
    dashed,
}: {
    color: string;
    label: string;
    dashed?: boolean;
}) {
    return (
        <View style={{ flexDirection: "row", alignItems: "center", gap: 4 }}>
            <View
                style={{
                    width: 10,
                    height: dashed ? 2 : 8,
                    backgroundColor: color,
                    borderRadius: 4,
                }}
            />
            <Text style={{ fontSize: 11, color: COLORS.mutedText }}>{label}</Text>
        </View>
    );
}

/* =========================================================
   MAIN SCREEN
   ========================================================= */
export default function DashboardScreen() {
    const { user } = useAuth();
    const router = useRouter();

    const [view, setView] = useState<MtdView>("global");
    const [refreshing, setRefreshing] = useState(false);
    const [loading, setLoading] = useState(false);

    const currency: CurrencyCode = useMemo(() => {
        if (view === "uk") return "GBP";
        if (view === "us") return "USD";
        return "GBP";
    }, [view]);

    const cards = MOCK_CARDS_BY_VIEW[view];
    const target = MOCK_TARGETS_BY_VIEW[view];
    const series = MOCK_SERIES;

    const handleRefresh = useCallback(async () => {
        setRefreshing(true);
        await new Promise((r) => setTimeout(r, 700));
        setRefreshing(false);
    }, []);

    return (
        <View style={styles.container}>
            <ScrollView 
                showsVerticalScrollIndicator={false}
                contentContainerStyle={{ paddingBottom: 140 }}
                refreshControl={
                    <RefreshControl
                        refreshing={refreshing}
                        onRefresh={handleRefresh}
                        tintColor={COLORS.green}
                    />
                }
            >
                {/* ---------- Header ---------- */}
                <View style={styles.headerRow}>
                    <View>
                        <Text style={styles.brand}>PHORMULA</Text>
                        <Text style={styles.heading}>MTD Sales</Text>
                        <Text style={styles.subtitle}>
                            {user?.member_name ? `Hi, ${user.member_name}` : "Welcome"}
                        </Text>
                    </View>
                </View>

                {/* ---------- View selector ---------- */}
                <View style={styles.viewSelector}>
                    {(["global", "uk", "us"] as const).map((v) => {
                        const active = view === v;
                        return (
                            <TouchableOpacity
                                key={v}
                                style={[
                                    styles.viewChip,
                                    active && styles.viewChipActive,
                                ]}
                                onPress={() => setView(v)}
                                activeOpacity={0.85}
                            >
                                <Text
                                    style={[
                                        styles.viewChipText,
                                        active && styles.viewChipTextActive,
                                    ]}
                                >
                                    {v === "global" ? "Global" : v.toUpperCase()}
                                </Text>
                            </TouchableOpacity>
                        );
                    })}
                </View>

                {/* ---------- Stat cards ---------- */}
                <View style={styles.cardsGrid}>
                    {cards.map((card) => (
                        <MobileStatCard
                            key={card.label}
                            card={card}
                            currency={currency}
                            loading={loading}
                        />
                    ))}
                </View>

                {/* ---------- Target card ---------- */}
                <TargetCard data={target} currency={currency} />

                {/* ---------- Daily chart ---------- */}
                <DailySalesChart series={series} currency={currency} />
            </ScrollView>
        </View>
    );
}

/* =========================================================
   STYLES (Updated to use COLORS)
   ========================================================= */
const styles = StyleSheet.create({
    container: { flex: 1, backgroundColor: COLORS.bgApp, paddingHorizontal: 16, paddingTop: 52 },

    /* header */
    headerRow: {
        flexDirection: "row",
        justifyContent: "space-between",
        alignItems: "flex-start",
        marginBottom: 16,
    },
    brand: { color: COLORS.green, fontWeight: "800", letterSpacing: 2, fontSize: 18 },
    heading: { fontSize: 26, fontWeight: "700", color: COLORS.darkText, marginTop: 14 },
    subtitle: { color: COLORS.mutedText, marginTop: 4, fontSize: 13 },

    /* view selector */
    viewSelector: {
        flexDirection: "row",
        backgroundColor: COLORS.bgCard,
        borderRadius: 12,
        padding: 4,
        borderWidth: 1,
        borderColor: COLORS.lightGray,
        marginBottom: 16,
    },
    viewChip: {
        flex: 1,
        paddingVertical: 10,
        alignItems: "center",
        borderRadius: 9,
    },
    viewChipActive: { backgroundColor: COLORS.green },
    viewChipText: { color: COLORS.textMid, fontWeight: "600", fontSize: 13 },
    viewChipTextActive: { color: COLORS.white },

    /* stat cards grid (2 columns) */
    cardsGrid: {
        flexDirection: "row",
        flexWrap: "wrap",
        justifyContent: "space-between",
        gap: 12,
        marginBottom: 16,
    },
    statCard: {
        width: "48%",
        backgroundColor: COLORS.bgCard,
        borderRadius: 12,
        borderTopWidth: 4,
        paddingVertical: 12,
        paddingHorizontal: 12,
        borderWidth: 1,
        borderColor: COLORS.lightGray,
        minHeight: 108,
        justifyContent: "space-between",
    },
    statLabel: { fontSize: 11, fontWeight: "700", color: COLORS.mutedText, letterSpacing: 0.4 },
    statCurrentRow: { flexDirection: "row", alignItems: "baseline", gap: 4, marginTop: 6 },
    statCurrent: { fontSize: 18, fontWeight: "800", color: COLORS.darkText, flexShrink: 1 },
    statSecondary: { fontSize: 10, color: COLORS.mutedText, fontWeight: "600" },
    statPerUnit: { fontSize: 10, color: COLORS.mutedText, marginTop: 2 },
    statFooter: {
        flexDirection: "row",
        justifyContent: "space-between",
        alignItems: "center",
        marginTop: 8,
    },
    statPrev: { fontSize: 11, color: COLORS.textLight, flexShrink: 1 },
    statDelta: { fontSize: 11, fontWeight: "700" },

    /* target card */
    targetCard: {
        backgroundColor: COLORS.bgCard,
        borderRadius: 14,
        borderWidth: 1,
        borderColor: COLORS.lightGray,
        padding: 16,
        marginBottom: 16,
    },
    targetHeader: {
        flexDirection: "row",
        justifyContent: "space-between",
        alignItems: "center",
        marginBottom: 4,
    },
    targetTitle: { fontSize: 14, fontWeight: "700", color: COLORS.darkText },
    targetPct: { fontSize: 14, fontWeight: "800", color: COLORS.green },
    targetAmountRow: { flexDirection: "row", alignItems: "baseline", gap: 4 },
    targetAmountCurrent: { fontSize: 22, fontWeight: "800", color: COLORS.darkText },
    targetAmountTotal: { fontSize: 13, color: COLORS.mutedText },
    progressTrack: {
        height: 8,
        backgroundColor: COLORS.lightGray,
        borderRadius: 999,
        marginTop: 10,
        marginBottom: 14,
        overflow: "hidden",
    },
    progressFill: { height: "100%", backgroundColor: COLORS.green, borderRadius: 999 },
    targetRows: { gap: 8 },
    targetRow: {
        flexDirection: "row",
        justifyContent: "space-between",
        alignItems: "center",
    },
    targetRowLabel: { fontSize: 12, color: COLORS.textMid },
    targetRowValue: { fontSize: 13, fontWeight: "700", color: COLORS.darkText },
    targetTrend: { fontSize: 11, fontWeight: "700" },
    targetMuted: { fontSize: 11, color: COLORS.mutedText },

    /* chart */
    chartCard: {
        backgroundColor: COLORS.bgCard,
        borderRadius: 14,
        borderWidth: 1,
        borderColor: COLORS.lightGray,
        padding: 12,
    },
    chartHeader: {
        flexDirection: "row",
        justifyContent: "space-between",
        alignItems: "center",
        marginBottom: 4,
    },
    chartTitle: { fontSize: 14, fontWeight: "700", color: COLORS.darkText },
});