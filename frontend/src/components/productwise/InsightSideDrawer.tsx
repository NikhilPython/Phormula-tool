'use client';

import React from 'react';
import Drawer from '@mui/material/Drawer';
import IconButton from '@mui/material/IconButton';
import { FaThumbsUp, FaThumbsDown } from 'react-icons/fa';
import Productinfoinpopup from '@/components/businessInsight/Productinfoinpopup';
import PageBreadcrumb from '../common/PageBreadCrumb';

// =========================
// Types (same as your page)
// =========================
export interface BestPerfItem {
  month: string;
  value: number;
}

export interface BestPerformance {
  sales?: BestPerfItem;
  units?: BestPerfItem;
  profit?: BestPerfItem;
}

export interface SkuInsight {
  product_name: string;
  insight: string;

  inventory_recommendation?: string;
  objective?: Record<string, any> | null;
  recommendation?: string;
  best_performance?: BestPerformance;

  [key: string]: any;
}


export interface InsightSideDrawerProps {
  open: boolean;
  selectedSku: string | null;

  skuInsights: Record<string, SkuInsight>;

  /** Your existing lookup (exact + global partial fallback etc.) */
  getInsightByProductName?: (productName: string) => [string, SkuInsight] | null;

  onClose: () => void;

  // Optional: feedback UI (hook to your existing submit flow)
  enableFeedback?: boolean;
  fbType?: 'like' | 'dislike' | null;
  setFbType?: (v: 'like' | 'dislike' | null) => void;
  fbText?: string;
  setFbText?: (v: string) => void;
  fbSubmitting?: boolean;
  fbSuccess?: boolean;
  onSubmitFeedback?: () => void;
}

// =========================
// Helpers (copied from your page)
// =========================
const highlightInsightText = (text: string) => {
  const greenWords = ['profit', 'profits', 'increase', 'growth', 'improvement', 'gain', 'gains', 'up', 'higher'];
  const redWords = ['loss', 'losses', 'decrease', 'decline', 'drop', 'down', 'lower'];

  const regex = new RegExp(`\\b(${[...greenWords, ...redWords].join('|')})\\b`, 'gi');
  const parts = text.split(regex);

  return parts.map((part, idx) => {
    const lower = part.toLowerCase();
    if (greenWords.includes(lower)) return <span key={idx} style={{ color: '#5EA68E', fontWeight: 600 }}>{part}</span>;
    if (redWords.includes(lower)) return <span key={idx} style={{ color: '#FF5C5C', fontWeight: 600 }}>{part}</span>;
    return <span key={idx}>{part}</span>;
  });
};

const renderFormattedInsight = (
  raw: string,
  feedback?: {
    enable?: boolean;
    fbType?: 'like' | 'dislike' | null;
    setFbType?: (v: 'like' | 'dislike' | null) => void;
    fbText?: string;
    setFbText?: (v: string) => void;
    fbSubmitting?: boolean;
    fbSuccess?: boolean;
    onSubmit?: () => void;
  }
) => {
  if (!raw) return null;

const lines = raw
  .split(/\r?\n/)
  .map((l) => l.trim())
  .filter(Boolean)
  .map((l) => l.replace(/^[-•–]\s*/, '')); // remove leading dash

// 🔥 If content is simple dash-style bullet list, render clean bullets
const isSimpleBulletList = raw.trim().startsWith('-');

if (isSimpleBulletList) {
  return (
    <ul
      style={{
        paddingLeft: 18,
        listStyle: 'disc',
        display: 'grid',
        gap: 12,
      }}
    >
      {lines.map((line, i) => (
        <li
          key={i}
          style={{
            fontSize: 14,
            lineHeight: 1.7,
            color: '#374151',
          }}
        >
          {highlightInsightText(line)}
        </li>
      ))}
    </ul>
  );
}



  const SECTION_ORDER = [
    'Details',
    'Observations',
    'Improvements',
    'Unit Growth',
    'ASP',
    'Sales',
    'Profit',
    'Unit Profitability',
    'Summary',
  ];

  const LIST_SECTIONS = new Set([
    'Observations',
    'Improvements',
    'Unit Growth',
    'ASP',
    'Sales',
    'Profit',
    'Unit Profitability',
  ]);

  const headingOf = (line: string): string | null => {
    const m =
      line.match(/^details\s+for/i) ? ['Details'] :
        line.match(/^(observations)\s*:?\s*$/i) ? ['Observations'] :
          line.match(/^(improvements)\s*:?\s*$/i) ? ['Improvements'] :
            line.match(/^(unit\s+growth)\s*:?\s*$/i) ? ['Unit Growth'] :
              line.match(/^(asp)\s*:?\s*$/i) ? ['ASP'] :
                line.match(/^(sales)\s*:?\s*$/i) ? ['Sales'] :
                  line.match(/^(profit)\s*:?\s*$/i) ? ['Profit'] :
                    line.match(/^(unit\s+profitability)\s*:?\s*$/i) ? ['Unit Profitability'] :
                      line.match(/^(summary)\s*:?\s*$/i) ? ['Summary'] :
                        null;
    return m ? m[0] : null;
  };

  const sections: Record<string, string[]> = {};
  let current: string | null = null;

  for (const line of lines) {
    const hd = headingOf(line);
    if (hd) {
      current = hd;
      if (!sections[current]) sections[current] = [];
      if (current === 'Details') sections[current].push(line);
      continue;
    }
    if (!current) current = 'Details';
    if (!sections[current]) sections[current] = [];

    const isLabel = !!line.match(
      /^(observations|improvements|unit\s+growth|asp|sales|profit|unit\s+profitability|summary)\s*:?\s*$/i
    );
    if (isLabel) continue;

    sections[current].push(line);
  }

  const clean = (s: string) =>
    s.replace(/^[•\-\u2013\u2014]\s+/, '').replace(/^\d+\.\s+/, '');

  return SECTION_ORDER.filter((sec) => sections[sec]?.length).map((sec, idx) => {
    const content = sections[sec];
    const isList = LIST_SECTIONS.has(sec);

    return (
      <div key={idx} style={{ marginBottom: 12 }}>
        {(isList || sec === 'Summary') && (
          <strong className="block mb-1.5 text-xs 2xl:text-sm text-charcoal-500 font-bold">
            {sec}
          </strong>
        )}

        {isList ? (
         <ul
  className="text-xs 2xl:text-sm text-charcoal-500"
  style={{
    margin: "10px 0 14px 0",
    paddingLeft: 18,     // controls left indent (reduce if too much)
    listStyle: "disc",
    display: "grid",
    gap: 10,             // space between bullet points
  }}
>

            {content.map((line, i) => {
              const trimmed = clean(line);

              const isSubHeading =
                /^[A-Za-z][A-Za-z\s\/]+:?$/i.test(trimmed) &&
                !trimmed.match(/\d|%|,/) &&
                trimmed.split(/\s+/).length <= 5;

              if (isSubHeading) {
                const label = trimmed.replace(/:$/, '').trim();
                return (
                  <li key={i} className="text-xs 2xl:text-sm text-charcoal-500" style={{ listStyle: 'none', marginTop: 10, marginBottom: 4 }}>
                    <span
                      style={{
                        fontWeight: 700,
                        // fontSize: 14,
                        color: '#374151',
                        borderLeft: '3px solid #60a68e',
                        paddingLeft: 8,
                      }}
                    >
                      {label}
                    </span>
                  </li>
                );
              }

              return (
               <li
  key={i}
  className="text-xs 2xl:text-sm text-charcoal-500"
  style={{
    lineHeight: 1.7,
    paddingLeft: 2,      // makes text align nicer after bulletconst lines = raw
  }}
>

                  {highlightInsightText(trimmed)}
                </li>
              );
            })}
          </ul>
        ) : (
          <div>
            {content.map((line, i) => (
              <p key={i} className="text-xs 2xl:text-sm text-charcoal-500" style={{ margin: '4px 0', lineHeight: 1.6}}>
                {highlightInsightText(line)}
              </p>
            ))}
          </div>
        )}

        {/* Optional feedback UI exactly like your Summary block */}
        {sec === 'Summary' && feedback?.enable && feedback?.setFbType && feedback?.setFbText && (
          <div style={{ marginTop: 10 }}>
            <div style={{ display: 'flex', justifyContent: 'center', gap: 12 }}>
              <button
                type="button"
                onClick={() => feedback.setFbType?.('like')}
                style={{ background: 'none', border: 'none', cursor: 'pointer', opacity: feedback.fbType === 'like' ? 1 : 0.6 }}
                title="Like"
              >
                <FaThumbsUp size={18} />
              </button>
              <button
                type="button"
                onClick={() => feedback.setFbType?.('dislike')}
                style={{ background: 'none', border: 'none', cursor: 'pointer', opacity: feedback.fbType === 'dislike' ? 1 : 0.6 }}
                title="Dislike"
              >
                <FaThumbsDown size={18} />
              </button>
            </div>

            <div
              style={{
                marginTop: 10,
                backgroundColor: '#f1f1f1',
                padding: '10px 12px',
                borderRadius: 8,
                display: 'flex',
                gap: 10,
                alignItems: 'center',
              }}
            >
              <input
                type="text"
                placeholder="Add a Comment......"
                value={feedback.fbText ?? ''}
                onChange={(e) => feedback.setFbText?.(e.target.value)}
                style={{ flex: 1, border: 'none', outline: 'none', background: 'transparent' }}
              />
              <button
                type="button"
                onClick={feedback.onSubmit}
                disabled={!!feedback.fbSubmitting}
                style={{
                  whiteSpace: 'nowrap',
                  padding: '8px 16px',
                  border: 'none',
                  borderRadius: 6,
                  cursor: 'pointer',
                  backgroundColor: '#2c3e50',
                  color: '#f8edcf',
                  fontWeight: 'bold',
                  boxShadow: '0 3px 6px rgba(0,0,0,.15)',
                }}
              >
                {feedback.fbSubmitting ? 'Submitting...' : 'Submit'}
              </button>
            </div>

            {feedback.fbSuccess && (
              <div style={{ color: '#2e7d32', fontWeight: 600, marginTop: 6 }}>
                Feedback submitted!
              </div>
            )}
          </div>
        )}
      </div>
    );
  });
};

// =========================
// Component
// =========================
const InsightSideDrawer: React.FC<InsightSideDrawerProps> = ({
  open,
  selectedSku,
  skuInsights,
  getInsightByProductName,
  onClose,

  enableFeedback = false,
  fbType,
  setFbType,
  fbText,
  setFbText,
  fbSubmitting,
  fbSuccess,
  onSubmitFeedback,
}) => {
  if (!open || !selectedSku) return null;

  const insightData =
  skuInsights[selectedSku as keyof typeof skuInsights] ||
  getInsightByProductName?.(selectedSku)?.[1];


  if (!insightData) return null;

  return (
    <Drawer
      anchor="right"
      open={open}
      onClose={onClose}
      PaperProps={{
        sx: {
          width: { xs: '100vw', sm: '80vw', md: '60vw', lg: '50vw' },
          maxWidth: 900,
          padding: 2,
        },
      }}
    >
      <div style={{ display: 'flex', flexDirection: 'column', gap: 12, height: '100%' }}>
        {/* Header */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            marginBottom: 8,
          }}
        >
          <PageBreadcrumb
            variant="page"
            align="left"
            textSize="2xl"
            pageTitle={
              <>
                AI Insight for{" "}
                <span style={{ color: "#60a68e" }}>
                  {insightData.product_name || selectedSku}
                </span>
              </>
            }
          />

          <IconButton size="small" onClick={onClose} aria-label="Close">
            ×
          </IconButton>
        </div>



        {/* Insight */}
<div style={{ flex: 1, overflowY: 'auto', marginTop: 8, paddingRight: 4 }}>
  <div
    style={{
      background: "#F9FAFB",
      border: "1px solid #E5E7EB",
      borderRadius: 16,
      padding: 16,
      display: "flex",
      flexDirection: "column",
      gap: 12,
    }}
  >
    {/* ✅ Best Performance */}
    {insightData.best_performance && (
      <div>
        <div style={{ fontWeight: 800, fontSize: 13, marginBottom: 10, color: "#111827" }}>
          Best Performance
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: 10 }}>
          <div style={{ border: "1px solid #E5E7EB", borderRadius: 14, padding: 12, background: "#fff" }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#111827" }}>Sales</div>
            <div style={{ fontSize: 12, color: "#6B7280" }}>{insightData.best_performance.sales?.month || "-"}</div>
            <div style={{ marginTop: 6, fontWeight: 800, color: "#111827" }}>
              {insightData.best_performance.sales?.value?.toLocaleString?.() ?? "-"}
            </div>
          </div>

          <div style={{ border: "1px solid #E5E7EB", borderRadius: 14, padding: 12, background: "#fff" }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#111827" }}>Units</div>
            <div style={{ fontSize: 12, color: "#6B7280" }}>{insightData.best_performance.units?.month || "-"}</div>
            <div style={{ marginTop: 6, fontWeight: 800, color: "#111827" }}>
              {insightData.best_performance.units?.value?.toLocaleString?.() ?? "-"}
            </div>
          </div>

          <div style={{ border: "1px solid #E5E7EB", borderRadius: 14, padding: 12, background: "#fff" }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#111827" }}>Profit</div>
            <div style={{ fontSize: 12, color: "#6B7280" }}>{insightData.best_performance.profit?.month || "-"}</div>
            <div style={{ marginTop: 6, fontWeight: 800, color: "#111827" }}>
              {insightData.best_performance.profit?.value?.toLocaleString?.() ?? "-"}
            </div>
          </div>
        </div>
      </div>
    )}

    
{/* ✅ Objective (Styled UI version) */}
{insightData.objective && (
  <div>
    <div style={{ fontWeight: 800, fontSize: 13, marginBottom: 10, color: "#111827" }}>
      Objective
    </div>

    <div
       className='flex  justify-between gap-4 w-full'
    >
      {/* Growth Intent */}
      {insightData.objective.growth_intent && (
        <div
          style={{
            background: "#fff",
            border: "1px solid #E5E7EB",
            borderRadius: 12,
            padding: 12,
            width: "100%",
          }}
        >
          <div style={{ fontSize: 11, color: "#6B7280", marginBottom: 4 }}>
            Growth Intent
          </div>
          <div style={{ fontWeight: 600, color: "#111827" }}>
            {insightData.objective.growth_intent}
          </div>
        </div>
      )}

      {/* Inventory Clearance Priority */}
      {typeof insightData.objective.inventory_clearance_priority !== "undefined" && (
        <div
          style={{
            background: "#fff",
            border: "1px solid #E5E7EB",
            borderRadius: 12,
            padding: 12,
            width: "100%",
          }}
        >
          <div style={{ fontSize: 11, color: "#6B7280", marginBottom: 4 }}>
            Inventory Clearance Priority
          </div>
          <div style={{ fontWeight: 600, color: "#111827" }}>
            {insightData.objective.inventory_clearance_priority ? "High Priority" : "No"}
          </div>
        </div>
      )}

      {/* Profit Priority */}
      {insightData.objective.profit_priority && (
        <div
          style={{
            background: "#fff",
            border: "1px solid #E5E7EB",
            borderRadius: 12,
            padding: 12,
            width: "100%",
          }}
        >
          <div style={{ fontSize: 11, color: "#6B7280", marginBottom: 4 }}>
            Profit Priority
          </div>
          <div style={{ fontWeight: 600, color: "#111827" }}>
            {insightData.objective.profit_priority}
          </div>
        </div>
      )}
    </div>
  </div>
)}

 <div>
  <div style={{ fontWeight: 800, fontSize: 13, marginBottom: 10, color: "#111827" }}>
         Product Journey
        </div>
      {renderFormattedInsight(insightData.insight, {
        enable: enableFeedback,
        fbType,
        setFbType,
        fbText,
        setFbText,
        fbSubmitting,
        fbSuccess,
        onSubmit: onSubmitFeedback,
      })}
    </div>

        {/* ✅ Recommendation */}
    {!!insightData.recommendation && (
      <div>
        <div style={{ fontWeight: 800, fontSize: 13, marginBottom: 6, color: "#111827" }}>
          Recommendation
        </div>
        <div style={{ background: "#fff", border: "1px solid #E5E7EB", borderRadius: 12, padding: 12 }}>
          <p style={{ margin: 0, fontSize: 13, lineHeight: 1.6, color: "#374151" }}>
            {insightData.recommendation}
          </p>
        </div>
      </div>
    )}

    {/* ✅ Inventory Recommendation */}
    {!!insightData.inventory_recommendation && (
      <div>
        <div style={{ fontWeight: 800, fontSize: 13, marginBottom: 6, color: "#111827" }}>
          Inventory Recommendation
        </div>
        <div style={{ background: "#fff", border: "1px solid #E5E7EB", borderRadius: 12, padding: 12 }}>
          <p style={{ margin: 0, fontSize: 13, lineHeight: 1.6, color: "#374151" }}>
            {insightData.inventory_recommendation}
          </p>
        </div>
      </div>
    )}



    {/* ✅ Existing AI Insight (bullets) */}
   
  </div>
</div>


      </div>
    </Drawer>
  );
};

export default InsightSideDrawer;
