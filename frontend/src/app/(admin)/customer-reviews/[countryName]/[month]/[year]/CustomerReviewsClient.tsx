"use client";

import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  BarChart3,
  ChevronDown,
  Boxes,
  ImageIcon,
  PackageSearch,
  RefreshCw,
  Search,
  Sparkles,
  Star,
  Tag,
  TrendingDown,
  TrendingUp,
} from "lucide-react";
import NextImage from "next/image";
import { useParams } from "next/navigation";
import { API_BASE } from "@/config/env";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";
import {
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionPanel,
} from "@/components/animate-ui/components/headless/accordion";
import Loader from "@/components/loader/Loader";

type ProductOption = {
  asin: string;
  sku?: string | null;
  sku_us?: string | null;
  sku_uk?: string | null;
  sku_canada?: string | null;
  title?: string | null;
  product_name?: string | null;
  product_barcode?: string | null;
  brand?: string | null;
  main_image_url?: string | null;
  status?: string | null;
  price?: number | string | null;
  currency?: string | null;
};

type Topic = {
  topic?: string;
  asinMetrics?: {
    numberOfMentions?: number;
    occurrencePercentage?: number;
    starRatingImpact?: number;
  };
  parentAsinMetrics?: {
    numberOfMentions?: number;
    occurrencePercentage?: number;
    starRatingImpact?: number;
  };
  reviewSnippets?: string[];
  subtopics?: {
    subtopic?: string;
    metrics?: {
      numberOfMentions?: number;
      occurrencePercentage?: number;
    };
    reviewSnippets?: string[];
  }[];
};

type TrendTopic = {
  topic?: string;
  trendMetrics?: {
    dateRange?: {
      startDate?: string;
      endDate?: string;
    };
    asinMetrics?: {
      occurrencePercentage?: number;
      starRatingImpact?: number;
    };
    browseNodeMetrics?: {
      occurrencePercentage?: {
        allProducts?: number;
        topTwentyFivePercentProducts?: number;
      };
    };
  }[];
};

type TrendTopicGroup = {
  positiveTrends?: TrendTopic[];
  negativeTrends?: TrendTopic[];
  positiveTopics?: TrendTopic[];
  negativeTopics?: TrendTopic[];
};

type ReviewTrendsGroup = {
  positiveTopics?: TrendTopic[];
  negativeTopics?: TrendTopic[];
};

type TrendResponsePayload = {
  reviewTrends?: ReviewTrendsGroup;
  trends?: TrendTopicGroup;
  payload?: {
    reviewTrends?: ReviewTrendsGroup;
    trends?: TrendTopicGroup;
  };
};

type FeedbackResponse = {
  success?: boolean;
  error?: string;
  asin?: string;
  marketplace_id?: string;
  country_code?: string;
  product?: ProductOption | null;
  topics?: {
    asin?: string;
    itemName?: string;
    dateRange?: {
      startDate?: string;
      endDate?: string;
    };
    topics?: {
      positiveTopics?: Topic[];
      negativeTopics?: Topic[];
    };
  } | null;
  rating_impact_topics?: {
    topics?: {
      positiveTopics?: Topic[];
      negativeTopics?: Topic[];
    };
  } | null;
  trends?: TrendResponsePayload | null;
  browse_node?: {
    browseNodeId?: string;
    displayName?: string;
    unavailable?: boolean;
  } | null;
};

const MARKETPLACE_BY_COUNTRY: Record<string, string> = {
  us: "ATVPDKIKX0DER",
  uk: "A1F83G8C2ARO7P",
  ca: "A2EUQ1WTGCTBG2",
};

const MARKETPLACE_LABELS: Record<string, string> = {
  us: "Amazon US",
  uk: "Amazon UK",
  ca: "Amazon CA",
};

const MONTH_NUMBER_BY_NAME: Record<string, number> = {
  january: 1,
  february: 2,
  march: 3,
  april: 4,
  may: 5,
  june: 6,
  july: 7,
  august: 8,
  september: 9,
  october: 10,
  november: 11,
  december: 12,
};

function normalizeCatalogKey(value?: string | null) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");
}

const EMPTY_TOPICS: Topic[] = [];
const EMPTY_TRENDS: TrendTopic[] = [];

function getToken() {
  if (typeof window === "undefined") return null;
  return localStorage.getItem("jwtToken");
}

function formatDate(value?: string) {
  if (!value) return "NA";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "NA";
  return date.toLocaleDateString(undefined, {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function metricValue(value?: number, suffix = "") {
  if (typeof value !== "number" || Number.isNaN(value)) return "NA";
  return `${Math.round(value * 10) / 10}${suffix}`;
}

function ratingImpactValue(value: number | null) {
  if (typeof value !== "number" || Number.isNaN(value)) return "NA";
  const rounded = Math.round(value * 100) / 100;
  const sign = rounded > 0 ? "+" : "";
  return `${sign}${rounded.toFixed(2)}`;
}

function productName(product?: ProductOption | null, fallback?: string | null) {
  return product?.product_name || product?.title || fallback || "";
}

function skuLabel(product?: ProductOption | null) {
  return product?.sku || product?.sku_us || product?.sku_uk || product?.sku_canada || "";
}

function getTopicImpact(topic: Topic) {
  const value =
    topic.asinMetrics?.starRatingImpact ??
    topic.parentAsinMetrics?.starRatingImpact;
  return typeof value === "number" && !Number.isNaN(value) ? value : null;
}

function getRatingImpactSummary(topics: Topic[]) {
  const impacts = topics
    .map(getTopicImpact)
    .filter((value): value is number => typeof value === "number");

  if (!impacts.length) return null;

  return impacts.reduce((total, current) => total + current, 0);
}

function findImpactForTopic(topic: Topic, impactTopics: Topic[]) {
  const topicName = String(topic.topic || "").trim().toLowerCase();
  if (!topicName) return getTopicImpact(topic);

  const match = impactTopics.find(
    (candidate) => String(candidate.topic || "").trim().toLowerCase() === topicName
  );

  return getTopicImpact(match || topic);
}

function normalizedTopicName(value?: string) {
  return String(value || "").trim().toLowerCase();
}

function uniqueStrings(values: (string | undefined | null)[]) {
  const seen = new Set<string>();
  const result: string[] = [];

  values.forEach((value) => {
    const cleaned = String(value || "").trim();
    const key = cleaned.toLowerCase();

    if (!cleaned || seen.has(key)) return;

    seen.add(key);
    result.push(cleaned);
  });

  return result;
}

function mergeSubtopics(
  primary: Topic["subtopics"] = [],
  secondary: Topic["subtopics"] = []
) {
  const merged = [...primary];
  const indexByName = new Map(
    merged.map((subtopic, index) => [normalizedTopicName(subtopic.subtopic), index])
  );

  secondary.forEach((subtopic) => {
    const key = normalizedTopicName(subtopic.subtopic);
    const existingIndex = indexByName.get(key);

    if (!key || existingIndex === undefined) {
      if (key) indexByName.set(key, merged.length);
      merged.push(subtopic);
      return;
    }

    const existing = merged[existingIndex];
    merged[existingIndex] = {
      ...subtopic,
      ...existing,
      metrics: {
        ...subtopic.metrics,
        ...existing.metrics,
      },
      reviewSnippets: uniqueStrings([
        ...(existing.reviewSnippets || []),
        ...(subtopic.reviewSnippets || []),
      ]),
    };
  });

  return merged;
}

function mergeTopicsByName(primary: Topic[], secondary: Topic[]) {
  const merged: Topic[] = [];
  const indexByName = new Map<string, number>();

  [...primary, ...secondary].forEach((topic) => {
    const key = normalizedTopicName(topic.topic);

    if (!key) {
      merged.push(topic);
      return;
    }

    const existingIndex = indexByName.get(key);

    if (existingIndex === undefined) {
      indexByName.set(key, merged.length);
      merged.push({
        ...topic,
        reviewSnippets: uniqueStrings(topic.reviewSnippets || []),
        subtopics: mergeSubtopics([], topic.subtopics || []),
      });
      return;
    }

    const existing = merged[existingIndex];

    merged[existingIndex] = {
      ...existing,
      ...topic,

      // Keep a single title entry, but preserve the most useful metrics.
      asinMetrics: {
        ...topic.asinMetrics,
        ...existing.asinMetrics,
        numberOfMentions: Math.max(
          Number(existing.asinMetrics?.numberOfMentions || 0),
          Number(topic.asinMetrics?.numberOfMentions || 0)
        ),
        occurrencePercentage: Math.max(
          Number(existing.asinMetrics?.occurrencePercentage || 0),
          Number(topic.asinMetrics?.occurrencePercentage || 0)
        ),
        starRatingImpact:
          existing.asinMetrics?.starRatingImpact ??
          topic.asinMetrics?.starRatingImpact,
      },

      parentAsinMetrics: {
        ...topic.parentAsinMetrics,
        ...existing.parentAsinMetrics,
        numberOfMentions: Math.max(
          Number(existing.parentAsinMetrics?.numberOfMentions || 0),
          Number(topic.parentAsinMetrics?.numberOfMentions || 0)
        ),
        occurrencePercentage: Math.max(
          Number(existing.parentAsinMetrics?.occurrencePercentage || 0),
          Number(topic.parentAsinMetrics?.occurrencePercentage || 0)
        ),
        starRatingImpact:
          existing.parentAsinMetrics?.starRatingImpact ??
          topic.parentAsinMetrics?.starRatingImpact,
      },

      // Preserve every distinct review returned for this title.
      reviewSnippets: uniqueStrings([
        ...(existing.reviewSnippets || []),
        ...(topic.reviewSnippets || []),
      ]),

      // Preserve/merge all subtopics and their review snippets as well.
      subtopics: mergeSubtopics(
        existing.subtopics || [],
        topic.subtopics || []
      ),
    };
  });

  return merged;
}

function getTopicReviewSnippets(topic: Topic) {
  return uniqueStrings([
    ...(topic.reviewSnippets || []),
    ...(topic.subtopics || []).flatMap((subtopic) => subtopic.reviewSnippets || []),
  ]);
}

function countUniqueReviewSnippets(topics: Topic[]) {
  return uniqueStrings(topics.flatMap(getTopicReviewSnippets)).length;
}

function RatingStars({
  value,
  size = "sm",
}: {
  value: number | null;
  size?: "xs" | "sm";
}) {
  const iconSize = size === "xs" ? "h-3.5 w-3.5" : "h-4 w-4";
  const filledCount =
    typeof value === "number"
      ? Math.max(0, Math.min(5, Math.round(Math.abs(value))))
      : 0;

  return (
    <div className="flex items-center gap-0.5" aria-label="Rating impact stars">
      {Array.from({ length: 5 }).map((_, index) => {
        const filled = index < filledCount;
        return (
          <Star
            key={index}
            className={`${iconSize} ${filled ? "fill-amber-400 text-amber-400" : "text-gray-300"
              }`}
          />
        );
      })}
    </div>
  );
}

function ProductThumb({
  imageUrl,
  asin,
  alt,
  size = "large",
}: {
  imageUrl?: string | null;
  asin?: string | null;
  alt: string;
  size?: "small" | "large";
}) {
  const candidates = useMemo(() => {
    const urls = [imageUrl];
    const cleanAsin = String(asin || "").trim().toUpperCase();

    if (cleanAsin) {
      urls.push(
        `https://m.media-amazon.com/images/P/${cleanAsin}.01._SCLZZZZZZZ_.jpg`,
        `https://images-na.ssl-images-amazon.com/images/P/${cleanAsin}.01._SCLZZZZZZZ_.jpg`
      );
    }

    return Array.from(new Set(urls.filter(Boolean) as string[]));
  }, [asin, imageUrl]);

  const [sourceIndex, setSourceIndex] = useState(0);
  const sizeClass = size === "small" ? "h-12 w-12" : "h-24 w-24";
  const iconClass = size === "small" ? "h-4 w-4" : "h-6 w-6";
  const imageSize = size === "small" ? 48 : 96;
  const currentSource = candidates[sourceIndex];
  const candidateSignature = candidates.join("|");

  useEffect(() => {
    setSourceIndex(0);
  }, [candidateSignature]);

  if (currentSource) {
    return (
      <NextImage
        unoptimized
        src={currentSource}
        alt={alt}
        width={imageSize}
        height={imageSize}
        onError={() => setSourceIndex((index) => index + 1)}
        className={`${sizeClass} shrink-0 rounded-md border border-gray-200 bg-white object-contain`}
      />
    );
  }

  return (
    <div
      className={`${sizeClass} flex shrink-0 items-center justify-center rounded-md border border-dashed border-gray-300 bg-gray-50 text-gray-400`}
      title="Product image unavailable"
    >
      <ImageIcon className={iconClass} />
    </div>
  );
}

function pickTrendTopics(response: FeedbackResponse | null, sentiment: "positive" | "negative") {
  const trendPayload = response?.trends;
  const trends: TrendTopicGroup | undefined =
    trendPayload?.reviewTrends ||
    trendPayload?.trends ||
    trendPayload?.payload?.reviewTrends ||
    trendPayload?.payload?.trends;

  if (!trends) return [];

  if (sentiment === "positive") {
    return trends.positiveTrends || trends.positiveTopics || EMPTY_TRENDS;
  }

  return trends.negativeTrends || trends.negativeTopics || EMPTY_TRENDS;
}

type TrendMetric = NonNullable<TrendTopic["trendMetrics"]>[number];

function trendMetricValue(metric?: TrendMetric) {
  return (
    metric?.asinMetrics?.occurrencePercentage ??
    metric?.asinMetrics?.starRatingImpact ??
    metric?.browseNodeMetrics?.occurrencePercentage?.allProducts ??
    metric?.browseNodeMetrics?.occurrencePercentage?.topTwentyFivePercentProducts ??
    0
  );
}

function trendMetricTime(metric: TrendMetric) {
  const dateValue = metric.dateRange?.endDate || metric.dateRange?.startDate || "";
  const timestamp = new Date(dateValue).getTime();
  return Number.isNaN(timestamp) ? 0 : timestamp;
}

function sortedTrendMetrics(topic: TrendTopic) {
  return [...(topic.trendMetrics || [])].sort(
    (left, right) => trendMetricTime(left) - trendMetricTime(right)
  );
}

function latestTrendValue(topic: TrendTopic) {
  const metrics = sortedTrendMetrics(topic);
  return trendMetricValue(metrics[metrics.length - 1]);
}

function getTrendSummary(topic: TrendTopic) {
  const metrics = sortedTrendMetrics(topic);
  const latestMetric = metrics[metrics.length - 1];
  const previousMetric = metrics[metrics.length - 2];
  const latestValue = trendMetricValue(latestMetric);
  const previousValue = trendMetricValue(previousMetric);

  return {
    metrics,
    latestMetric,
    latestValue,
    delta: metrics.length > 1 ? latestValue - previousValue : 0,
  };
}

function formatTrendDelta(delta: number) {
  if (Math.abs(delta) < 0.05) return "0 pp";
  return `${delta > 0 ? "+" : ""}${metricValue(delta)} pp`;
}

function formatTrendPeriod(metric?: TrendMetric) {
  const value = metric?.dateRange?.endDate || metric?.dateRange?.startDate;
  if (!value) return "NA";

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "NA";

  return date.toLocaleDateString(undefined, {
    month: "short",
    year: "numeric",
  });
}

function getLatestTrendPeriod(topics: TrendTopic[]) {
  const latestMetric = topics
    .map((topic) => getTrendSummary(topic).latestMetric)
    .filter((metric): metric is TrendMetric => Boolean(metric))
    .sort((left, right) => trendMetricTime(right) - trendMetricTime(left))[0];

  return formatTrendPeriod(latestMetric);
}

function TrendHistoryTicks({
  metrics,
  tone,
}: {
  metrics: TrendMetric[];
  tone: "green" | "red";
}) {
  if (!metrics.length) {
    return <div className="h-2 rounded-full bg-gray-100" aria-hidden="true" />;
  }

  const values = metrics.map(trendMetricValue);
  const maxValue = Math.max(1, ...values);
  const color = tone === "green" ? "bg-green-500" : "bg-red-600";

  return (
    <div className="flex h-2 gap-1" aria-hidden="true">
      {metrics.map((metric, index) => (
        <span
          key={`${metric.dateRange?.startDate || index}-${index}`}
          className={`flex-1 rounded-full ${trendMetricValue(metric) > 0 ? color : "bg-gray-100"}`}
          style={{
            opacity:
              trendMetricValue(metric) > 0
                ? Math.max(0.35, trendMetricValue(metric) / maxValue)
                : 1,
          }}
          title={`${formatTrendPeriod(metric)}: ${metricValue(trendMetricValue(metric), "%")}`}
        />
      ))}
    </div>
  );
}

function TopicCard({
  topic,
  sentiment,
  impactTopics,
  defaultOpen = false,
}: {
  topic: Topic;
  sentiment: "positive" | "negative";
  impactTopics?: Topic[];
  defaultOpen?: boolean;
}) {
  const impact = findImpactForTopic(topic, impactTopics || []);
  const hasImpact = impact !== null;
  const reviewSnippets = getTopicReviewSnippets(topic);

  return (
    <Accordion className="w-full">
      <AccordionItem
        className="overflow-hidden rounded-lg border border-gray-200 bg-white shadow-sm"
        defaultOpen={defaultOpen}
      >
        <AccordionButton className="w-full px-4 py-3 text-left text-sm font-semibold text-gray-900 hover:bg-gray-50">
          {topic.topic || "Untitled topic"}
        </AccordionButton>

        <AccordionPanel>
          <div className="border-t border-gray-100 px-4 py-3">
            {/* {hasImpact && (
              <div className="mb-3 flex items-center gap-2 rounded-md bg-amber-50 px-3 py-2 text-xs text-amber-900">
                <RatingStars value={impact} size="xs" />
                <span>
                  Rating impact {metricValue(impact)}
                </span>
              </div>
            )} */}

            {reviewSnippets.length ? (
              <ul className="space-y-1.5 pl-5 text-sm text-gray-700">
                {reviewSnippets.map((snippet, index) => (
                  <li
                    key={`${snippet}-${index}`}
                    className="list-disc leading-5"
                  >
                    {snippet}
                  </li>
                ))}
              </ul>
            ) : (
              <p className="text-sm text-gray-500">
                No review snippets returned for this topic.
              </p>
            )}
          </div>
        </AccordionPanel>
      </AccordionItem>
    </Accordion>
  );
}

function SentimentTrendOverview({
  positiveTopics,
  negativeTopics,
  positiveReviewCount,
  negativeReviewCount,
  ratingImpact,
  // loading = false,
}: {
  positiveTopics: Topic[];
  negativeTopics: Topic[];
  positiveReviewCount: number;
  negativeReviewCount: number;
  ratingImpact: number | null;
  // loading?: boolean;
}) {
  const positiveTotal = Math.max(0, positiveReviewCount);
  const negativeTotal = Math.max(0, negativeReviewCount);

  const combinedTotal = positiveTotal + negativeTotal;

  const positivePct =
    combinedTotal > 0
      ? (positiveTotal / combinedTotal) * 100
      : 0;

  const negativePct =
    combinedTotal > 0
      ? (negativeTotal / combinedTotal) * 100
      : 0;

  // const positivePeriod = getLatestTrendPeriod(positiveTopics);
  // const negativePeriod = getLatestTrendPeriod(negativeTopics);
  // const period = positivePeriod !== "NA" ? positivePeriod : negativePeriod;

  // const sortedPositive = [...positiveTopics].sort(
  //   (a, b) => latestTrendValue(b) - latestTrendValue(a)
  // );
  // const sortedNegative = [...negativeTopics].sort(
  //   (a, b) => latestTrendValue(b) - latestTrendValue(a)
  // );

  return (
    <section className="rounded-xl border border-gray-200 bg-white shadow-sm">
      <div className="flex flex-col gap-1 border-b border-gray-100 px-4 py-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <PageBreadcrumb pageTitle="Review Analysis" align="left" textSize="xl" />

          {/* <h3 className="text-sm font-semibold text-gray-900">Review sentiment trends</h3> */}

          {/* {period !== "NA" && (
            <p className="mt-0.5 text-[11px] font-medium text-gray-500">Period {period}</p>
          )} */}
        </div>
        {ratingImpact !== null && (
          <div className="flex items-center gap-2 rounded-full border border-amber-200 bg-amber-50 px-3 py-1.5 text-xs font-semibold text-amber-700">
            <RatingStars value={ratingImpact} size="xs" />
            <span>Product impact {ratingImpactValue(ratingImpact)}</span>
          </div>
        )}
      </div>

      {/* {loading ? (
        <div className="flex min-h-[150px] items-center justify-center gap-2 px-4 py-6 text-sm text-gray-500">
          <RefreshCw className="h-4 w-4 animate-spin text-[#5EA68E]" />
          <span>Loading review sentiment...</span>
        </div>
      ) : ( */}
      <>
        <div className="border-b border-gray-100 px-4 py-4">
          <div className="mb-2 flex items-center justify-between gap-3 text-xs font-semibold">
            <span className="text-green-700">Positive {metricValue(positivePct, "%")}</span>
            <span className="text-red-600">Negative {metricValue(negativePct, "%")}</span>
          </div>

          <div className="h-2 w-full overflow-hidden rounded-full">
            <div
              className="h-full w-full rounded-full"
              style={{
                background: `linear-gradient(
        90deg,
        #10b981 0%,
        #10b981 ${Math.max(0, positivePct - 6)}%,
        #f59e8b ${positivePct}%,
        #fb7185 ${Math.min(100, positivePct + 6)}%,
        #f43f5e 100%
      )`,
              }}
            />
          </div>
        </div>

        <div className="grid gap-4 p-4 lg:grid-cols-2">
          <div className="min-w-0 rounded-xl border border-t-4 border-green-500 bg-white p-3 shadow-sm">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <TrendingUp className="h-4 w-4 text-green-500" />
                <h4 className="text-sm font-semibold text-gray-900">Positive</h4>
              </div>
            </div>

            {positiveTopics.length ? (
              <div className="flex flex-wrap gap-2">
                {positiveTopics.map((topic, index) => (
                  <span
                    key={`${topic.topic}-${index}`}
                    className="inline-flex max-w-full items-center gap-2 rounded-full border border-gray-200 bg-white px-3 py-1.5 text-xs font-medium text-charcoal-500 shadow-sm"
                    title={topic.topic || "Positive topic"}
                  >
                    <span className="max-w-[220px] truncate">
                      {topic.topic || "Topic"}
                    </span>
                  </span>
                ))}
              </div>
            ) : (
              <p className="text-sm text-gray-500">
                No positive reviews returned for this ASIN.
              </p>
            )}
          </div>

          <div className="min-w-0 rounded-xl border border-t-4 border-red-600 bg-white p-3 shadow-sm">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div className="flex items-center gap-2">
                <TrendingDown className="h-4 w-4 text-red-600" />
                <h4 className="text-sm font-semibold text-gray-900">Negative</h4>
              </div>
            </div>

            {negativeTopics.length ? (
              <div className="flex flex-wrap gap-2">
                {negativeTopics.map((topic, index) => (
                  <span
                    key={`${topic.topic}-${index}`}
                    className="inline-flex max-w-full items-center gap-2 rounded-full border border-gray-200 bg-white px-3 py-1.5 text-xs font-medium text-charcoal-500 shadow-sm"
                    title={topic.topic || "Negative topic"}
                  >
                    <span className="max-w-[220px] truncate">
                      {topic.topic || "Topic"}
                    </span>
                  </span>
                ))}
              </div>
            ) : (
              <p className="text-sm text-gray-500">
                No negative reviews returned for this ASIN.
              </p>
            )}
          </div>
        </div>
      </>
      {/* )} */}
    </section>
  );
}

function sortTopicsByTrendOrder(
  topics: Topic[],
  trends: TrendTopic[]
) {
  const trendOrder = new Map(
    [...trends]
      .sort((a, b) => latestTrendValue(b) - latestTrendValue(a))
      .map((trend, index) => [
        normalizedTopicName(trend.topic),
        index,
      ])
  );

  return [...topics].sort((a, b) => {
    const aOrder =
      trendOrder.get(normalizedTopicName(a.topic)) ??
      Number.MAX_SAFE_INTEGER;

    const bOrder =
      trendOrder.get(normalizedTopicName(b.topic)) ??
      Number.MAX_SAFE_INTEGER;

    return aOrder - bOrder;
  });
}

export default function CustomerReviewsClient() {
  const routeParams = useParams<{
    countryName?: string;
    month?: string;
    year?: string;
  }>();
  const countryName = String(routeParams.countryName || "us").toLowerCase();
  const marketplaceId = MARKETPLACE_BY_COUNTRY[countryName] || MARKETPLACE_BY_COUNTRY.us;
  const marketplaceLabel = MARKETPLACE_LABELS[countryName] || "Amazon";
  const routeMonth = String(routeParams.month || "").trim().toLowerCase();
  const routeYear = String(routeParams.year || "").trim();

  const [products, setProducts] = useState<ProductOption[]>([]);
  const [productsLoading, setProductsLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [asin, setAsin] = useState("");
  const [feedback, setFeedback] = useState<FeedbackResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [pnlSkuOrder, setPnlSkuOrder] = useState<Map<string, number>>(new Map());
  const [pnlNameOrder, setPnlNameOrder] = useState<Map<string, number>>(new Map());
  const [pnlOrderLoaded, setPnlOrderLoaded] = useState(false);
  const [defaultSelectionKey, setDefaultSelectionKey] = useState("");

  const mentionPositiveTopics = feedback?.topics?.topics?.positiveTopics ?? EMPTY_TOPICS;
  const mentionNegativeTopics = feedback?.topics?.topics?.negativeTopics ?? EMPTY_TOPICS;
  const positiveImpactTopics =
    feedback?.rating_impact_topics?.topics?.positiveTopics ?? EMPTY_TOPICS;
  const negativeImpactTopics =
    feedback?.rating_impact_topics?.topics?.negativeTopics ?? EMPTY_TOPICS;
  const positiveTopics = useMemo(
    () =>
      mergeTopicsByName(mentionPositiveTopics, positiveImpactTopics).filter(
        (topic) => getTopicReviewSnippets(topic).length > 0
      ),
    [mentionPositiveTopics, positiveImpactTopics]
  );

  const negativeTopics = useMemo(
    () =>
      mergeTopicsByName(mentionNegativeTopics, negativeImpactTopics).filter(
        (topic) => getTopicReviewSnippets(topic).length > 0
      ),
    [mentionNegativeTopics, negativeImpactTopics]
  );
  const positiveReviewCount = useMemo(
    () => countUniqueReviewSnippets(positiveTopics),
    [positiveTopics]
  );
  const negativeReviewCount = useMemo(
    () => countUniqueReviewSnippets(negativeTopics),
    [negativeTopics]
  );
  const allTopics = useMemo(
    () => [
      ...positiveTopics,
      ...negativeTopics,
    ],
    [negativeTopics, positiveTopics]
  );
  const positiveTrends = useMemo(() => pickTrendTopics(feedback, "positive"), [feedback]);
  const negativeTrends = useMemo(() => pickTrendTopics(feedback, "negative"), [feedback]);

  const orderedPositiveTopics = useMemo(
    () => sortTopicsByTrendOrder(positiveTopics, positiveTrends),
    [positiveTopics, positiveTrends]
  );

  const orderedNegativeTopics = useMemo(
    () => sortTopicsByTrendOrder(negativeTopics, negativeTrends),
    [negativeTopics, negativeTrends]
  );

  const orderedProducts = useMemo(() => {
    if (!products.length) return products;

    return products
      .map((product, originalIndex) => {
        const skuKeys = [
          product.sku,
          product.sku_us,
          product.sku_uk,
          product.sku_canada,
        ]
          .map((value) => String(value || "").trim().toUpperCase())
          .filter(Boolean);

        const skuRank = skuKeys.reduce<number | null>((best, key) => {
          const rank = pnlSkuOrder.get(key);
          if (rank === undefined) return best;
          return best === null ? rank : Math.min(best, rank);
        }, null);

        const nameRank = pnlNameOrder.get(
          normalizeCatalogKey(productName(product))
        );

        return {
          product,
          originalIndex,
          rank: skuRank ?? nameRank ?? Number.MAX_SAFE_INTEGER,
        };
      })
      .sort((left, right) => {
        if (left.rank !== right.rank) return left.rank - right.rank;
        return left.originalIndex - right.originalIndex;
      })
      .map(({ product }) => product);
  }, [products, pnlSkuOrder, pnlNameOrder]);

  const selectedProduct = useMemo(() => {
    return feedback?.product || products.find((product) => product.asin === asin) || null;
  }, [asin, feedback?.product, products]);
  const selectedAsin = feedback?.asin || selectedProduct?.asin || asin || "";
  const selectedSku = skuLabel(selectedProduct);
  const selectedProductName = productName(
    selectedProduct,
    feedback?.topics?.itemName || null
  );
  const selectedBrowseNode = feedback?.browse_node?.displayName || "";
  const ratingImpact = useMemo(
    () => getRatingImpactSummary(allTopics),
    [allTopics]
  );

  const reviewStartDate = feedback?.topics?.dateRange?.startDate;
  const reviewEndDate = feedback?.topics?.dateRange?.endDate;

  const pageLoading =
    productsLoading ||
    !pnlOrderLoaded ||
    loading ||
    (!feedback && orderedProducts.length > 0 && !error);

  const fetchPnlProductOrder = useCallback(async () => {
    const token = getToken();
    setPnlOrderLoaded(false);

    if (!token) {
      setPnlOrderLoaded(true);
      return;
    }

    const monthNumber = MONTH_NUMBER_BY_NAME[routeMonth];
    const yearNumber = Number(routeYear);

    if (!monthNumber || !Number.isFinite(yearNumber)) {
      setPnlSkuOrder(new Map());
      setPnlNameOrder(new Map());
      setPnlOrderLoaded(true);
      return;
    }

    try {
      const platform =
        countryName === "uk"
          ? "amazon-uk"
          : countryName === "ca"
            ? "amazon-ca"
            : "amazon-us";

      const params = new URLSearchParams({
        country: countryName,
        platform,
        region: countryName.toUpperCase(),
        month: String(monthNumber),
        year: String(yearNumber),
      });

      const response = await fetch(
        `${API_BASE}/amazon_api/live-dashboard/save?${params.toString()}`,
        {
          method: "GET",
          headers: {
            Authorization: `Bearer ${token}`,
            Accept: "application/json",
          },
          cache: "no-store",
        }
      );

      const json = await response.json().catch(() => null);
      if (!response.ok || !json?.success) {
        setPnlSkuOrder(new Map());
        setPnlNameOrder(new Map());
        return;
      }

      const payload = json?.data?.payload;
      const possibleRows = [
        payload?.data?.skuwise_items,
        payload?.data?.skuwise_items_us,
        payload?.data?.skuwise_items_uk,
        payload?.data?.skuwise_items_ca,
        payload?.liveBiPayload?.skuwise_items,
        payload?.liveBiPayload?.skuwise_items_us,
        payload?.liveBiPayload?.skuwise_items_uk,
        payload?.liveBiPayload?.skuwise_items_ca,
      ];

      const rows =
        possibleRows.find((value) => Array.isArray(value) && value.length) || [];

      const bodyRows = rows
        .filter((row: any) => {
          const sku = String(row?.sku || "").trim().toUpperCase();
          const name = normalizeCatalogKey(row?.product_name);

          return (
            !row?.isTotal &&
            sku !== "GRAND_TOTAL" &&
            sku !== "TOTAL" &&
            name !== "grand total" &&
            name !== "total" &&
            name !== "others"
          );
        })
        .sort(
          (left: any, right: any) =>
            Number(right?.net_sales || 0) - Number(left?.net_sales || 0)
        );

      const skuOrder = new Map<string, number>();
      const nameOrder = new Map<string, number>();

      bodyRows.forEach((row: any, index: number) => {
        const sku = String(row?.sku || "").trim().toUpperCase();
        const name = normalizeCatalogKey(row?.product_name);

        if (sku && !skuOrder.has(sku)) skuOrder.set(sku, index);
        if (name && !nameOrder.has(name)) nameOrder.set(name, index);
      });

      setPnlSkuOrder(skuOrder);
      setPnlNameOrder(nameOrder);
    } catch (err) {
      console.warn("Could not load P&L product order for Customer Reviews:", err);
      setPnlSkuOrder(new Map());
      setPnlNameOrder(new Map());
    } finally {
      setPnlOrderLoaded(true);
    }
  }, [countryName, routeMonth, routeYear]);

  const fetchProducts = useCallback(async (query = "") => {
    const token = getToken();
    if (!token) {
      setError("Missing login token. Please sign in again.");
      return;
    }

    setProductsLoading(true);
    setError("");

    try {
      const params = new URLSearchParams({
        marketplace_id: marketplaceId,
        limit: "40",
        include_images: "true",
      });

      if (query.trim()) {
        params.set("search", query.trim());
      }

      const response = await fetch(`${API_BASE}/amazon_api/customer-feedback/products?${params.toString()}`, {
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/json",
        },
      });
      const json = await response.json().catch(() => ({}));

      if (!response.ok || !json?.success) {
        throw new Error(json?.error || "Failed to load synced ASINs");
      }

      setProducts(json.products || []);
    } catch (err) {
      setProducts([]);
      setError(err instanceof Error ? err.message : "Failed to load products");
    } finally {
      setProductsLoading(false);
    }
  }, [marketplaceId]);

  const fetchFeedback = async (targetAsin = asin) => {
    const token = getToken();
    const cleanAsin = targetAsin.trim().toUpperCase();

    if (!token) {
      setError("Missing login token. Please sign in again.");
      return;
    }

    if (!/^[A-Z0-9]{10}$/.test(cleanAsin)) {
      setError("Enter a valid 10-character child ASIN.");
      return;
    }

    setAsin(cleanAsin);
    setFeedback(null);
    setLoading(true);
    setError("");

    try {
      const params = new URLSearchParams({
        asin: cleanAsin,
        marketplace_id: marketplaceId,
      });

      const response = await fetch(`${API_BASE}/amazon_api/customer-feedback/item-reviews?${params.toString()}`, {
        headers: {
          Authorization: `Bearer ${token}`,
          Accept: "application/json",
        },
      });
      const json = await response.json().catch(() => ({}));

      if (!response.ok || !json?.success) {
        throw new Error(json?.error || "Failed to fetch Amazon customer review insights");
      }

      setFeedback(json);
    } catch (err) {
      setFeedback(null);
      setError(err instanceof Error ? err.message : "Failed to fetch customer review insights");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchProducts();
    fetchPnlProductOrder();
  }, [fetchProducts, fetchPnlProductOrder]);

  useEffect(() => {
    const firstProduct = orderedProducts[0];
    const routeKey = `${countryName}:${routeMonth}:${routeYear}`;

    if (
      productsLoading ||
      !pnlOrderLoaded ||
      !firstProduct ||
      defaultSelectionKey === routeKey
    ) {
      return;
    }

    setDefaultSelectionKey(routeKey);
    setAsin(firstProduct.asin);
    void fetchFeedback(firstProduct.asin);
  }, [
    countryName,
    routeMonth,
    routeYear,
    orderedProducts,
    productsLoading,
    pnlOrderLoaded,
    defaultSelectionKey,
  ]);

  return (
    <div className=" mt-4 relative min-h-[calc(100vh-100px)] flex flex-col gap-4">
      {pageLoading && (
        <Loader
          contained
          fullscreen
          backgroundClass="bg-white/80"
          roundedClass="rounded-none"
          label="Loading customer review insights..."
          zIndex={50}
        />
      )}

      {/* <div>
        <PageBreadcrumb pageTitle="Customer Reviews" align="left" />
        <p className="mt-1 text-sm text-gray-500">
          {marketplaceLabel} - Customer Feedback insights
        </p>
      </div> */}

      {error && (
        <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      <div className="grid items-start gap-4 lg:grid-cols-[320px_minmax(0,1fr)]">
        {/* LEFT: Sticky Product Catalog */}
        <aside
          className="
    rounded-lg
    border border-gray-200
    bg-white
    p-4
    shadow-sm

    lg:sticky
    lg:top-[140px]
    lg:flex
    lg:h-[calc(100dvh-250px)]
    lg:flex-col
    lg:overflow-hidden
  "
        >
          {/* Fixed catalog header */}
          <div className="shrink-0">
            <div className="mb-3 flex items-center justify-between gap-3">
              <div>
                <PageBreadcrumb pageTitle="Product catalog" align="left" textSize="xl" />
              </div>

              <button
                type="button"
                onClick={() => fetchProducts(searchTerm)}
                className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-gray-200 text-gray-500 transition hover:bg-gray-50"
                aria-label="Refresh products"
                title="Refresh products"
              >
                <RefreshCw
                  className={`h-4 w-4 ${productsLoading ? "animate-spin" : ""
                    }`}
                />
              </button>
            </div>

            <div className="relative mb-3">
              <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />

              <input
                value={searchTerm}
                onChange={(event) => setSearchTerm(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === "Enter") {
                    fetchProducts(searchTerm);
                  }
                }}
                placeholder="Search ASIN, SKU, title"
                className="h-9 w-full rounded-md border border-gray-300 bg-white pl-9 pr-3 text-sm outline-none focus:border-[#5EA68E] focus:ring-2 focus:ring-[#5EA68E]/20"
              />
            </div>
          </div>

          {/* Only this section scrolls */}
          <div className="min-h-0 flex-1 space-y-2 overflow-y-auto pr-1">
            {orderedProducts.length ? (
              orderedProducts.map((product) => (
                <button
                  key={`${product.asin}-${product.sku || ""}`}
                  type="button"
                  onClick={() => {
                    setAsin(product.asin);
                    fetchFeedback(product.asin);
                  }}
                  className={`w-full rounded-md border p-3 text-left transition ${asin === product.asin
                    ? "border-[#5EA68E] bg-[#5EA68E]/10"
                    : "border-gray-200 bg-white hover:bg-gray-50"
                    }`}
                >
                  <div className="flex items-center justify-between gap-3">
                    <div className="min-w-0">
                      <div className="line-clamp-2 text-sm font-semibold text-charcoal-500">
                        {productName(product) || "Amazon product"}
                      </div>
                    </div>

                    <ProductThumb
                      imageUrl={product.main_image_url}
                      asin={product.asin}
                      alt={productName(product) || product.asin}
                      size="small"
                    />
                  </div>
                </button>
              ))
            ) : (
              <div className="rounded-md bg-gray-50 p-3 text-sm text-gray-500">
                No synced ASINs found. Enter a child ASIN manually and fetch from
                Amazon.
              </div>
            )}
          </div>
        </aside>

        {/* RIGHT: normal page scrolling */}
        <main className="flex min-w-0 flex-col gap-4">
          <section className="relative rounded-lg border border-gray-200 bg-white p-4 shadow-sm">


            <div className="flex flex-col gap-4 sm:flex-row sm:items-center">
              <ProductThumb
                imageUrl={selectedProduct?.main_image_url}
                asin={selectedAsin}
                alt={selectedProductName || selectedAsin || "Product image"}
              />

              {/* <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="rounded-md bg-gray-100 px-2 py-1 text-xs font-semibold text-gray-700">
                    ASIN: {selectedAsin || "NA"}
                  </span>

                  <span className="rounded-md bg-[#5EA68E]/10 px-2 py-1 text-xs font-semibold text-[#3e806b]">
                    SKU: {selectedSku || "NA"}
                  </span>

                  {selectedBrowseNode && (
                    <span className="rounded-md bg-blue-50 px-2 py-1 text-xs font-semibold text-blue-700">
                      {selectedBrowseNode}
                    </span>
                  )}
                </div>

                <h2 className="mt-2 line-clamp-2 text-lg font-semibold text-gray-900">
                  {selectedProductName ||
                    "Fetch an ASIN to view Amazon review insights"}
                </h2>
              </div> */}

              <div className="min-w-0 flex-1">
                <div className="flex items-center justify-between gap-4">
                  <div className="min-w-0 flex-1">
                    <PageBreadcrumb
                      pageTitle={
                        selectedProductName ||
                        "Fetch an ASIN to view Amazon review insights"
                      }
                      align="left"
                      textSize="2xl"
                    />

                    <div className="mt-1 flex flex-wrap items-center gap-4 text-xs text-gray-500">
                      {reviewStartDate && reviewEndDate && (
                        <span>
                          Reviews available:{" "}
                          <span className="font-medium text-gray-700">
                            {formatDate(reviewStartDate)} - {formatDate(reviewEndDate)}
                          </span>
                        </span>
                      )}
                    </div>
                  </div>

                  <div className="flex shrink-0 flex-wrap items-center justify-end gap-2">
                    <span className="rounded-md bg-gray-100 px-2 py-1 text-xs font-semibold text-gray-700">
                      ASIN: {selectedAsin || "NA"}
                    </span>

                    <span className="rounded-md bg-[#5EA68E]/10 px-2 py-1 text-xs font-semibold text-[#3e806b]">
                      SKU: {selectedSku || "NA"}
                    </span>

                    {selectedBrowseNode && (
                      <span className="rounded-md bg-blue-50 px-2 py-1 text-xs font-semibold text-blue-700">
                        {selectedBrowseNode}
                      </span>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </section>

          <SentimentTrendOverview
            positiveTopics={orderedPositiveTopics}
            negativeTopics={orderedNegativeTopics}
            positiveReviewCount={positiveReviewCount}
            negativeReviewCount={negativeReviewCount}
            ratingImpact={ratingImpact}
          // loading={loading}
          />

          <section className="grid gap-4 xl:grid-cols-2">
            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between">
                <PageBreadcrumb pageTitle="Positive Reviews" align="left" textSize="lg" />
              </div>

              {orderedPositiveTopics.length ? (
                orderedPositiveTopics.map((topic, index) => (
                  <TopicCard
                    key={`${topic.topic}-${index}`}
                    topic={topic}
                    sentiment="positive"
                    impactTopics={positiveImpactTopics}
                    defaultOpen={index === 0}
                  />
                ))
              ) : (
                <div className="rounded-lg border border-gray-200 bg-white p-4 text-sm text-gray-500 shadow-sm">
                  No positive review topics available.
                </div>
              )}
            </div>

            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between">
                <PageBreadcrumb pageTitle="Negative Reviews" align="left" textSize="lg" />
              </div>

              {orderedNegativeTopics.length ? (
                orderedNegativeTopics.map((topic, index) => (
                  <TopicCard
                    key={`${topic.topic}-${index}`}
                    topic={topic}
                    sentiment="negative"
                    impactTopics={negativeImpactTopics}
                    defaultOpen={index === 0}
                  />
                ))
              ) : (
                <div className="rounded-lg border border-gray-200 bg-white p-4 text-sm text-gray-500 shadow-sm">
                  No negative review topics available.
                </div>
              )}
            </div>
          </section>
        </main>
      </div>

    </div>
  );
}
