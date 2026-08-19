"use client";

import React, { useEffect, useMemo, useState } from "react";
import {
  AlertCircle,
  BarChart3,
  Boxes,
  ImageIcon,
  PackageSearch,
  RefreshCw,
  Search,
  Sparkles,
  Star,
  Tag,
} from "lucide-react";
import { useParams } from "next/navigation";
import { API_BASE } from "@/config/env";
import PageBreadcrumb from "@/components/common/PageBreadCrumb";

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
  trends?: {
    trends?: {
      positiveTrends?: TrendTopic[];
      negativeTrends?: TrendTopic[];
      positiveTopics?: TrendTopic[];
      negativeTopics?: TrendTopic[];
    };
  } | null;
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

function productName(product?: ProductOption | null, fallback?: string | null) {
  return product?.product_name || product?.title || fallback || "";
}

function skuLabel(product?: ProductOption | null) {
  return product?.sku || product?.sku_us || product?.sku_uk || product?.sku_canada || "";
}

function getTopicImpact(topic: Topic) {
  const value = topic.asinMetrics?.starRatingImpact;
  return typeof value === "number" && !Number.isNaN(value) ? value : null;
}

function getRatingImpactSummary(topics: Topic[]) {
  const impacts = topics
    .map(getTopicImpact)
    .filter((value): value is number => typeof value === "number");

  if (!impacts.length) return null;

  return impacts.reduce((best, current) =>
    Math.abs(current) > Math.abs(best) ? current : best
  );
}

function findImpactForTopic(topic: Topic, impactTopics: Topic[]) {
  const topicName = String(topic.topic || "").trim().toLowerCase();
  if (!topicName) return getTopicImpact(topic);

  const match = impactTopics.find(
    (candidate) => String(candidate.topic || "").trim().toLowerCase() === topicName
  );

  return getTopicImpact(match || topic);
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
            className={`${iconSize} ${
              filled ? "fill-amber-400 text-amber-400" : "text-gray-300"
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
  const currentSource = candidates[sourceIndex];

  useEffect(() => {
    setSourceIndex(0);
  }, [candidates.join("|")]);

  if (currentSource) {
    return (
      <img
        src={currentSource}
        alt={alt}
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
  const trends = response?.trends?.trends as any;
  if (!trends) return [];

  if (sentiment === "positive") {
    return trends.positiveTrends || trends.positiveTopics || [];
  }

  return trends.negativeTrends || trends.negativeTopics || [];
}

function latestTrendValue(topic: TrendTopic) {
  const metrics = topic.trendMetrics || [];
  const latest = metrics[metrics.length - 1];
  return (
    latest?.asinMetrics?.occurrencePercentage ??
    latest?.asinMetrics?.starRatingImpact ??
    latest?.browseNodeMetrics?.occurrencePercentage?.allProducts ??
    latest?.browseNodeMetrics?.occurrencePercentage?.topTwentyFivePercentProducts ??
    0
  );
}

function TopicCard({
  topic,
  sentiment,
  impactTopics,
}: {
  topic: Topic;
  sentiment: "positive" | "negative";
  impactTopics?: Topic[];
}) {
  const impact = findImpactForTopic(topic, impactTopics || []);
  const hasImpact = impact !== null;
  const accent =
    sentiment === "positive"
      ? "border-emerald-200 bg-emerald-50/70 text-emerald-700"
      : "border-rose-200 bg-rose-50/70 text-rose-700";

  return (
    <article className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div>
          <div className={`inline-flex rounded-md border px-2 py-1 text-xs font-semibold ${accent}`}>
            {sentiment === "positive" ? "Positive" : "Negative"}
          </div>
          <h3 className="mt-2 text-base font-semibold text-gray-900">
            {topic.topic || "Untitled topic"}
          </h3>
        </div>
        <div
          className={`grid ${
            hasImpact ? "grid-cols-3" : "grid-cols-2"
          } gap-2 text-right text-xs text-gray-500`}
        >
          <div>
            <div className="font-semibold text-gray-900">
              {metricValue(topic.asinMetrics?.numberOfMentions)}
            </div>
            <div>Mentions</div>
          </div>
          <div>
            <div className="font-semibold text-gray-900">
              {metricValue(topic.asinMetrics?.occurrencePercentage, "%")}
            </div>
            <div>Share</div>
          </div>
          {hasImpact && (
            <div>
              <div className="font-semibold text-gray-900">
                {metricValue(impact)}
              </div>
              <div>Impact</div>
            </div>
          )}
        </div>
      </div>

      {hasImpact && (
        <div className="mt-3 flex items-center gap-2 rounded-md bg-amber-50 px-3 py-2 text-xs text-amber-900">
          <RatingStars value={impact} size="xs" />
          <span>{`Rating impact ${metricValue(impact)}`}</span>
        </div>
      )}

      {!!topic.reviewSnippets?.length && (
        <div className="mt-3 space-y-2">
          {topic.reviewSnippets.slice(0, 3).map((snippet, index) => (
            <p key={`${snippet}-${index}`} className="rounded-md bg-gray-50 px-3 py-2 text-sm text-gray-700">
              {snippet}
            </p>
          ))}
        </div>
      )}

      {!!topic.subtopics?.length && (
        <div className="mt-3 flex flex-wrap gap-2">
          {topic.subtopics.slice(0, 4).map((subtopic, index) => (
            <span
              key={`${subtopic.subtopic}-${index}`}
              className="rounded-md border border-gray-200 bg-white px-2 py-1 text-xs text-gray-600"
            >
              {subtopic.subtopic || "Subtopic"} - {metricValue(subtopic.metrics?.numberOfMentions)} mentions
            </span>
          ))}
        </div>
      )}
    </article>
  );
}

function TrendBlock({
  title,
  topics,
  tone,
}: {
  title: string;
  topics: TrendTopic[];
  tone: "green" | "rose";
}) {
  const color = tone === "green" ? "bg-emerald-500" : "bg-rose-500";
  const maxValue = Math.max(1, ...topics.map(latestTrendValue));

  return (
    <section className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
      <div className="mb-4 flex items-center gap-2">
        <BarChart3 className="h-4 w-4 text-gray-500" />
        <h3 className="text-sm font-semibold text-gray-900">{title}</h3>
      </div>
      {topics.length ? (
        <div className="space-y-3">
          {topics.slice(0, 6).map((topic, index) => {
            const value = latestTrendValue(topic);
            return (
              <div key={`${topic.topic}-${index}`}>
                <div className="mb-1 flex items-center justify-between gap-3 text-xs">
                  <span className="truncate font-medium text-gray-700">{topic.topic || "Topic"}</span>
                  <span className="shrink-0 text-gray-500">{metricValue(value, "%")}</span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-gray-100">
                  <div
                    className={`h-full rounded-full ${color}`}
                    style={{ width: `${Math.min(100, Math.max(4, (value / maxValue) * 100))}%` }}
                  />
                </div>
              </div>
            );
          })}
        </div>
      ) : (
        <p className="text-sm text-gray-500">No trend data returned for this ASIN.</p>
      )}
    </section>
  );
}

export default function CustomerReviewsPage() {
  const routeParams = useParams<{
    countryName?: string;
    month?: string;
    year?: string;
  }>();
  const countryName = String(routeParams.countryName || "us").toLowerCase();
  const marketplaceId = MARKETPLACE_BY_COUNTRY[countryName] || MARKETPLACE_BY_COUNTRY.us;
  const marketplaceLabel = MARKETPLACE_LABELS[countryName] || "Amazon";

  const [products, setProducts] = useState<ProductOption[]>([]);
  const [productsLoading, setProductsLoading] = useState(false);
  const [searchTerm, setSearchTerm] = useState("");
  const [asin, setAsin] = useState("");
  const [feedback, setFeedback] = useState<FeedbackResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const positiveTopics = feedback?.topics?.topics?.positiveTopics || [];
  const negativeTopics = feedback?.topics?.topics?.negativeTopics || [];
  const positiveImpactTopics = feedback?.rating_impact_topics?.topics?.positiveTopics || [];
  const negativeImpactTopics = feedback?.rating_impact_topics?.topics?.negativeTopics || [];
  const allTopics = useMemo(
    () => [
      ...(positiveImpactTopics.length ? positiveImpactTopics : positiveTopics),
      ...(negativeImpactTopics.length ? negativeImpactTopics : negativeTopics),
    ],
    [negativeImpactTopics, negativeTopics, positiveImpactTopics, positiveTopics]
  );
  const positiveTrends = useMemo(() => pickTrendTopics(feedback, "positive"), [feedback]);
  const negativeTrends = useMemo(() => pickTrendTopics(feedback, "negative"), [feedback]);

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
  const showRatingImpact = ratingImpact !== null;
  const summaryGridClass = showRatingImpact
    ? "md:grid-cols-2 xl:grid-cols-4"
    : "md:grid-cols-3";

  const fetchProducts = async (query = "") => {
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
      if (!asin && json.products?.[0]?.asin) {
        setAsin(json.products[0].asin);
      }
    } catch (err) {
      setProducts([]);
      setError(err instanceof Error ? err.message : "Failed to load products");
    } finally {
      setProductsLoading(false);
    }
  };

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
  }, [marketplaceId]);

  return (
    <div className="mx-auto flex max-w-7xl flex-col gap-4">
      <div className="rounded-lg border border-gray-200 bg-white shadow-sm">
        <div className="flex flex-col gap-4 border-b border-gray-100 p-4 lg:flex-row lg:items-center lg:justify-between">
        <div>
          <PageBreadcrumb pageTitle="Customer Reviews" align="left" />
          <p className="mt-1 text-sm text-gray-500">
            {marketplaceLabel} - Customer Feedback insights from Amazon SP-API
          </p>
        </div>

        <div className="flex flex-col gap-2 sm:flex-row">
          <div className="relative">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
            <input
              value={asin}
              onChange={(event) => setAsin(event.target.value.toUpperCase())}
              placeholder="Child ASIN"
              className="h-10 w-full rounded-md border border-gray-300 bg-white pl-9 pr-3 text-sm outline-none focus:border-[#5EA68E] focus:ring-2 focus:ring-[#5EA68E]/20 sm:w-44"
            />
          </div>
          <button
            type="button"
            onClick={() => fetchFeedback()}
            disabled={loading}
            className="inline-flex h-10 items-center justify-center gap-2 rounded-md bg-[#5EA68E] px-4 text-sm font-semibold text-white shadow-sm transition hover:bg-[#4d927c] disabled:cursor-not-allowed disabled:opacity-60"
          >
            {loading ? <RefreshCw className="h-4 w-4 animate-spin" /> : <Sparkles className="h-4 w-4" />}
            Fetch
          </button>
        </div>
      </div>

        <div className={`grid gap-3 p-4 ${summaryGridClass}`}>
          <div className="rounded-lg border border-gray-200 bg-gray-50 px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-xs font-semibold uppercase text-gray-500">
              <PackageSearch className="h-4 w-4" />
              Product
            </div>
            <div className="line-clamp-2 min-h-10 text-sm font-semibold text-gray-900">
              {selectedProductName || "Select or enter a child ASIN"}
            </div>
          </div>
          <div className="rounded-lg border border-gray-200 bg-gray-50 px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-xs font-semibold uppercase text-gray-500">
              <Boxes className="h-4 w-4" />
              SKU
            </div>
            <div className="truncate text-sm font-semibold text-gray-900">
              {selectedSku || "SKU will appear after matching catalog data"}
            </div>
          </div>
          <div className="rounded-lg border border-gray-200 bg-gray-50 px-4 py-3">
            <div className="mb-2 flex items-center gap-2 text-xs font-semibold uppercase text-gray-500">
              <Tag className="h-4 w-4" />
              ASIN
            </div>
            <div className="truncate text-sm font-semibold text-gray-900">
              {selectedAsin || "No ASIN selected"}
            </div>
          </div>
          {showRatingImpact && (
            <div className="rounded-lg border border-gray-200 bg-gray-50 px-4 py-3">
              <div className="mb-2 flex items-center gap-2 text-xs font-semibold uppercase text-gray-500">
                <Star className="h-4 w-4" />
                Rating Impact
              </div>
              <div className="flex items-center gap-2">
                <RatingStars value={ratingImpact} />
                <span className="text-sm font-semibold text-gray-900">
                  {metricValue(ratingImpact)}
                </span>
              </div>
            </div>
          )}
        </div>
      </div>

      {error && (
        <div className="flex items-start gap-2 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
          <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
          <span>{error}</span>
        </div>
      )}

      <div className="grid gap-4 lg:grid-cols-[320px_1fr]">
        <aside className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
          <div className="mb-3 flex items-center justify-between gap-3">
            <div>
              <h2 className="text-sm font-semibold text-gray-900">Product catalog</h2>
              <p className="text-xs text-gray-500">{products.length} ASINs with product mapping</p>
            </div>
            <button
              type="button"
              onClick={() => fetchProducts(searchTerm)}
              className="inline-flex h-8 w-8 items-center justify-center rounded-md border border-gray-200 text-gray-500 transition hover:bg-gray-50"
              aria-label="Refresh products"
              title="Refresh products"
            >
              <RefreshCw className={`h-4 w-4 ${productsLoading ? "animate-spin" : ""}`} />
            </button>
          </div>

          <div className="relative mb-3">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-gray-400" />
            <input
              value={searchTerm}
              onChange={(event) => setSearchTerm(event.target.value)}
              onKeyDown={(event) => {
                if (event.key === "Enter") fetchProducts(searchTerm);
              }}
              placeholder="Search ASIN, SKU, title"
              className="h-9 w-full rounded-md border border-gray-300 bg-white pl-9 pr-3 text-sm outline-none focus:border-[#5EA68E] focus:ring-2 focus:ring-[#5EA68E]/20"
            />
          </div>

          <div className="max-h-[calc(100vh-310px)] space-y-2 overflow-y-auto pr-1">
            {productsLoading ? (
              <div className="rounded-md bg-gray-50 p-3 text-sm text-gray-500">Loading products...</div>
            ) : products.length ? (
              products.map((product) => (
                <button
                  key={`${product.asin}-${product.sku || ""}`}
                  type="button"
                  onClick={() => {
                    setAsin(product.asin);
                    fetchFeedback(product.asin);
                  }}
                  className={`w-full rounded-md border p-3 text-left transition ${
                    asin === product.asin
                      ? "border-[#5EA68E] bg-[#5EA68E]/10"
                      : "border-gray-200 bg-white hover:bg-gray-50"
                  }`}
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <div className="line-clamp-2 text-sm font-semibold text-gray-900">
                        {productName(product) || "Amazon product"}
                      </div>
                      <div className="mt-2 flex flex-wrap gap-1.5">
                        <span className="rounded bg-gray-100 px-1.5 py-0.5 text-[11px] font-semibold text-gray-700">
                          {product.asin}
                        </span>
                        {skuLabel(product) && (
                          <span className="rounded bg-[#5EA68E]/10 px-1.5 py-0.5 text-[11px] font-semibold text-[#3e806b]">
                            {skuLabel(product)}
                          </span>
                        )}
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
                No synced ASINs found. Enter a child ASIN manually and fetch from Amazon.
              </div>
            )}
          </div>
        </aside>

        <main className="flex min-w-0 flex-col gap-4">
          <section className="rounded-lg border border-gray-200 bg-white p-4 shadow-sm">
            <div className="flex flex-col gap-4 sm:flex-row sm:items-center">
              <ProductThumb
                imageUrl={selectedProduct?.main_image_url}
                asin={selectedAsin}
                alt={selectedProductName || selectedAsin || "Product image"}
              />
              <div className="min-w-0 flex-1">
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
                  {selectedProductName || "Fetch an ASIN to view Amazon review insights"}
                </h2>
                <div className="mt-2 grid gap-2 text-sm text-gray-500 sm:grid-cols-3">
                  <div>
                    <span className="font-medium text-gray-700">Data range:</span>{" "}
                    {formatDate(feedback?.topics?.dateRange?.startDate)} to{" "}
                    {formatDate(feedback?.topics?.dateRange?.endDate)}
                  </div>
                  <div>
                    <span className="font-medium text-gray-700">Barcode:</span>{" "}
                    {selectedProduct?.product_barcode || "NA"}
                  </div>
                  <div>
                    <span className="font-medium text-gray-700">Price:</span>{" "}
                    {selectedProduct?.price ? `${selectedProduct.currency || ""} ${selectedProduct.price}`.trim() : "NA"}
                  </div>
                </div>
              </div>
            </div>
          </section>

          <div className="grid gap-4 xl:grid-cols-2">
            <TrendBlock title="Positive review trends" topics={positiveTrends} tone="green" />
            <TrendBlock title="Negative review trends" topics={negativeTrends} tone="rose" />
          </div>

          <section className="grid gap-4 xl:grid-cols-2">
            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between">
                <h2 className="text-sm font-semibold text-gray-900">Positive topics</h2>
                <span className="text-xs text-gray-500">{positiveTopics.length} returned</span>
              </div>
              {positiveTopics.length ? (
                positiveTopics.map((topic, index) => (
                  <TopicCard
                    key={`${topic.topic}-${index}`}
                    topic={topic}
                    sentiment="positive"
                    impactTopics={positiveImpactTopics}
                  />
                ))
              ) : (
                <div className="rounded-lg border border-gray-200 bg-white p-4 text-sm text-gray-500 shadow-sm">
                  Fetch a child ASIN to see positive topics.
                </div>
              )}
            </div>

            <div className="flex flex-col gap-3">
              <div className="flex items-center justify-between">
                <h2 className="text-sm font-semibold text-gray-900">Negative topics</h2>
                <span className="text-xs text-gray-500">{negativeTopics.length} returned</span>
              </div>
              {negativeTopics.length ? (
                negativeTopics.map((topic, index) => (
                  <TopicCard
                    key={`${topic.topic}-${index}`}
                    topic={topic}
                    sentiment="negative"
                    impactTopics={negativeImpactTopics}
                  />
                ))
              ) : (
                <div className="rounded-lg border border-gray-200 bg-white p-4 text-sm text-gray-500 shadow-sm">
                  Fetch a child ASIN to see negative topics.
                </div>
              )}
            </div>
          </section>
        </main>
      </div>
    </div>
  );
}
