"use client";

import React, { useEffect, useState } from "react";
import NextImage from "next/image";

export interface ProductSearchProduct {
  product_name: string;
  sku?: string | null;
  sku_us?: string | null;
  sku_uk?: string | null;
  sku_canada?: string | null;
  asin?: string | null;
  title?: string | null;
  brand?: string | null;
  marketplace_id?: string | null;
  main_image_url?: string | null;
}

interface ProductSearchDropdownProps {
  authToken?: string | null;
  countryName?: string;
  range?: string;
  selectedMonth?: string;
  selectedQuarter?: string;
  selectedYear?: string | number | "";
  homeCurrency?: string;
  dedupeBy?: "product" | "sku";
  onProductSelect: (
    productName: string,
    product?: ProductSearchProduct
  ) => void;
}

const normalizeProductNameKey = (value?: string | null) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/\s+/g, " ");

const isAggregateProductName = (value?: string | null) => {
  const key = normalizeProductNameKey(value);
  return ["total", "other", "others", "other sku", "other skus"].includes(key);
};

const productOptionScore = (product: ProductSearchProduct) => {
  let score = 0;
  if (product.main_image_url) score += 8;
  if (product.asin) score += 4;
  if (product.sku) score += 2;
  if (product.title) score += 1;
  return score;
};

const dedupeProductsByName = (
  products: ProductSearchProduct[]
): ProductSearchProduct[] => {
  const byName = new Map<string, ProductSearchProduct>();

  products.forEach((product) => {
    const productName = product.product_name.trim();
    if (!productName || isAggregateProductName(productName)) return;

    const key = normalizeProductNameKey(productName);
    const current = byName.get(key);

    if (!current) {
      byName.set(key, { ...product, product_name: productName });
      return;
    }

    const merged: ProductSearchProduct = {
      ...current,
      asin: current.asin || product.asin,
      sku: current.sku || product.sku,
      sku_us: current.sku_us || product.sku_us,
      sku_uk: current.sku_uk || product.sku_uk,
      sku_canada: current.sku_canada || product.sku_canada,
      title: current.title || product.title,
      brand: current.brand || product.brand,
      marketplace_id: current.marketplace_id || product.marketplace_id,
      main_image_url: current.main_image_url || product.main_image_url,
    };

    byName.set(
      key,
      productOptionScore(product) > productOptionScore(merged)
        ? { ...merged, ...product, product_name: productName }
        : merged
    );
  });

  return Array.from(byName.values());
};

const normalizeSkuKey = (product: ProductSearchProduct) =>
  normalizeProductNameKey(
    product.sku || product.sku_us || product.sku_uk || product.sku_canada
  );

const dedupeProductsBySku = (
  products: ProductSearchProduct[]
): ProductSearchProduct[] => {
  const bySku = new Map<string, ProductSearchProduct>();

  products.forEach((product) => {
    const productName = product.product_name.trim();
    if (!productName || isAggregateProductName(productName)) return;

    const skuKey = normalizeSkuKey(product);
    const key = skuKey
      ? `sku:${skuKey}`
      : `name:${normalizeProductNameKey(productName)}`;

    const current = bySku.get(key);

    if (!current) {
      bySku.set(key, { ...product, product_name: productName });
      return;
    }

    const merged: ProductSearchProduct = {
      ...current,
      asin: current.asin || product.asin,
      sku: current.sku || product.sku,
      sku_us: current.sku_us || product.sku_us,
      sku_uk: current.sku_uk || product.sku_uk,
      sku_canada: current.sku_canada || product.sku_canada,
      title: current.title || product.title,
      brand: current.brand || product.brand,
      marketplace_id: current.marketplace_id || product.marketplace_id,
      main_image_url: current.main_image_url || product.main_image_url,
    };

    bySku.set(
      key,
      productOptionScore(product) > productOptionScore(merged)
        ? { ...merged, ...product, product_name: productName }
        : merged
    );
  });

  return Array.from(bySku.values());
};

// helper: accept array of strings OR array of objects
const normalizeProducts = (
  raw: any,
  dedupeBy: "product" | "sku" = "product"
): ProductSearchProduct[] => {
  if (!Array.isArray(raw)) return [];

  if (raw.length === 0) return [];

  const dedupe = dedupeBy === "sku" ? dedupeProductsBySku : dedupeProductsByName;

  // case 1: ["A", "B", ...]
  if (typeof raw[0] === "string") {
    return dedupe(
      raw.map((name) => ({ product_name: name as string }))
    );
  }

  // case 2: [{product_name: "A"}, ...]
  if (typeof raw[0] === "object" && raw[0] !== null) {
    const products = raw
      .map((item) => ({
        product_name:
          typeof item.product_name === "string"
            ? item.product_name
            : typeof item.title === "string"
              ? item.title
              : "",
        sku: item.sku ?? null,
        sku_us: item.sku_us ?? null,
        sku_uk: item.sku_uk ?? null,
        sku_canada: item.sku_canada ?? null,
        asin: item.asin ?? null,
        title: item.title ?? null,
        brand: item.brand ?? null,
        marketplace_id: item.marketplace_id ?? null,
        main_image_url: item.main_image_url ?? null,
      }))
      .filter((item) => item.product_name.trim().length > 0);

    return dedupe(products);
  }

  return [];
};

function ProductSearchThumb({ product }: { product: ProductSearchProduct }) {
  const [failed, setFailed] = useState(false);
  const imageUrl = String(product.main_image_url || "").trim();
  const label = product.product_name.trim().slice(0, 1).toUpperCase() || "P";

  if (imageUrl && !failed) {
    return (
      <NextImage
        unoptimized
        src={imageUrl}
        alt={product.product_name}
        width={40}
        height={40}
        onError={() => setFailed(true)}
        className="h-10 w-10 shrink-0 rounded-md border border-gray-200 bg-white object-contain"
      />
    );
  }

  return (
    <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-md border border-gray-200 bg-gray-50 text-xs font-bold text-[#5EA68E]">
      {label}
    </div>
  );
}

const ProductSearchDropdown: React.FC<ProductSearchDropdownProps> = ({
  authToken,
  countryName,
  range,
  selectedMonth,
  selectedQuarter,
  selectedYear,
  homeCurrency,
  dedupeBy = "product",
  onProductSelect,
}) => {
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<ProductSearchProduct[]>([]);
  const [allProducts, setAllProducts] = useState<ProductSearchProduct[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [allLoading, setAllLoading] = useState(false);
  const [hasLoadedAll, setHasLoadedAll] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  const hasSearch = searchQuery.trim().length > 0;
  const displayedProducts = hasSearch ? searchResults : allProducts;

  useEffect(() => {
    setAllProducts([]);
    setSearchResults([]);
    setHasLoadedAll(false);
  }, [countryName, range, selectedMonth, selectedQuarter, selectedYear, homeCurrency, dedupeBy]);

  const appendPeriodParams = (params: URLSearchParams) => {
    if (countryName) params.set("country", countryName);
    if (homeCurrency) params.set("homeCurrency", homeCurrency);

    if (!range || !selectedYear) return;
    if (!["monthly", "quarterly", "yearly"].includes(range)) return;

    params.set("range", range);
    params.set("year", String(selectedYear));

    if (range === "monthly" && selectedMonth) {
      params.set("month", selectedMonth);
    }

    if (range === "quarterly" && selectedQuarter) {
      params.set("quarter", selectedQuarter);
    }
  };

  // -------- Fetch ALL products (for dropdown) --------
  const fetchAllProducts = async () => {
    if (hasLoadedAll) return;
    try {
      setAllLoading(true);
      setLoadError(null);

      const params = new URLSearchParams();
      appendPeriodParams(params);

      const query = params.toString();
      const res = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/Product_names${query ? `?${query}` : ""}`, {
        method: "GET",
        headers: {
          Authorization: `Bearer ${authToken ?? ""}`,
          "Content-Type": "application/json",
        },
      });

      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);

      const json = await res.json();

      // tries multiple keys in case backend uses a different name
      const list = normalizeProducts(
        json.product_names ?? json.products ?? json.product_list,
        dedupeBy
      );

      setAllProducts(list);
      setHasLoadedAll(true);
    } catch (err: any) {
      console.error("Error fetching all products:", err);
      setAllProducts([]);
      setLoadError(err.message || "Failed to load products");
    } finally {
      setAllLoading(false);
    }
  };

  // -------- Debounced SEARCH behaviour --------
  useEffect(() => {
    const timeoutId = setTimeout(async () => {
      if (!searchQuery.trim()) {
        setSearchResults([]);
        return;
      }
      setSearchLoading(true);
      try {
        const params = new URLSearchParams();
        params.set("query", searchQuery);
        appendPeriodParams(params);

        const res = await fetch(
          `${process.env.NEXT_PUBLIC_API_BASE_URL}/Product_search?${params.toString()}`,
          {
            method: "GET",
            headers: {
              Authorization: `Bearer ${authToken ?? ""}`,
              "Content-Type": "application/json",
            },
          }
        );
        if (!res.ok) {
          throw new Error(`HTTP error! status: ${res.status}`);
        }
        const json = await res.json();

        const list = normalizeProducts(
          json.product_names ?? json.products ?? json.product_list,
          dedupeBy
        );
        setSearchResults(list);
      } catch (e: any) {
        console.error("Search error:", e);
        setSearchResults([]);
      } finally {
        setSearchLoading(false);
      }
    }, 300);

    return () => clearTimeout(timeoutId);
  }, [searchQuery, authToken, countryName, range, selectedMonth, selectedQuarter, selectedYear, homeCurrency, dedupeBy]);

  const handleToggleDropdown = async () => {
    const next = !showDropdown;
    setShowDropdown(next);
    if (next && !hasLoadedAll) {
      await fetchAllProducts();   // ✅ still works if user clicks the arrow
    }
  };


  const handleSelect = (p: ProductSearchProduct) => {
    onProductSelect(p.product_name, p);
    setShowDropdown(false);
    setSearchQuery("");
  };

  return (
    <div className="relative w-full">
      <div className="relative flex items-center">
        <input
          type="text"
          placeholder="Search products"
          className="
  w-full
  rounded-lg
  border border-[#C4C4C4]
  bg-[#FBFBFB]
  shadow-sm
  outline-none
  focus:border-[#C4C4C4]
  focus:ring-0

  text-xs sm:text-sm
  px-3 sm:px-4
  py-1.5 sm:py-2
  pl-8 sm:pl-9
  text-charcoal-500
"

          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          onFocus={() => {
            setShowDropdown(true);
            if (!hasLoadedAll) {
              fetchAllProducts(); 
            }
          }}
          onBlur={() => setTimeout(() => setShowDropdown(false), 200)}
        />


        {/* Search icon */}
        <svg
          className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-[#C2C2C2]"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
        >
          <circle cx="11" cy="11" r="8" />
          <line x1="21" y1="21" x2="16.65" y2="16.65" />
        </svg>

        {/* Dropdown toggle */}
        <button
          type="button"
          className="absolute right-3 top-1/2 -translate-y-1/2 flex h-4 w-4 items-center justify-center text-[10px] text-[#B0B0B0]"
          onMouseDown={(e) => e.preventDefault()}
          onClick={handleToggleDropdown}
          aria-label="Toggle products dropdown"
        >
          <svg
            xmlns="http://www.w3.org/2000/svg"
            viewBox="0 0 24 24"
            fill="none"
            stroke="currentColor"
            strokeWidth="2"
            className={`h-3 w-3 transition-transform ${showDropdown ? "rotate-180" : ""
              }`}
          >
            <polyline points="6 9 12 15 18 9" />
          </svg>
        </button>

        {(searchLoading || allLoading) && (
          <span className="absolute right-8 top-1/2 h-3 w-3 -translate-y-1/2 animate-spin rounded-full border-2 border-gray-200 border-t-gray-400" />
        )}
      </div>
      {showDropdown && (
        <div className="absolute left-0 right-0 z-50 mt-1 max-h-72 overflow-y-auto rounded-xl border border-gray-200 bg-white shadow-md">
          {allLoading ? (
            <div className="p-3 text-center text-gray-500 text-xs">
              Loading products...
            </div>
          ) : displayedProducts.length > 0 ? (
            displayedProducts.map((p, i) => (
              <button
                key={`${p.product_name}-${i}`}
                className="w-full cursor-pointer px-4 py-2.5 text-left hover:bg-gray-50 text-sm"
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => handleSelect(p)}
              >
                <div className="flex items-center gap-3">
                  <ProductSearchThumb product={p} />
                  <div className="min-w-0">
                    <div className="truncate text-xs text-gray-800 sm:text-sm">
                      {p.product_name}
                    </div>
                    {p.sku ? (
                      <div className="mt-0.5 truncate text-[11px] font-medium text-gray-500">
                        SKU: {p.sku}
                      </div>
                    ) : null}
                  </div>
                </div>
              </button>
            ))
          ) : (
            <div className="p-3 text-center text-gray-500 text-xs">
              {hasSearch
                ? `No products found for "${searchQuery}"`
                : "No products available"}
            </div>
          )}
        </div>
      )}

    </div>
  );
};

export default ProductSearchDropdown;
