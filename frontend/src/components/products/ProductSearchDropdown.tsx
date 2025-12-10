// "use client";

// import React, { useEffect, useState } from "react";

// interface Product {
//     product_name: string;
// }

// interface ProductSearchDropdownProps {
//     authToken?: string | null;
//     onProductSelect: (productName: string) => void;
// }

// const ProductSearchDropdown: React.FC<ProductSearchDropdownProps> = ({
//     authToken,
//     onProductSelect,
// }) => {
//     const [searchQuery, setSearchQuery] = useState("");
//     const [searchResults, setSearchResults] = useState<Product[]>([]);
//     const [allProducts, setAllProducts] = useState<Product[]>([]);
//     const [showDropdown, setShowDropdown] = useState(false);
//     const [searchLoading, setSearchLoading] = useState(false);
//     const [allLoading, setAllLoading] = useState(false);
//     const [hasLoadedAll, setHasLoadedAll] = useState(false);

//     const hasSearch = searchQuery.trim().length > 0;

//     const displayedProducts = hasSearch ? searchResults : allProducts;

//     // -------- Fetch ALL products (for dropdown) --------
//     const fetchAllProducts = async () => {
//         if (hasLoadedAll) return;
//         try {
//             setAllLoading(true);
//             // TODO: change this endpoint to whatever returns ALL products
//             const res = await fetch("http://localhost:5000/Product_list", {
//                 method: "GET",
//                 headers: {
//                     Authorization: `Bearer ${authToken ?? ""}`,
//                     "Content-Type": "application/json",
//                 },
//             });
//             if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);
//             const json = await res.json();
//             setAllProducts(json.products || []);
//             setHasLoadedAll(true);
//         } catch (err) {
//             console.error("Error fetching all products:", err);
//             setAllProducts([]);
//         } finally {
//             setAllLoading(false);
//         }
//     };

//     // -------- Debounced SEARCH behaviour --------
//     useEffect(() => {
//         const timeoutId = setTimeout(async () => {
//             if (!searchQuery.trim()) {
//                 setSearchResults([]);
//                 return;
//             }
//             setSearchLoading(true);
//             try {
//                 const res = await fetch(
//                     `http://localhost:5000/Product_search?query=${encodeURIComponent(
//                         searchQuery
//                     )}`,
//                     {
//                         method: "GET",
//                         headers: {
//                             Authorization: `Bearer ${authToken ?? ""}`,
//                             "Content-Type": "application/json",
//                         },
//                     }
//                 );
//                 if (!res.ok) {
//                     throw new Error(`HTTP error! status: ${res.status}`);
//                 }
//                 const json = await res.json();
//                 setSearchResults(json.products || []);
//             } catch (e) {
//                 console.error("Search error:", e);
//                 setSearchResults([]);
//             } finally {
//                 setSearchLoading(false);
//             }
//         }, 300);

//         return () => clearTimeout(timeoutId);
//     }, [searchQuery, authToken]);

//     const handleToggleDropdown = async () => {
//         const next = !showDropdown;
//         setShowDropdown(next);
//         if (next && !hasLoadedAll) {
//             await fetchAllProducts();
//         }
//     };

//     const handleSelect = (p: Product) => {
//         onProductSelect(p.product_name);
//         setShowDropdown(false);
//         setSearchQuery("");
//     };

// return (
//   <div className="relative w-full max-w-xs">
//     <div className="relative flex items-center">
//       <input
//         type="text"
//         placeholder="Search products, SKUs..."
//         className="
//           w-full
//           rounded-lg
//           border border-[#8B8585]
//           bg-[#FBFBFB]
//           px-4 py-2 pl-9
//           text-sm text-charcoal-500
//           outline-none
//           shadow-sm
//           focus:border-[#C4C4C4]
//           focus:ring-0
//         "
//         value={searchQuery}
//         onChange={(e) => setSearchQuery(e.target.value)}
//         onFocus={() => setShowDropdown(true)}
//         onBlur={() => setTimeout(() => setShowDropdown(false), 200)}
//       />

//       {/* Search icon */}
//       <svg
//         className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-[#C2C2C2]"
//         viewBox="0 0 24 24"
//         fill="none"
//         stroke="currentColor"
//         strokeWidth="2"
//       >
//         <circle cx="11" cy="11" r="8" />
//         <line x1="21" y1="21" x2="16.65" y2="16.65" />
//       </svg>

//       {/* Dropdown toggle (optional – keep logic, just make it subtle) */}
//       <button
//         type="button"
//         className="absolute right-3 top-1/2 -translate-y-1/2 flex h-4 w-4 items-center justify-center text-[10px] text-[#B0B0B0]"
//         onMouseDown={(e) => e.preventDefault()}
//         onClick={handleToggleDropdown}
//         aria-label="Toggle products dropdown"
//       >
//         <svg
//           xmlns="http://www.w3.org/2000/svg"
//           viewBox="0 0 24 24"
//           fill="none"
//           stroke="currentColor"
//           strokeWidth="2"
//           className={`h-3 w-3 transition-transform ${
//             showDropdown ? "rotate-180" : ""
//           }`}
//         >
//           <polyline points="6 9 12 15 18 9" />
//         </svg>
//       </button>

//       {(searchLoading || allLoading) && (
//         <span className="absolute right-8 top-1/2 h-3 w-3 -translate-y-1/2 animate-spin rounded-full border-2 border-gray-200 border-t-gray-400" />
//       )}
//     </div>

//     {showDropdown && (
//       <div className="absolute left-0 right-0 z-50 mt-1 max-h-72 overflow-y-auto rounded-xl border border-gray-200 bg-white shadow-md">
//         {displayedProducts.length > 0 ? (
//           displayedProducts.map((p, i) => (
//             <button
//               key={`${p.product_name}-${i}`}
//               className="w-full cursor-pointer px-4 py-2.5 text-left hover:bg-gray-50 text-sm"
//               onMouseDown={(e) => e.preventDefault()}
//               onClick={() => handleSelect(p)}
//             >
//               <div className="text-gray-800">{p.product_name}</div>
//             </button>
//           ))
//         ) : (
//           <div className="p-3 text-center text-gray-500 text-xs">
//             {hasSearch
//               ? `No products found for "${searchQuery}"`
//               : hasLoadedAll
//               ? "No products available"
//               : "Loading products..."}
//           </div>
//         )}
//       </div>
//     )}
//   </div>
// );

// };

// export default ProductSearchDropdown;














"use client";

import React, { useEffect, useState } from "react";

interface Product {
  product_name: string;
}

interface ProductSearchDropdownProps {
  authToken?: string | null;
  onProductSelect: (productName: string) => void;
}

// helper: accept array of strings OR array of objects
const normalizeProducts = (raw: any): Product[] => {
  if (!Array.isArray(raw)) return [];

  if (raw.length === 0) return [];

  // case 1: ["A", "B", ...]
  if (typeof raw[0] === "string") {
    return raw.map((name) => ({ product_name: name as string }));
  }

  // case 2: [{product_name: "A"}, ...]
  if (typeof raw[0] === "object" && raw[0] !== null) {
    return raw
      .filter((item) => typeof item.product_name === "string")
      .map((item) => ({ product_name: item.product_name }));
  }

  return [];
};

const ProductSearchDropdown: React.FC<ProductSearchDropdownProps> = ({
  authToken,
  onProductSelect,
}) => {
  const [searchQuery, setSearchQuery] = useState("");
  const [searchResults, setSearchResults] = useState<Product[]>([]);
  const [allProducts, setAllProducts] = useState<Product[]>([]);
  const [showDropdown, setShowDropdown] = useState(false);
  const [searchLoading, setSearchLoading] = useState(false);
  const [allLoading, setAllLoading] = useState(false);
  const [hasLoadedAll, setHasLoadedAll] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  const hasSearch = searchQuery.trim().length > 0;
  const displayedProducts = hasSearch ? searchResults : allProducts;

  // -------- Fetch ALL products (for dropdown) --------
  const fetchAllProducts = async () => {
    if (hasLoadedAll) return;
    try {
      setAllLoading(true);
      setLoadError(null);

      const res = await fetch("http://localhost:5000/Product_names", {
        method: "GET",
        headers: {
          Authorization: `Bearer ${authToken ?? ""}`,
          "Content-Type": "application/json",
        },
      });

      if (!res.ok) throw new Error(`HTTP error! status: ${res.status}`);

      const json = await res.json();
      console.log("ALL products response:", json);

      // tries multiple keys in case backend uses a different name
      const list = normalizeProducts(
        json.product_names ?? json.products ?? json.product_list
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
        const res = await fetch(
          `http://localhost:5000/Product_search?query=${encodeURIComponent(
            searchQuery
          )}`,
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
        console.log("SEARCH response:", json);

        const list = normalizeProducts(
          json.product_names ?? json.products ?? json.product_list
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
  }, [searchQuery, authToken]);

  const handleToggleDropdown = async () => {
    const next = !showDropdown;
    setShowDropdown(next);
    if (next && !hasLoadedAll) {
      await fetchAllProducts();   // ✅ still works if user clicks the arrow
    }
  };


  const handleSelect = (p: Product) => {
    onProductSelect(p.product_name);
    setShowDropdown(false);
    setSearchQuery("");
  };

  return (
    <div className="relative w-full max-w-xs">
      <div className="relative flex items-center">
        <input
          type="text"
          placeholder="Search products, SKUs..."
          className="
    w-full
    rounded-lg
    border border-[#8B8585]
    bg-[#FBFBFB]
    px-4 py-2 pl-9
    text-sm text-charcoal-500
    outline-none
    shadow-sm
    focus:border-[#C4C4C4]
    focus:ring-0
  "
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
          onFocus={() => {
            setShowDropdown(true);
            if (!hasLoadedAll) {
              fetchAllProducts();   // 🔴 trigger loading all products as soon as user focuses
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
                <div className="text-gray-800">{p.product_name}</div>
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
