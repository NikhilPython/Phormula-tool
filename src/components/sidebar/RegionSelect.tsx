// // src/components/sidebar/RegionSelect.tsx
// "use client";
// import * as React from "react";
// import { FiMapPin, FiChevronDown } from "react-icons/fi";

// export type RegionOption = { value: string; label: string };

// type Props = {
//   label?: string;
//   selectedCountry: string; // actually selected platform
//   options: RegionOption[];
//   onChange: (value: string) => void;
//   className?: string;
// };

// export default function RegionSelect({
//   label = "PLATFORM",
//   selectedCountry,
//   options,
//   onChange,
//   className = "",
// }: Props) {
//   // Track whether the user has interacted with the dropdown at least once
//   const [hasUserSelected, setHasUserSelected] = React.useState(false);

//   // Decide what the <select> should show:
//   // - Before any user interaction: always show placeholder ("select")
//   // - After interaction: show the actual selectedCountry from props
//   const effectiveValue =
//     hasUserSelected && selectedCountry ? selectedCountry : "select";

//   const handleChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
//     const value = e.target.value;

//     // Ignore if user somehow re-selects the placeholder
//     if (value === "select") return;

//     // First real interaction flips this so we start showing the platform
//     if (!hasUserSelected) {
//       setHasUserSelected(true);
//     }

//     onChange(value);
//   };

//   return (
//     <div className={`mb-4 ${className}`}>
//       <label className="flex items-center gap-2 text-xs text-[var(--color-green-500)] mb-1">
//         <FiMapPin className="h-5 w-5" />
//         <span className="tracking-wide">{label}</span>
//       </label>

//       <div className="relative">
//         <select
//           value={effectiveValue}
//           onChange={handleChange}
//           aria-label={label}
//           className="
//             w-full rounded-md border border-[#41404233] bg-white text-sm text-charcoal-500
//             pl-3 pr-9 py-1 shadow-sm outline-none
//             focus:ring-2 focus:ring-green-500 focus:border-green-500
//             transition appearance-none
//           "
//         >
//           {/* Placeholder shown on initial load */}
//           <option value="select" disabled>
//             Select a Platform
//           </option>

//           {options.map((opt, i) => (
//             <option key={`${opt.value}-${i}`} value={opt.value}>
//               {opt.label}
//             </option>
//           ))}
//           <option value="add_more_countries">➕ Add More Countries</option>
//         </select>

//         <FiChevronDown
//           className="pointer-events-none absolute right-3 top-1/2 h-4 w-4 -translate-y-1/2 text-green-500"
//           aria-hidden="true"
//         />
//       </div>
//     </div>
//   );
// }























// src/components/sidebar/RegionSelect.tsx
"use client";
import * as React from "react";
import { FiMapPin, FiChevronDown } from "react-icons/fi";

export type RegionOption = { value: string; label: string };

type Props = {
  label?: string;
  selectedCountry: string; // actually selected platform
  options: RegionOption[];
  onChange: (value: string) => void;
  className?: string;
};

export default function RegionSelect({
  label = "Platform",
  selectedCountry,
  options,
  onChange,
  className = "",
}: Props) {
  const [isOpen, setIsOpen] = React.useState(false);
  const containerRef = React.useRef<HTMLDivElement | null>(null);

  // Close dropdown on outside click
  React.useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    };

    document.addEventListener("mousedown", handleClickOutside);
    return () => document.removeEventListener("mousedown", handleClickOutside);
  }, []);

  // Find label for currently selected value
  const selectedOption = options.find((opt) => opt.value === selectedCountry);
  const displayLabel = selectedOption?.label || "Select a Platform";

  const handleSelect = (value: string) => {
    // Static only, but we still pass value up so parent
    // can decide what to do with "add_more_countries" later.
    onChange(value);
    setIsOpen(false);
  };

  return (
    <div ref={containerRef} className={`mb-4 ${className}`}>
      <label className="flex items-center gap-2 text-sm font-semibold text-green-500 mb-2 ml-2.5">
        <FiMapPin className="h-5 w-5" />
        <span className="tracking-wide">{label}</span>
      </label>

    <div className="relative ml-8">
        {/* Trigger button (replaces <select>) */}
        <button
          type="button"
          onClick={() => setIsOpen((prev) => !prev)}
          aria-label={label}
          className="
            w-full rounded-md border border-[#41404233]
            bg-white text-sm text-charcoal-500
            px-3 py-1 shadow-sm outline-none
            flex items-center justify-between
            focus:ring-1 focus:ring-green-500 focus:border-green-500
            transition
          "
        >
          <span className={displayLabel === "Select a Platform" ? "text-gray-400" : ""}>
            {displayLabel}
          </span>

          <FiChevronDown
            className="h-4 w-4 text-green-500"
            aria-hidden="true"
          />
        </button>

        {/* Dropdown menu */}
        {isOpen && (
          <div
            className="
      absolute z-20 mt-1 w-full max-h-60 overflow-auto
      rounded-md border border-[#414042]
      bg-[#F2F0F0]
    "
          >
            {options.map((opt) => (
              <button
                key={opt.value}
                onClick={() => handleSelect(opt.value)}
                className="
          block w-full text-left px-3 py-1 text-sm
          text-charcoal-500 hover:text-yellow-200
          hover:bg-green-500
        "
              >
                {opt.label}
              </button>
            ))}

            <button
              onClick={() => handleSelect('add_more_countries')}
              className="
        block w-full text-left px-3 py-1 text-sm font-semibold
        text-green-500 border-t border-gray-400/40
        hover:bg-green-500 hover:text-yellow-200
      "
            >
              Add More Countries
            </button>
          </div>
        )}

      </div>
    </div>
  );
}
