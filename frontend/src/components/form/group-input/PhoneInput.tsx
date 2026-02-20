// // // // "use client";
// // // // import React, { useState } from "react";

// // // // interface CountryCode {
// // // //   code: string;
// // // //   label: string;
// // // // }

// // // // interface PhoneInputProps {
// // // //   countries: CountryCode[];
// // // //   placeholder?: string;
// // // //   onChange?: (phoneNumber: string) => void;
// // // //   selectPosition?: "start" | "end"; // New prop for dropdown position
// // // // }

// // // // const PhoneInput: React.FC<PhoneInputProps> = ({
// // // //   countries,
// // // //   placeholder = "+1 (555) 000-0000",
// // // //   onChange,
// // // //   selectPosition = "start", // Default position is 'start'
// // // // }) => {
// // // //   const [selectedCountry, setSelectedCountry] = useState<string>("US");
// // // //   const [phoneNumber, setPhoneNumber] = useState<string>("+1");

// // // //   const countryCodes: Record<string, string> = countries.reduce(
// // // //     (acc, { code, label }) => ({ ...acc, [code]: label }),
// // // //     {}
// // // //   );

// // // //   const handleCountryChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
// // // //     const newCountry = e.target.value;
// // // //     setSelectedCountry(newCountry);
// // // //     setPhoneNumber(countryCodes[newCountry]);
// // // //     if (onChange) {
// // // //       onChange(countryCodes[newCountry]);
// // // //     }
// // // //   };

// // // //   const handlePhoneNumberChange = (e: React.ChangeEvent<HTMLInputElement>) => {
// // // //     const newPhoneNumber = e.target.value;
// // // //     setPhoneNumber(newPhoneNumber);
// // // //     if (onChange) {
// // // //       onChange(newPhoneNumber);
// // // //     }
// // // //   };

// // // //   return (
// // // //     <div className="relative flex">
// // // //       {/* Dropdown position: Start */}
// // // //       {selectPosition === "start" && (
// // // //         <div className="absolute">
// // // //           <select
// // // //             value={selectedCountry}
// // // //             onChange={handleCountryChange}
// // // //             className="appearance-none bg-none rounded-l-lg border-0 border-r border-gray-200 bg-transparent py-3 pl-3.5 pr-8 leading-tight text-gray-700 focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10 dark:border-gray-800 dark:text-gray-400"
// // // //           >
// // // //             {countries.map((country) => (
// // // //               <option
// // // //                 key={country.code}
// // // //                 value={country.code}
// // // //                 className="text-gray-700 dark:bg-gray-900 dark:text-gray-400"
// // // //               >
// // // //                 {country.code}
// // // //               </option>
// // // //             ))}
// // // //           </select>
// // // //           <div className="absolute inset-y-0 flex items-center text-gray-700 pointer-events-none bg-none right-3 dark:text-gray-400">
// // // //             <svg
// // // //               className="stroke-current"
// // // //               width="20"
// // // //               height="20"
// // // //               viewBox="0 0 20 20"
// // // //               fill="none"
// // // //               xmlns="http://www.w3.org/2000/svg"
// // // //             >
// // // //               <path
// // // //                 d="M4.79175 7.396L10.0001 12.6043L15.2084 7.396"
// // // //                 stroke="currentColor"
// // // //                 strokeWidth="1.5"
// // // //                 strokeLinecap="round"
// // // //                 strokeLinejoin="round"
// // // //               />
// // // //             </svg>
// // // //           </div>
// // // //         </div>
// // // //       )}

// // // //       {/* Input field */}
// // // //       <input
// // // //         type="tel"
// // // //         value={phoneNumber}
// // // //         onChange={handlePhoneNumberChange}
// // // //         placeholder={placeholder}
// // // //         className={`dark:bg-dark-900 h-11 w-full ${
// // // //           selectPosition === "start" ? "pl-[84px]" : "pr-[84px]"
// // // //         } rounded-lg border border-gray-300 bg-transparent py-3 px-4 text-sm text-gray-800 shadow-theme-xs placeholder:text-gray-400 focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10 dark:border-gray-700 dark:bg-gray-900 dark:text-white/90 dark:placeholder:text-white/30 dark:focus:border-brand-800`}
// // // //       />

// // // //       {/* Dropdown position: End */}
// // // //       {selectPosition === "end" && (
// // // //         <div className="absolute right-0">
// // // //           <select
// // // //             value={selectedCountry}
// // // //             onChange={handleCountryChange}
// // // //             className="appearance-none bg-none rounded-r-lg border-0 border-l border-gray-200 bg-transparent py-3 pl-3.5 pr-8 leading-tight text-gray-700 focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10 dark:border-gray-800 dark:text-gray-400"
// // // //           >
// // // //             {countries.map((country) => (
// // // //               <option
// // // //                 key={country.code}
// // // //                 value={country.code}
// // // //                 className="text-gray-700 dark:bg-gray-900 dark:text-gray-400"
// // // //               >
// // // //                 {country.code}
// // // //               </option>
// // // //             ))}
// // // //           </select>
// // // //           <div className="absolute inset-y-0 flex items-center text-gray-700 pointer-events-none right-3 dark:text-gray-400">
// // // //             <svg
// // // //               className="stroke-current"
// // // //               width="20"
// // // //               height="20"
// // // //               viewBox="0 0 20 20"
// // // //               fill="none"
// // // //               xmlns="http://www.w3.org/2000/svg"
// // // //             >
// // // //               <path
// // // //                 d="M4.79175 7.396L10.0001 12.6043L15.2084 7.396"
// // // //                 stroke="currentColor"
// // // //                 strokeWidth="1.5"
// // // //                 strokeLinecap="round"
// // // //                 strokeLinejoin="round"
// // // //               />
// // // //             </svg>
// // // //           </div>
// // // //         </div>
// // // //       )}
// // // //     </div>
// // // //   );
// // // // };

// // // // export default PhoneInput;



































// // // "use client";
// // // import React, { useState } from "react";

// // // interface CountryCode {
// // //   code: string;  // ISO-2 like 'US', 'IN'
// // //   label: string; // dial code like '+1', '+91'
// // // }

// // // interface PhoneInputProps {
// // //   countries: CountryCode[];
// // //   placeholder?: string;
// // //   onChange?: (phoneNumber: string) => void;
// // //   selectPosition?: "start" | "end";
// // // }

// // // // helper to convert ISO code -> emoji flag 🇮🇳
// // // const isoToFlag = (iso: string) =>
// // //   iso
// // //     .toUpperCase()
// // //     .replace(/./g, (char) =>
// // //       String.fromCodePoint(127397 + char.charCodeAt(0))
// // //     );

// // // const PhoneInput: React.FC<PhoneInputProps> = ({
// // //   countries,
// // //   placeholder = "+1 (555) 000-0000",
// // //   onChange,
// // //   selectPosition = "start",
// // // }) => {
// // //   const [selectedCountry, setSelectedCountry] = useState<string>("US");
// // //   const [phoneNumber, setPhoneNumber] = useState<string>("+1");

// // //   // map: 'US' -> '+1'
// // //   const countryCodes: Record<string, string> = countries.reduce(
// // //     (acc, { code, label }) => ({ ...acc, [code]: label }),
// // //     {}
// // //   );

// // //   const handleCountryChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
// // //     const newCountry = e.target.value;
// // //     setSelectedCountry(newCountry);

// // //     const newDialCode = countryCodes[newCountry] || "";
// // //     setPhoneNumber(newDialCode);

// // //     onChange?.(newDialCode);
// // //   };

// // //   const handlePhoneNumberChange = (e: React.ChangeEvent<HTMLInputElement>) => {
// // //     const newPhoneNumber = e.target.value;
// // //     setPhoneNumber(newPhoneNumber);
// // //     onChange?.(newPhoneNumber);
// // //   };

// // //   const Select = (
// // //     <div className={selectPosition === "start" ? "absolute" : "absolute right-0"}>
// // //       <select
// // //         value={selectedCountry}
// // //         onChange={handleCountryChange}
// // //         className={`appearance-none bg-none ${
// // //           selectPosition === "start" ? "rounded-l-lg border-r" : "rounded-r-lg border-l"
// // //         } border-gray-200 bg-transparent py-3 pl-3.5 pr-8 leading-tight text-gray-700 focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10 dark:border-gray-800 dark:text-gray-400`}
// // //       >
// // //         {countries.map((country) => (
// // //           <option
// // //             key={country.code}
// // //             value={country.code}
// // //             className="text-gray-700 dark:bg-gray-900 dark:text-gray-400"
// // //           >
// // //             {/* 👇 flag + dial code */}
// // //             {isoToFlag(country.code)} {country.label}
// // //           </option>
// // //         ))}
// // //       </select>
// // //       <div className="absolute inset-y-0 flex items-center text-gray-700 pointer-events-none right-3 dark:text-gray-400">
// // //         <svg
// // //           className="stroke-current"
// // //           width="20"
// // //           height="20"
// // //           viewBox="0 0 20 20"
// // //           fill="none"
// // //           xmlns="http://www.w3.org/2000/svg"
// // //         >
// // //           <path
// // //             d="M4.79175 7.396L10.0001 12.6043L15.2084 7.396"
// // //             stroke="currentColor"
// // //             strokeWidth="1.5"
// // //             strokeLinecap="round"
// // //             strokeLinejoin="round"
// // //           />
// // //         </svg>
// // //       </div>
// // //     </div>
// // //   );

// // //   return (
// // //     <div className="relative flex">
// // //       {selectPosition === "start" && Select}

// // //       <input
// // //         type="tel"
// // //         value={phoneNumber}
// // //         onChange={handlePhoneNumberChange}
// // //         placeholder={placeholder}
// // //         className={`dark:bg-dark-900 h-11 w-full ${
// // //           selectPosition === "start" ? "pl-[84px]" : "pr-[84px]"
// // //         } rounded-lg border border-gray-300 bg-transparent py-3 px-4 text-sm text-gray-800 shadow-theme-xs placeholder:text-gray-400 focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10 dark:border-gray-700 dark:bg-gray-900 dark:text-white/90 dark:placeholder:text-white/30 dark:focus:border-brand-800`}
// // //       />

// // //       {selectPosition === "end" && Select}
// // //     </div>
// // //   );
// // // };

// // // export default PhoneInput;












// "use client";
// import React, { useMemo, useState } from "react";

// export interface CountryCode {
//   code: string;    // ISO-2 code, e.g. "US"
//   name: string;    // Country name, e.g. "United States"
//   label: string;   // Dial code, e.g. "+1"
// }

// interface PhoneInputProps {
//   countries: CountryCode[];
//   placeholder?: string;
//   onChange?: (phoneNumber: string) => void;
//   selectPosition?: "start" | "end"; // dropdown position
//   value?: string; // optional controlled value
// }

// // ISO code -> emoji flag 🇮🇳
// const isoToFlag = (iso: string) =>
//   iso
//     .toUpperCase()
//     .replace(/./g, (char) =>
//       String.fromCodePoint(127397 + char.charCodeAt(0))
//     );

// const PhoneInput: React.FC<PhoneInputProps> = ({
//   countries,
//   placeholder = "Enter phone number",
//   onChange,
//   selectPosition = "start",
//   value,
// }) => {
//   const defaultCountry = countries.find((c) => c.code === "US") ?? countries[0];

//   const [selectedCountry, setSelectedCountry] = useState<string>(
//     defaultCountry?.code || ""
//   );

//   const [phoneNumber, setPhoneNumber] = useState<string>(
//     value ?? defaultCountry?.label ?? ""
//   );

//   // map: "US" -> "+1"
//   const countryCodes: Record<string, string> = useMemo(
//     () =>
//       countries.reduce(
//         (acc, { code, label }) => ({ ...acc, [code]: label }),
//         {} as Record<string, string>
//       ),
//     [countries]
//   );

//   const handleCountryChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
//     const newCountry = e.target.value;
//     setSelectedCountry(newCountry);

//     const newDialCode = countryCodes[newCountry] || "";
//     setPhoneNumber(newDialCode);

//     onChange?.(newDialCode);
//   };

//   const handlePhoneNumberChange = (e: React.ChangeEvent<HTMLInputElement>) => {
//     const newPhoneNumber = e.target.value;
//     setPhoneNumber(newPhoneNumber);
//     onChange?.(newPhoneNumber);
//   };

//   const Select = (
//     <div className={selectPosition === "start" ? "absolute" : "absolute right-0"}>
//       <select
//         value={selectedCountry}
//         onChange={handleCountryChange}
//         className={`appearance-none bg-none ${selectPosition === "start" ? "rounded-l-lg border-r" : "rounded-r-lg border-l"
//           } border-gray-200 bg-transparent py-3 pl-3.5 pr-8 leading-tight text-gray-700 focus:border-brand-300 focus:outline-hidden focus:ring-3 focus:ring-brand-500/10  dark:text-gray-400`}
//       >
//         {countries.map((country) => (
//           <option
//             key={country.code}
//             value={country.code}
//             className="text-gray-700 dark:bg-gray-900 dark:text-gray-400"
//           >
//             {isoToFlag(country.code)} {country.name} {country.label}
//           </option>
//         ))}
//       </select>
//       <div className="absolute inset-y-0 flex items-center text-gray-700 pointer-events-none right-3 dark:text-gray-400">
//         <svg
//           className="stroke-current"
//           width="20"
//           height="20"
//           viewBox="0 0 20 20"
//           fill="none"
//           xmlns="http://www.w3.org/2000/svg"
//         >
//           <path
//             d="M4.79175 7.396L10.0001 12.6043L15.2084 7.396"
//             stroke="currentColor"
//             strokeWidth="1.5"
//             strokeLinecap="round"
//             strokeLinejoin="round"
//           />
//         </svg>
//       </div>
//     </div>
//   );

//   return (
//     <div className="relative flex">
//       {/* Dropdown on the left – same as previous UI */}
//       {selectPosition === "start" && Select}

//       {/* Input field (unchanged layout) */}
//       <input
//         type="tel"
//         value={phoneNumber}
//         onChange={handlePhoneNumberChange}
//         placeholder={placeholder}
//         className={`dark:bg-dark-900 h-11 w-full ${selectPosition === "start" ? "pl-[84px]" : "pr-[84px]"
//           } rounded-lg bg-transparent py-3 px-4 text-sm text-gray-800 shadow-theme-xs 
//   placeholder:text-gray-400 focus:outline-hidden focus:ring-0 
//   dark:bg-gray-900 dark:text-white/90 dark:placeholder:text-white/30`}
//       />


//       {/* Dropdown on the right if needed */}
//       {selectPosition === "end" && Select}
//     </div>
//   );
// };

// export default PhoneInput;











"use client";

import React, { useMemo, useState } from "react";
import ReactCountryFlag from "react-country-flag";

export interface CountryCode {
  code: string;   // ISO-2 code, e.g. "US"
  name: string;   // Country name
  label: string;  // Dial code, e.g. "+1"
}

interface PhoneInputMeta {
  dialCode: string;          // e.g. "1", "91"
  country: CountryCode;      // full country object
}

interface PhoneInputProps {
  countries: CountryCode[];
  placeholder?: string;
  onChange?: (phoneNumber: string, meta: PhoneInputMeta) => void;
  value?: string;
  required?: boolean;
  autoComplete?: string;
  onBlur?: (e: React.FocusEvent<HTMLInputElement>) => void;
  selectPosition?: "start" | "end";
}

const PRIORITY_CODES = ["US", "IN", "GB", "CA"];

const PhoneInput: React.FC<PhoneInputProps> = ({
  countries,
  placeholder = "Enter phone number",
  onChange,
  value,
  required,
  autoComplete,
  onBlur,
  selectPosition = "start",
}) => {
  const defaultCountry =
    countries.find((c) => c.code === "US") ?? countries[0];

  const [selectedCountry, setSelectedCountry] = useState<CountryCode>(
    defaultCountry
  );
  const [open, setOpen] = useState(false);
  const [phoneNumber, setPhoneNumber] = useState<string>(value ?? "");

  const { priorityCountries, otherCountries } = useMemo(() => {
    const priority = countries.filter((c) => PRIORITY_CODES.includes(c.code));
    const others = countries.filter((c) => !PRIORITY_CODES.includes(c.code));
    return { priorityCountries: priority, otherCountries: others };
  }, [countries]);

  const getDialCodeDigits = (country: CountryCode) =>
    country.label.replace(/\D/g, ""); // "+91" -> "91"

  const emitChange = (val: string, country: CountryCode) => {
    onChange?.(val, {
      dialCode: getDialCodeDigits(country),
      country,
    });
  };

  const handleSelect = (country: CountryCode) => {
    setSelectedCountry(country);
    setPhoneNumber("");        // keep input empty, dial code only in selector
    emitChange("", country);
    setOpen(false);
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const val = e.target.value;
    setPhoneNumber(val);
    emitChange(val, selectedCountry);
  };

  return (
    <div className="relative flex items-center w-full 2xl:h-9 h-8">
      {/* Left selector: flag + dial code */}
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className="flex items-center gap-1 px-2 py-1 cursor-pointer select-none focus:outline-none"
      >
        <ReactCountryFlag
          svg
          countryCode={selectedCountry.code}
          className="w-5 h-5 rounded-sm"
        />
        <span className="text-sm text-gray-700 dark:text-gray-300">
          {selectedCountry.label}
        </span>
        <svg
          className="w-4 h-4 ml-1 text-gray-600"
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth={2}
            d="M6 9l6 6 6-6"
          />
        </svg>
      </button>

      {/* Divider */}
      <div className="w-px h-6 bg-gray-300 mx-2" />

      {/* Phone input (user types here) */}
      <input
        type="tel"
        value={phoneNumber}
        onChange={handleInputChange}
        placeholder={placeholder}
        className="flex-1 bg-transparent outline-none text-sm text-gray-800 dark:text-white/90 placeholder:text-gray-400"
      />

      {/* Dropdown */}
      {open && (
        <div
          className="
            absolute left-0 top-full mt-1 
            w-64 max-h-56 overflow-y-auto 
            rounded-md border border-gray-200 bg-white shadow-lg 
            z-50
            dark:bg-gray-800 dark:border-gray-700
          "
        >
          {/* Priority countries */}
          {priorityCountries.map((country) => (
            <button
              key={country.code}
              type="button"
              onClick={() => handleSelect(country)}
              className="w-full px-3 py-1.5 text-left flex items-center gap-2 text-sm hover:bg-gray-100 dark:hover:bg-gray-700"
            >
              <ReactCountryFlag
                svg
                countryCode={country.code}
                className="w-5 h-5 rounded-sm"
              />
              <span>{country.name}</span>
              <span className="text-gray-500">{country.label}</span>
            </button>
          ))}

          {priorityCountries.length > 0 && otherCountries.length > 0 && (
            <div className="border-t my-1 border-gray-200 dark:border-gray-700" />
          )}

          {/* Other countries */}
          {otherCountries.map((country) => (
            <button
              key={country.code}
              type="button"
              onClick={() => handleSelect(country)}
              className="w-full px-3 py-1.5 text-left flex items-center gap-2 text-sm hover:bg-gray-100 dark:hover:bg-gray-700"
            >
              <ReactCountryFlag
                svg
                countryCode={country.code}
                className="w-5 h-5 rounded-sm"
              />
              <span>{country.name}</span>
              <span className="text-gray-500">{country.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
};

export default PhoneInput;
