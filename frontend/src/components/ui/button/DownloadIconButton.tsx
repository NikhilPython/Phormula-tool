// import React from "react";
// import { IoDownload } from "react-icons/io5";

// interface DownloadIconButtonProps {
//   onClick?: () => void;
//   disabled?: boolean;
//   className?: string;
//   size?: "sm" | "md";
//   type?: "button" | "submit" | "reset";
// }

// const DownloadIconButton: React.FC<DownloadIconButtonProps> = ({
//   onClick,
//   disabled = false,
//   className = "",
//   size = "sm",
//   type = "button"
// }) => {
//   // Size styles (matching your Button)
//   const sizeClasses = {
//     sm: "p-1 text-sm sm:p-1.5",
//     md: "p-2 text-sm sm:p-2.5"
//   };

//   // Outline variant (copied from your original button)
//   const outlineClasses =
//     "bg-white text-charcoal-500 ring-1 ring-inset ring-gray-300 hover:bg-gray-50 " +
//     "dark:bg-gray-800 dark:text-gray-400 dark:ring-gray-700 " +
//     "dark:hover:bg-white/[0.03] dark:hover:text-gray-300";

//   return (
//     <button
//       type={type}
//       onClick={onClick}
//       disabled={disabled}
//       className={`inline-flex items-center justify-center rounded-lg transition 
//         ${sizeClasses[size]} 
//         ${outlineClasses} 
//         ${disabled ? "cursor-not-allowed opacity-50" : ""} 
//         ${className}`}
//     >
//       <IoDownload size={22} />
//     </button>
//   );
// };

// export default DownloadIconButton;














import React from "react";
import { IoDownload } from "react-icons/io5";

interface DownloadIconButtonProps {
  onClick?: React.MouseEventHandler<HTMLButtonElement>;
  disabled?: boolean;
  className?: string;        // button class
  iconClassName?: string;    // icon class ✅
  size?: "sm" | "md";
  type?: "button" | "submit" | "reset";
}

const DownloadIconButton: React.FC<DownloadIconButtonProps> = ({
  onClick,
  disabled = false,
  className = "",
  iconClassName = "",
  size = "sm",
  type = "button"
}) => {
  const sizeClasses = {
    sm: "p-1 text-sm lg:p-1.5",
    md: "p-1.5 text-sm sm:p-2 lg:p-2.5"
  };

  const iconSizeClasses = {
    sm: "h-4 w-4 lg:h-[22px] lg:w-[22px]",
    md: "h-4 w-4 sm:h-5 sm:w-5 lg:h-[22px] lg:w-[22px]"
  };

  const outlineClasses =
    "bg-white text-charcoal-500 ring-1 ring-inset ring-gray-300 hover:bg-gray-50 " +
    "dark:bg-gray-800 dark:text-gray-400 dark:ring-gray-700 " +
    "dark:hover:bg-white/[0.03] dark:hover:text-gray-300";

  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`inline-flex items-center justify-center rounded-lg transition
        ${sizeClasses[size]}
        ${outlineClasses}
        ${disabled ? "cursor-not-allowed opacity-50" : ""}
        ${className}`}
    >
      <IoDownload
        className={`${iconSizeClasses[size]} ${iconClassName}`}
      />
    </button>
  );
};

export default DownloadIconButton;
