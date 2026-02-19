import React, { ReactNode } from "react";

interface ButtonProps {
  children: ReactNode; // Button text or content
  size?: "sm" | "md"; // Button size
  variant?: "primary" | "outline"; // Button variant
  startIcon?: ReactNode; // Icon before the text
  endIcon?: ReactNode; // Icon after the text
  onClick?: () => void; // Click handler
  disabled?: boolean; // Disabled state
  className?: string; // Disabled state
  type?: "button" | "submit" | "reset"; // ✅ Added this line
  title?: string; // Title for the button
}

const Button: React.FC<ButtonProps> = ({
  children,
  size = "md",
  variant = "primary",
  startIcon,
  endIcon,
  onClick,
  className = "",
  disabled = false,
  type = "button",
  title
}) => {
  // Size Classes
  const sizeClasses = {
    sm: "px-2.5 py-1 text-xs sm:px-3 sm:py-1 sm:text-sm",
    md: "px-3 py-2 text-sm sm:px-4 sm:py-2.5 sm:text-md",
  };

  // Variant Classes
  const variantClasses = {
    primary:
      "bg-blue-700 text-yellow-200 shadow-theme-xs hover:bg-blue-600 disabled:bg-[#51617D]",
    outline:
      "bg-white text-charcoal-500 ring-1 ring-inset ring-gray-300 hover:bg-[#d9d9d966] dark:bg-gray-800 dark:text-gray-400 dark:ring-gray-700 dark:hover:bg-white/[0.03] dark:hover:text-gray-300",
  };

  return (
    <button
      type={type}
      className={`inline-flex items-center justify-center font-medium gap-2 rounded-lg transition h-11 ${className} ${sizeClasses[size]
        } ${variantClasses[variant]} ${disabled ? "cursor-not-allowed opacity-50" : ""
        }`}
      onClick={onClick}
      disabled={disabled}
    >
      {startIcon && <span className="flex items-center">{startIcon}</span>}
      {children}
      {endIcon && <span className="flex items-center">{endIcon}</span>}
    </button>
  );
};

export default Button;
