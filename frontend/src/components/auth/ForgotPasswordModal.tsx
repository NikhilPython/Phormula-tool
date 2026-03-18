// "use client";

// import { useState } from "react";
// import { FaCircleCheck } from "react-icons/fa6";
// import Button from "../ui/button/Button";
// import PageBreadcrumb from "../common/PageBreadCrumb";

// type ForgotPasswordModalProps = {
//   onClose: () => void;
// };

// const ForgotPasswordModal: React.FC<ForgotPasswordModalProps> = ({ onClose }) => {
//   const [email, setEmail] = useState<string>("");
//   const [message, setMessage] = useState<string>("");
//   const [error, setError] = useState<string>("");
//   const [loading, setLoading] = useState<boolean>(false);

//   const handleForgotPassword = async (e: React.FormEvent<HTMLFormElement>) => {
//     e.preventDefault();
//     setMessage("");
//     setError("");
//     setLoading(true);

//     try {
//       const response = await fetch(`${process.env.NEXT_PUBLIC_API_BASE_URL}/forgot_password`, {
//         method: "POST",
//         headers: {
//           "Content-Type": "application/json",
//         },
//         body: JSON.stringify({ email }),
//       });

//       const data = await response.json();
//       if (response.ok) {
//         setMessage(data.message);
//       } else {
//         setError(data.message || "An error occurred. Please try again.");
//       }
//     } catch (err) {
//       setError("An error occurred. Please try again.");
//     } finally {
//       setLoading(false);
//     }
//   };

//   const isSuccess = message === "Password reset email sent.";

//   return (
//     <div
//       className="fixed inset-0 flex items-center justify-center bg-black/40 z-50"
//       // optional: click on backdrop closes modal
//       onClick={onClose}
//     >
//       {/* Modal */}
//       <div
//         className="bg-white rounded-2xl w-full max-w-lg mx-4 p-6 font-[Lato] relative shadow-[6px_6px_7px_0px_#00000026]"
//         onClick={(e) => e.stopPropagation()} // prevent closing when clicking inside
//       >
//         {/* Close button */}
//         <button
//           onClick={onClose}
//           className="absolute top-3 right-3 text-gray-500 hover:text-gray-700 text-sm"
//           type="button"
//         >
//           ✕
//         </button>

//         {isSuccess ? (
//           <div className="space-y-4 mt-2">
//             <div className="flex flex-col items-center gap-2 text-[#5EA68E] text-3xl">
//               <FaCircleCheck size={40} />
//               <span className="font-semibold text-2xl">Email Sent</span>
//             </div>
//             <p className="text-sm text-[#414042] text-center">
//               Check your email and open the link we sent to continue.
//             </p>
//             <div className="flex justify-center">
//               <Button
//               variant="outline"
//               size="sm"
//                 // className="px-4 py-2 rounded-md bg-white text-[#414042] text-sm font-bold border border-[#D9D9D9]"
//                 onClick={onClose}
//                 type="button"
//               >
//                 Back to Login
//               </Button>
//             </div>
//           </div>
//         ) : (
//           <form className="space-y-4 mt-2" onSubmit={handleForgotPassword}>
//             {/* <h1 className="text-2xl font-semibold text-[#414042] text-center">
//               <span className="text-[#5EA68E] ">Forgot Password?</span>
//             </h1> */}
//             <PageBreadcrumb pageTitle="Forgot Password?" variant="table" textSize="2xl"/>

//             <p className="text-sm text-[#414042] text-center">
//               Enter your email and we will send you a link to reset your password.
//             </p>

//             <div className="border border-[#414042] rounded-md overflow-hidden">
//               <input
//                 type="email"
//                 id="email"
//                 placeholder="Enter your email"
//                 className="w-full px-3 py-2 text-sm outline-none"
//                 value={email}
//                 onChange={(e) => setEmail(e.target.value)}
//                 required
//               />
//             </div>

//             {message && <p className="text-sm text-center text-green-600">{message}</p>}
//             {error && <p className="text-sm text-center text-red-500">{error}</p>}

//             <div className="flex justify-center gap-2 pt-2">
//               <Button
//                 type="button"
//                 variant="outline"
//                 size="sm"
//                 onClick={onClose}
//                 // className="px-4 py-2 rounded-md border border-[#2c3854] text-[#2c3854] text-sm font-bold"
//                 >
//                 Back
//               </Button>
//               <Button
//                 type="submit"
//                 variant="primary"
//                 size="sm"
//                 disabled={loading}
//                 // className="px-4 py-2 rounded-md bg-[#2c3854] text-[#f8edcf] text-sm font-bold disabled:opacity-60"
//               >
//                 {loading ? "Sending..." : "Send"}
//               </Button>
//             </div>
//           </form>
//         )}
//       </div>
//     </div>
//   );
// };

// export default ForgotPasswordModal;















"use client";

import { useState } from "react";
import { FaCircleCheck } from "react-icons/fa6";
import Button from "../ui/button/Button";
import PageBreadcrumb from "../common/PageBreadCrumb";
import {
  forgotPasswordSchema,
  getForgotPasswordFieldErrors,
  sanitizeEmail,
  type ForgotPasswordFormErrors,
  type ForgotPasswordFormValues,
} from "@/lib/validations/authValidation";

type ForgotPasswordModalProps = {
  onClose: () => void;
};

type TouchedFields = {
  email: boolean;
};

const initialTouched: TouchedFields = {
  email: false,
};

const ForgotPasswordModal: React.FC<ForgotPasswordModalProps> = ({ onClose }) => {
  const [form, setForm] = useState<ForgotPasswordFormValues>({
    email: "",
  });

  const [touched, setTouched] = useState<TouchedFields>(initialTouched);
  const [errors, setErrors] = useState<ForgotPasswordFormErrors>({});
  const [message, setMessage] = useState<string>("");
  const [error, setError] = useState<string>("");
  const [loading, setLoading] = useState<boolean>(false);

  const validateForm = (values: ForgotPasswordFormValues = form) => {
    const result = forgotPasswordSchema.safeParse(values);

    if (result.success) {
      setErrors({});
      return { success: true as const, data: result.data };
    }

    const nextErrors = getForgotPasswordFieldErrors(result.error);
    setErrors(nextErrors);
    return { success: false as const, errors: nextErrors };
  };

  const validateSingleField = (
    field: keyof ForgotPasswordFormValues,
    nextValues?: Partial<ForgotPasswordFormValues>
  ) => {
    const merged = { ...form, ...nextValues };
    const result = forgotPasswordSchema.safeParse(merged);

    if (result.success) {
      setErrors((prev) => ({ ...prev, [field]: undefined }));
      return;
    }

    const nextErrors = getForgotPasswordFieldErrors(result.error);

    setErrors((prev) => ({
      ...prev,
      [field]: nextErrors[field],
    }));
  };

  const markTouched = (field: keyof TouchedFields) => {
    setTouched((prev) => ({ ...prev, [field]: true }));
  };

  const handleEmailChange = (value: string) => {
    const cleaned = sanitizeEmail(value);

    setForm((prev) => ({
      ...prev,
      email: cleaned,
    }));

    if (touched.email) {
      validateSingleField("email", { email: cleaned });
    }
  };

  const handleForgotPassword = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();

    setTouched({ email: true });
    setMessage("");
    setError("");

    const validation = validateForm();
    if (!validation.success) return;

    setLoading(true);

    try {
      const response = await fetch(
        `${process.env.NEXT_PUBLIC_API_BASE_URL}/forgot_password`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ email: form.email.trim().toLowerCase() }),
        }
      );

      const data = await response.json();

      if (response.ok) {
        setMessage(data.message);
        setErrors({});
      } else {
        setError(data.message || "An error occurred. Please try again.");
      }
    } catch {
      setError("An error occurred. Please try again.");
    } finally {
      setLoading(false);
    }
  };

  const isSuccess = message === "Password reset email sent.";

  return (
    <div
      className="fixed inset-0 flex items-center justify-center bg-black/40 z-50"
      onClick={onClose}
    >
      <div
        className="bg-white rounded-2xl w-full max-w-lg mx-4 p-6 font-[Lato] relative shadow-[6px_6px_7px_0px_#00000026]"
        onClick={(e) => e.stopPropagation()}
      >
        <button
          onClick={onClose}
          className="absolute top-3 right-3 text-gray-500 hover:text-gray-700 text-sm"
          type="button"
        >
          ✕
        </button>

        {isSuccess ? (
          <div className="space-y-4 mt-2">
            <div className="flex flex-col items-center gap-2 text-[#5EA68E] text-3xl">
              <FaCircleCheck size={40} />
              <span className="font-semibold text-2xl">Email Sent</span>
            </div>

            <p className="text-sm text-[#414042] text-center">
              Check your email and open the link we sent to continue.
            </p>

            <div className="flex justify-center">
              <Button
                variant="outline"
                size="sm"
                onClick={onClose}
                type="button"
              >
                Back to Login
              </Button>
            </div>
          </div>
        ) : (
          <form className="space-y-4 mt-2" onSubmit={handleForgotPassword} noValidate>
            <PageBreadcrumb
              pageTitle="Forgot Password?"
              variant="table"
              textSize="2xl"
            />

            <p className="text-sm text-[#414042] text-center">
              Enter your email and we will send you a link to reset your password.
            </p>

            <div className="border border-[#414042] rounded-md overflow-hidden">
              <input
                type="email"
                id="email"
                placeholder="Enter your email"
                className={`w-full px-3 py-2 text-sm outline-none ${
                  touched.email && errors.email ? "border-red-500" : ""
                }`}
                value={form.email}
                onChange={(e) => handleEmailChange(e.target.value)}
                onBlur={() => {
                  markTouched("email");
                  validateSingleField("email");
                }}
                autoComplete="email"
                inputMode="email"
                required
              />
            </div>

            {touched.email && errors.email && (
              <p className="text-sm text-center text-red-500" aria-live="polite">
                {errors.email}
              </p>
            )}

            {message && <p className="text-sm text-center text-green-600">{message}</p>}
            {error && <p className="text-sm text-center text-red-500">{error}</p>}

            <div className="flex justify-center gap-2 pt-2">
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={onClose}
              >
                Back
              </Button>

              <Button
                type="submit"
                variant="primary"
                size="sm"
                disabled={loading}
              >
                {loading ? "Sending..." : "Send"}
              </Button>
            </div>
          </form>
        )}
      </div>
    </div>
  );
};

export default ForgotPasswordModal;