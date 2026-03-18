// "use client";

// import Checkbox from "@/components/form/input/Checkbox";
// import Input from "@/components/form/input/InputField";
// import Label from "@/components/form/Label";
// import Button from "@/components/ui/button/Button";
// import { EyeCloseIcon, EyeIcon } from "@/icons";
// import Link from "next/link";
// import React, { useEffect, useState } from "react";
// import { useRouter, useSearchParams } from "next/navigation";
// import { useAppDispatch, useAppSelector } from "@/lib/store";
// import {
//   setAuthError,
//   setAuthLoading,
//   setCredentials,
//   setUser,
//   clearAuthError,
// } from "@/lib/features/auth/authSlice";
// import { useLoginMutation, useMemberLoginMutation } from "@/lib/api/authApi";
// import { API_BASE } from "@/config/env";
// import ForgotPasswordModal from "./ForgotPasswordModal";
// import { auth, googleProvider } from "@/lib/firebase/firebase";
// import { signInWithPopup } from "firebase/auth";
// import axios from "axios";

// export default function SignInForm() {
//   const router = useRouter();
//   const search = useSearchParams();
//   const dispatch = useAppDispatch();
//   const { status, error } = useAppSelector((s) => s.auth);

//   const redirect = search.get("redirect") || "/";

//   const [showPassword, setShowPassword] = useState(false);
//   const [isChecked, setIsChecked] = useState(true);
//   const [email, setEmail] = useState("");
//   const [password, setPassword] = useState("");

//   const [showForgotModal, setShowForgotModal] = useState(false);
//   const [isFallbacking, setIsFallbacking] = useState(false);

//   const [login, { isLoading: isLoggingIn }] = useLoginMutation();
//   const [memberLogin, { isLoading: isMemberLoggingIn }] =
//     useMemberLoginMutation();

//   useEffect(() => {
//     const savedEmail = localStorage.getItem("email") || "";
//     const savedPassword = localStorage.getItem("password") || "";
//     if (savedEmail && savedPassword) {
//       setEmail(savedEmail);
//       setPassword(savedPassword);
//       setIsChecked(true);
//     }
//   }, []);

//   const routeToDashboard = (country: string) => {
//     const now = new Date();
//     const currentMonth = now.toLocaleString("en-US", { month: "long" });
//     const currentYear = String(now.getFullYear());
//     router.replace(`/live-dashboard/${country}/${currentMonth}/${currentYear}`);
//   };

//   const onSubmit = async (e: React.FormEvent) => {
//     e.preventDefault();
//     if (status === "loading" || isLoggingIn || isMemberLoggingIn) return;

//     dispatch(setAuthLoading());
//     dispatch(clearAuthError());

//     const cleanEmail = email.trim().toLowerCase();

//     try {
//       let result: any;
//       let loginType: "client" | "member" = "client";

//       try {
//         result = await login({ email: cleanEmail, password }).unwrap();
//         loginType = "client";
//       } catch (clientErr: any) {
//         setIsFallbacking(true);
//         dispatch(clearAuthError());

//         try {
//           result = await memberLogin({ email: cleanEmail, password }).unwrap();
//           loginType = "member";
//         } catch (memberErr: any) {
//           const msg =
//             memberErr?.data?.message ||
//             memberErr?.error ||
//             "Login failed. Please try again.";
//           dispatch(setAuthError(msg));
//           return;
//         } finally {
//           setIsFallbacking(false);
//         }
//       }

//       dispatch(setCredentials({ token: result.token }));

//       if (isChecked) {
//         localStorage.setItem("email", cleanEmail);
//         localStorage.setItem("password", password);
//       } else {
//         localStorage.removeItem("email");
//         localStorage.removeItem("password");
//       }

//       if (loginType === "client") {
//         const me = await fetch(`${API_BASE}/get_user_data`, {
//           method: "GET",
//           headers: { Authorization: `Bearer ${result.token}` },
//         })
//           .then((r) => r.json())
//           .catch(() => null);

//         if (me) dispatch(setUser({ ...me, is_member: false }));

//         const hasMarketplace =
//           typeof me?.marketplace_id === "string" &&
//           me.marketplace_id.trim().length > 0;

//         if (!hasMarketplace) {
//           router.replace("/choose-country?onboard=1");
//           return;
//         }

//         const countryFromBackend =
//           typeof me?.country === "string" && me.country.trim().length > 0
//             ? me.country.split(",")[0]
//             : "global";

//         routeToDashboard(countryFromBackend);
//         return;
//       }

//       dispatch(
//         setUser({
//           email: cleanEmail,
//           is_member: true,
//           member_id: result.member_id,
//           owner_user_id: result.owner_user_id,
//           modules: result.modules || [],
//           marketplaces: result.marketplaces || [],
//           countries: result.countries || [],
//         })
//       );

//       const memberCountry =
//         Array.isArray(result?.countries) && result.countries.length > 0
//           ? String(result.countries[0]).trim().toLowerCase()
//           : "global";

//       routeToDashboard(memberCountry);
//     } catch (err: any) {
//       const msg =
//         err?.status === 403
//           ? "Please verify your email first."
//           : err?.data?.message || err?.error || "Login failed. Please try again.";
//       dispatch(setAuthError(msg));
//     }
//   };

//   const onGoogleSignIn = async () => {
//     if (status === "loading" || isLoggingIn) return;

//     dispatch(setAuthLoading());

//     try {
//       const cred = await signInWithPopup(auth, googleProvider);
//       const email = cred.user.email;
//       const name = cred.user.displayName;
//       if (!email) throw new Error("Google account did not return an email");

//       const { data } = await axios.post(`${API_BASE}/google_register`, {
//         email,
//         name,
//       });
//       if (!data?.token) throw new Error(data?.message || "No token returned from server");

//       dispatch(setCredentials({ token: data.token }));

//       const me = await fetch(`${API_BASE}/get_user_data`, {
//         method: "GET",
//         headers: { Authorization: `Bearer ${data.token}` },
//       })
//         .then((r) => r.json())
//         .catch(() => null);

//       if (me) dispatch(setUser({ ...me, is_member: false }));

//       const hasMarketplace =
//         typeof me?.marketplace_id === "string" &&
//         me.marketplace_id.trim().length > 0;

//       if (!hasMarketplace) {
//         router.replace("/choose-country?onboard=1");
//         return;
//       }

//       const countryFromBackend =
//         typeof me?.country === "string" && me.country.trim().length > 0
//           ? me.country.split(",")[0]
//           : "global";

//       routeToDashboard(countryFromBackend);
//     } catch (err: any) {
//       const msg =
//         err?.response?.data?.message ||
//         err?.message ||
//         "Google sign-in failed. Please try again.";
//       dispatch(setAuthError(msg));
//     }
//   };

//   return (
//     <div className="flex flex-col  lg:w-1/2 w-full">
//       <div className="flex flex-col justify-center  flex-1 w-full xl:max-w-lg lg:mx-6 max-w-md xl:mx-auto mx-auto ">
//         <div>
//           <div className="mb-5 2xl:mb-8">
//             <h1 className="mb-2  text-green-500 text-title-sm dark:text-white/90 xl:text-title-lg lg:text-4xl sm:text-title-lg">
//               Welcome!
//             </h1>
//             <p className="2xl:text-base text-sm text-gray-500 dark:text-gray-400">
//               Please enter your login details
//             </p>
//           </div>

//           <form onSubmit={onSubmit} noValidate>
//             <div className="space-y-3">
//               <div>
//                 <Label>
//                   Email <span className="text-error-500 ">*</span>{" "}
//                 </Label>
//                 <Input
//                   placeholder="info@gmail.com"
//                   type="email"
//                   value={email}
//                   onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
//                     setEmail(e.target.value)
//                   }
//                   required
//                   autoComplete="email"
//                 />
//               </div>

//               <div>
//                 <Label>
//                   Password <span className="text-error-500">*</span>{" "}
//                 </Label>
//                 <div className="relative">
//                   <Input
//                     type={showPassword ? "text" : "password"}
//                     placeholder="Enter your password"
//                     value={password}
//                     onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
//                       setPassword(e.target.value)
//                     }
//                     required
//                     autoComplete="current-password"
//                   />
//                   <button
//                     type="button"
//                     onClick={() => setShowPassword(!showPassword)}
//                     className="absolute z-30 -translate-y-1/2 right-4 top-1/2"
//                     aria-label={showPassword ? "Hide password" : "Show password"}
//                   >
//                     {showPassword ? (
//                       <EyeIcon className="fill-gray-500 dark:fill-gray-400" />
//                     ) : (
//                       <EyeCloseIcon className="fill-gray-500 dark:fill-gray-400" />
//                     )}
//                   </button>
//                 </div>
//               </div>

//               <div className="flex items-center justify-between">
//                 <label className="inline-flex items-center gap-3 cursor-pointer">
//                   <Checkbox checked={isChecked} onChange={setIsChecked} />
//                   <span className="block font-normal text-gray-700 text-theme-sm dark:text-gray-400">
//                     Keep me logged in
//                   </span>
//                 </label>
//                 <button
//                   type="button"
//                   onClick={() => setShowForgotModal(true)}
//                   className="text-theme-sm text-blue-700"
//                 >
//                   Forgot password?
//                 </button>
//               </div>

//               {error &&
//                 !(status === "loading" || isLoggingIn || isMemberLoggingIn || isFallbacking) && (
//                   <p className="text-sm text-red-500 -mt-2" aria-live="polite">
//                     {error}
//                   </p>
//                 )}

//               <div>
//                 <Button
//                   className="w-full"
//                   size="md"
//                   type="submit"
//                   disabled={status === "loading" || isLoggingIn || isMemberLoggingIn || isFallbacking}
//                 >
//                   {status === "loading" || isLoggingIn || isMemberLoggingIn
//                     ? "Signing in…"
//                     : "Sign in"}
//                 </Button>
//               </div>
//             </div>
//           </form>

//           <div className="mt-2 w-full border border-charcoal-500 rounded-lg">
//             <button
//               type="button"
//               onClick={onGoogleSignIn}
//               className="w-full inline-flex items-center justify-center gap-3 px-4 py-2.5
//     text-charcoal-500 rounded-lg transition-colors text-md font-bold
//     dark:bg-white/5 dark:text-white/90 dark:hover:bg-white/10"
//             >
//               <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
//                 <path d="M18.7511 10.1944C18.7511 9.47495 18.6915 8.94995 18.5626 8.40552H10.1797V11.6527H15.1003C15.0011 12.4597 14.4654 13.675 13.2749 14.4916L13.2582 14.6003L15.9087 16.6126L16.0924 16.6305C17.7788 15.1041 18.7511 12.8583 18.7511 10.1944Z" fill="#4285F4" />
//                 <path d="M10.1788 18.75C12.5895 18.75 14.6133 17.9722 16.0915 16.6305L13.274 14.4916C12.5201 15.0068 11.5081 15.3666 10.1788 15.3666C7.81773 15.3666 5.81379 13.8402 5.09944 11.7305L4.99473 11.7392L2.23868 13.8295L2.20264 13.9277C3.67087 16.786 6.68674 18.75 10.1788 18.75Z" fill="#34A853" />
//                 <path d="M5.10014 11.7305C4.91165 11.186 4.80257 10.6027 4.80257 9.99992C4.80257 9.3971 4.91165 8.81379 5.09022 8.26935L5.08523 8.1534L2.29464 6.02954L2.20333 6.0721C1.5982 7.25823 1.25098 8.5902 1.25098 9.99992C1.25098 11.4096 1.5982 12.7415 2.20333 13.9277L5.10014 11.7305Z" fill="#FBBC05" />
//                 <path d="M10.1789 4.63331C11.8554 4.63331 12.9864 5.34303 13.6312 5.93612L16.1511 3.525C14.6035 2.11528 12.5895 1.25 10.1789 1.25C6.68676 1.25 3.67088 3.21387 2.20264 6.07218L5.08953 8.26943C5.81381 6.15972 7.81776 4.63331 10.1789 4.63331Z" fill="#EB4335" />
//               </svg>
//               Continue with Google
//             </button>
//           </div>

//           <div className="mt-5 max-w-fit mx-auto">
//             <p className="text-base font-normal text-center text-gray-700 dark:text-gray-400 sm:text-start">
//               Don&apos;t have an account ?{" "}
//               <Link href="/signup" className="text-blue-700">
//                 Sign Up
//               </Link>
//             </p>
//           </div>

//           {showForgotModal && (
//             <ForgotPasswordModal onClose={() => setShowForgotModal(false)} />
//           )}
//         </div>
//       </div>
//     </div>
//   );
// }



















"use client";

import Checkbox from "@/components/form/input/Checkbox";
import Input from "@/components/form/input/InputField";
import Label from "@/components/form/Label";
import Button from "@/components/ui/button/Button";
import { EyeCloseIcon, EyeIcon } from "@/icons";
import Link from "next/link";
import React, { useEffect, useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { useAppDispatch, useAppSelector } from "@/lib/store";
import {
  setAuthError,
  setAuthLoading,
  setCredentials,
  setUser,
  clearAuthError,
} from "@/lib/features/auth/authSlice";
import { useLoginMutation, useMemberLoginMutation } from "@/lib/api/authApi";
import { API_BASE } from "@/config/env";
import ForgotPasswordModal from "./ForgotPasswordModal";
import { auth, googleProvider } from "@/lib/firebase/firebase";
import { signInWithPopup } from "firebase/auth";
import axios from "axios";
import {
  getSignInFieldErrors,
  sanitizeEmail,
  sanitizePassword,
  signInSchema,
  type SignInFormErrors,
  type SignInFormValues,
} from "@/lib/validations/authValidation";

type TouchedFields = {
  email: boolean;
  password: boolean;
};

const initialTouched: TouchedFields = {
  email: false,
  password: false,
};

export default function SignInForm() {
  const router = useRouter();
  const search = useSearchParams();
  const dispatch = useAppDispatch();
  const { status, error } = useAppSelector((s) => s.auth);

  const redirect = search.get("redirect") || "/";

  const [showPassword, setShowPassword] = useState(false);
  const [isChecked, setIsChecked] = useState(true);
  const [showForgotModal, setShowForgotModal] = useState(false);
  const [isFallbacking, setIsFallbacking] = useState(false);

  const [form, setForm] = useState<SignInFormValues>({
    email: "",
    password: "",
  });

  const [touched, setTouched] = useState<TouchedFields>(initialTouched);
  const [errors, setErrors] = useState<SignInFormErrors>({});

  const [login, { isLoading: isLoggingIn }] = useLoginMutation();
  const [memberLogin, { isLoading: isMemberLoggingIn }] =
    useMemberLoginMutation();

  useEffect(() => {
    const savedEmail = localStorage.getItem("email") || "";
    const savedPassword = localStorage.getItem("password") || "";

    if (savedEmail && savedPassword) {
      setForm({
        email: savedEmail,
        password: savedPassword,
      });
      setIsChecked(true);
    }
  }, []);

  const routeToDashboard = (country: string) => {
    const now = new Date();
    const currentMonth = now.toLocaleString("en-US", { month: "long" });
    const currentYear = String(now.getFullYear());
    router.replace(`/live-dashboard/${country}/${currentMonth}/${currentYear}`);
  };

  const validateForm = (values: SignInFormValues = form) => {
    const result = signInSchema.safeParse(values);

    if (result.success) {
      setErrors({});
      return { success: true as const, data: result.data };
    }

    const nextErrors = getSignInFieldErrors(result.error);
    setErrors(nextErrors);
    return { success: false as const, errors: nextErrors };
  };

  const validateSingleField = (
    field: keyof SignInFormValues,
    nextValues?: Partial<SignInFormValues>
  ) => {
    const merged = { ...form, ...nextValues };
    const result = signInSchema.safeParse(merged);

    if (result.success) {
      setErrors((prev) => ({ ...prev, [field]: undefined }));
      return;
    }

    const nextErrors = getSignInFieldErrors(result.error);

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

  const handlePasswordChange = (value: string) => {
    const cleaned = sanitizePassword(value);

    setForm((prev) => ({
      ...prev,
      password: cleaned,
    }));

    if (touched.password) {
      validateSingleField("password", { password: cleaned });
    }
  };

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (status === "loading" || isLoggingIn || isMemberLoggingIn) return;

    setTouched({
      email: true,
      password: true,
    });

    const validation = validateForm();
    if (!validation.success) return;

    dispatch(setAuthLoading());
    dispatch(clearAuthError());

    const cleanEmail = form.email.trim().toLowerCase();

    try {
      let result: any;
      let loginType: "client" | "member" = "client";

      try {
        result = await login({ email: cleanEmail, password: form.password }).unwrap();
        loginType = "client";
      } catch (clientErr: any) {
        setIsFallbacking(true);
        dispatch(clearAuthError());

        try {
          result = await memberLogin({
            email: cleanEmail,
            password: form.password,
          }).unwrap();
          loginType = "member";
        } catch (memberErr: any) {
          const msg =
            memberErr?.data?.message ||
            memberErr?.error ||
            "Login failed. Please try again.";
          dispatch(setAuthError(msg));
          return;
        } finally {
          setIsFallbacking(false);
        }
      }

      dispatch(setCredentials({ token: result.token }));

      if (isChecked) {
        localStorage.setItem("email", cleanEmail);
        localStorage.setItem("password", form.password);
      } else {
        localStorage.removeItem("email");
        localStorage.removeItem("password");
      }

      if (loginType === "client") {
        const me = await fetch(`${API_BASE}/get_user_data`, {
          method: "GET",
          headers: { Authorization: `Bearer ${result.token}` },
        })
          .then((r) => r.json())
          .catch(() => null);

        if (me) dispatch(setUser({ ...me, is_member: false }));

        const hasMarketplace =
          typeof me?.marketplace_id === "string" &&
          me.marketplace_id.trim().length > 0;

        if (!hasMarketplace) {
          router.replace("/choose-country?onboard=1");
          return;
        }

        const countryFromBackend =
          typeof me?.country === "string" && me.country.trim().length > 0
            ? me.country.split(",")[0]
            : "global";

        routeToDashboard(countryFromBackend);
        return;
      }

      dispatch(
        setUser({
          email: cleanEmail,
          is_member: true,
          member_id: result.member_id,
          owner_user_id: result.owner_user_id,
          modules: result.modules || [],
          marketplaces: result.marketplaces || [],
          countries: result.countries || [],
        })
      );

      const memberCountry =
        Array.isArray(result?.countries) && result.countries.length > 0
          ? String(result.countries[0]).trim().toLowerCase()
          : "global";

      routeToDashboard(memberCountry);
    } catch (err: any) {
      const msg =
        err?.status === 403
          ? "Please verify your email first."
          : err?.data?.message || err?.error || "Login failed. Please try again.";
      dispatch(setAuthError(msg));
    }
  };

  const onGoogleSignIn = async () => {
    if (status === "loading" || isLoggingIn) return;

    dispatch(setAuthLoading());

    try {
      const cred = await signInWithPopup(auth, googleProvider);
      const email = cred.user.email;
      const name = cred.user.displayName;

      if (!email) throw new Error("Google account did not return an email");

      const { data } = await axios.post(`${API_BASE}/google_register`, {
        email,
        name,
      });

      if (!data?.token) {
        throw new Error(data?.message || "No token returned from server");
      }

      dispatch(setCredentials({ token: data.token }));

      const me = await fetch(`${API_BASE}/get_user_data`, {
        method: "GET",
        headers: { Authorization: `Bearer ${data.token}` },
      })
        .then((r) => r.json())
        .catch(() => null);

      if (me) dispatch(setUser({ ...me, is_member: false }));

      const hasMarketplace =
        typeof me?.marketplace_id === "string" &&
        me.marketplace_id.trim().length > 0;

      if (!hasMarketplace) {
        router.replace("/choose-country?onboard=1");
        return;
      }

      const countryFromBackend =
        typeof me?.country === "string" && me.country.trim().length > 0
          ? me.country.split(",")[0]
          : "global";

      routeToDashboard(countryFromBackend);
    } catch (err: any) {
      const msg =
        err?.response?.data?.message ||
        err?.message ||
        "Google sign-in failed. Please try again.";
      dispatch(setAuthError(msg));
    }
  };

  return (
    <div className="flex flex-col lg:w-1/2 w-full">
      <div className="flex flex-col justify-center flex-1 w-full xl:max-w-lg lg:mx-6 max-w-md xl:mx-auto mx-auto">
        <div>
          <div className="mb-5 2xl:mb-8">
            <h1 className="mb-2 text-green-500 text-title-sm dark:text-white/90 xl:text-title-lg lg:text-4xl sm:text-title-lg">
              Welcome!
            </h1>
            <p className="2xl:text-base text-sm text-gray-500 dark:text-gray-400">
              Please enter your login details
            </p>
          </div>

          <form onSubmit={onSubmit} noValidate>
            <div className="space-y-3">
              <div>
                <Label>
                  Email <span className="text-error-500">*</span>
                </Label>
                <Input
                  placeholder="info@gmail.com"
                  type="email"
                  value={form.email}
                  onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                    handleEmailChange(e.target.value)
                  }
                  onBlur={() => {
                    markTouched("email");
                    validateSingleField("email");
                  }}
                  required
                  autoComplete="email"
                  inputMode="email"
                  error={!!(touched.email && errors.email)}
                />
                {touched.email && errors.email && (
                  <p className="mt-1.5 text-xs text-red-500" aria-live="polite">
                    {errors.email}
                  </p>
                )}
              </div>

              <div>
                <Label>
                  Password <span className="text-error-500">*</span>
                </Label>
                <div className="relative">
                  <Input
                    type={showPassword ? "text" : "password"}
                    placeholder="Enter your password"
                    value={form.password}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) =>
                      handlePasswordChange(e.target.value)
                    }
                    onBlur={() => {
                      markTouched("password");
                      validateSingleField("password");
                    }}
                    required
                    autoComplete="current-password"
                    error={!!(touched.password && errors.password)}
                  />
                  <button
                    type="button"
                    onClick={() => setShowPassword(!showPassword)}
                    className="absolute z-30 -translate-y-1/2 right-4 top-1/2"
                    aria-label={showPassword ? "Hide password" : "Show password"}
                  >
                    {showPassword ? (
                      <EyeIcon className="fill-gray-500 dark:fill-gray-400" />
                    ) : (
                      <EyeCloseIcon className="fill-gray-500 dark:fill-gray-400" />
                    )}
                  </button>
                </div>
                {touched.password && errors.password && (
                  <p className="mt-1.5 text-xs text-red-500" aria-live="polite">
                    {errors.password}
                  </p>
                )}
              </div>

              <div className="flex items-center justify-between">
                <label className="inline-flex items-center gap-3 cursor-pointer">
                  <Checkbox checked={isChecked} onChange={setIsChecked} />
                  <span className="block font-normal text-gray-700 text-theme-sm dark:text-gray-400">
                    Keep me logged in
                  </span>
                </label>
                <button
                  type="button"
                  onClick={() => setShowForgotModal(true)}
                  className="text-theme-sm text-blue-700"
                >
                  Forgot password?
                </button>
              </div>

              {error &&
                !(status === "loading" || isLoggingIn || isMemberLoggingIn || isFallbacking) && (
                  <p className="text-sm text-red-500 -mt-2" aria-live="polite">
                    {error}
                  </p>
                )}

              <div>
                <Button
                  className="w-full"
                  size="md"
                  type="submit"
                  disabled={status === "loading" || isLoggingIn || isMemberLoggingIn || isFallbacking}
                >
                  {status === "loading" || isLoggingIn || isMemberLoggingIn
                    ? "Signing in…"
                    : "Sign in"}
                </Button>
              </div>
            </div>
          </form>

          <div className="mt-2 w-full border border-charcoal-500 rounded-lg">
            <button
              type="button"
              onClick={onGoogleSignIn}
              className="w-full inline-flex items-center justify-center gap-3 px-4 py-2.5 text-charcoal-500 rounded-lg transition-colors text-md font-bold dark:bg-white/5 dark:text-white/90 dark:hover:bg-white/10"
            >
              <svg width="20" height="20" viewBox="0 0 20 20" fill="none" aria-hidden="true">
                <path d="M18.7511 10.1944C18.7511 9.47495 18.6915 8.94995 18.5626 8.40552H10.1797V11.6527H15.1003C15.0011 12.4597 14.4654 13.675 13.2749 14.4916L13.2582 14.6003L15.9087 16.6126L16.0924 16.6305C17.7788 15.1041 18.7511 12.8583 18.7511 10.1944Z" fill="#4285F4" />
                <path d="M10.1788 18.75C12.5895 18.75 14.6133 17.9722 16.0915 16.6305L13.274 14.4916C12.5201 15.0068 11.5081 15.3666 10.1788 15.3666C7.81773 15.3666 5.81379 13.8402 5.09944 11.7305L4.99473 11.7392L2.23868 13.8295L2.20264 13.9277C3.67087 16.786 6.68674 18.75 10.1788 18.75Z" fill="#34A853" />
                <path d="M5.10014 11.7305C4.91165 11.186 4.80257 10.6027 4.80257 9.99992C4.80257 9.3971 4.91165 8.81379 5.09022 8.26935L5.08523 8.1534L2.29464 6.02954L2.20333 6.0721C1.5982 7.25823 1.25098 8.5902 1.25098 9.99992C1.25098 11.4096 1.5982 12.7415 2.20333 13.9277L5.10014 11.7305Z" fill="#FBBC05" />
                <path d="M10.1789 4.63331C11.8554 4.63331 12.9864 5.34303 13.6312 5.93612L16.1511 3.525C14.6035 2.11528 12.5895 1.25 10.1789 1.25C6.68676 1.25 3.67088 3.21387 2.20264 6.07218L5.08953 8.26943C5.81381 6.15972 7.81776 4.63331 10.1789 4.63331Z" fill="#EB4335" />
              </svg>
              Continue with Google
            </button>
          </div>

          <div className="mt-5 max-w-fit mx-auto">
            <p className="text-base font-normal text-center text-gray-700 dark:text-gray-400 sm:text-start">
              Don&apos;t have an account ?{" "}
              <Link href="/signup" className="text-blue-700">
                Sign Up
              </Link>
            </p>
          </div>

          {showForgotModal && (
            <ForgotPasswordModal onClose={() => setShowForgotModal(false)} />
          )}
        </div>
      </div>
    </div>
  );
}