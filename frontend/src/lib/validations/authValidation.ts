import { z } from "zod";

export const NATIONAL_LENGTH_BY_DIAL: Record<string, number[]> = {
  "91": [10],
  "1": [10],
  "44": [10, 11],
  "971": [9],
  "92": [10],
  "880": [10],
  "94": [9],
  "977": [10],
};

export const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/i;
export const nameRegex = /^[a-zA-Z\s]+$/;
export const digitsOnlyRegex = /^\d+$/;

export const sanitizeName = (value: string) => value.replace(/[^a-zA-Z\s]/g, "");
export const sanitizeEmail = (value: string) => value.replace(/\s/g, "");
export const sanitizePhone = (value: string) => value.replace(/\D/g, "");
export const sanitizePassword = (value: string) => value.replace(/\s/g, "");

export const limitPhoneByDialCode = (digits: string, dialCode: string) => {
  const allowedLens = NATIONAL_LENGTH_BY_DIAL[dialCode];
  const maxLen = allowedLens?.length ? Math.max(...allowedLens) : 14;
  return digits.slice(0, maxLen);
};

/* -------------------- SIGN UP -------------------- */

export const signUpSchema = z
  .object({
    name: z
      .string()
      .trim()
      .min(1, "Name is required")
      .min(2, "Name must be at least 2 characters")
      .regex(nameRegex, "Name can contain only letters and spaces"),

    email: z
      .string()
      .trim()
      .min(1, "Email is required")
      .regex(emailRegex, "Enter a valid email"),

    phoneRaw: z
      .string()
      .min(1, "Phone number is required")
      .regex(digitsOnlyRegex, "Only numbers are allowed"),

    phoneDialCode: z.string().min(1, "Select a country code"),

    password: z
      .string()
      .min(1, "Password is required")
      .min(6, "At least 6 characters")
      .refine((val) => (val.match(/\d/g) || []).length >= 2, {
        message: "At least 2 numbers",
      })
      .refine((val) => (val.match(/[a-zA-Z]/g) || []).length >= 2, {
        message: "At least 2 alphabets",
      }),

    confirm: z.string().min(1, "Confirm password is required"),

    isChecked: z.boolean(),
  })
  .superRefine((data, ctx) => {
    const allowedLens = NATIONAL_LENGTH_BY_DIAL[data.phoneDialCode];

    if (allowedLens) {
      if (!allowedLens.includes(data.phoneRaw.length)) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["phoneRaw"],
          message: `Invalid number length for +${data.phoneDialCode} (need ${allowedLens.join(" or ")} digits)`,
        });
      }
    } else {
      if (data.phoneRaw.length < 6) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["phoneRaw"],
          message: "Phone number is too short",
        });
      }

      if (data.phoneRaw.length > 14) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["phoneRaw"],
          message: "Phone number is too long",
        });
      }
    }

    if (data.confirm !== data.password) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["confirm"],
        message: "Passwords do not match",
      });
    }

    if (!data.isChecked) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        path: ["isChecked"],
        message: "You must accept Terms and Conditions",
      });
    }
  });

  export const getZodFieldErrors = (
  error: z.ZodError<SignUpFormValues>
): SignUpFormErrors => {
  const fieldErrors = error.flatten().fieldErrors;

  return {
    name: fieldErrors.name?.[0],
    email: fieldErrors.email?.[0],
    phoneRaw: fieldErrors.phoneRaw?.[0] || fieldErrors.phoneDialCode?.[0],
    phoneDialCode: fieldErrors.phoneDialCode?.[0],
    password: fieldErrors.password?.[0],
    confirm: fieldErrors.confirm?.[0],
    isChecked: fieldErrors.isChecked?.[0],
  };
};

export type SignUpFormValues = z.infer<typeof signUpSchema>;
export type SignUpFormErrors = Partial<Record<keyof SignUpFormValues, string>>;

export const getSignUpFieldErrors = (
  error: z.ZodError<SignUpFormValues>
): SignUpFormErrors => {
  const fieldErrors = error.flatten().fieldErrors;

  return {
    name: fieldErrors.name?.[0],
    email: fieldErrors.email?.[0],
    phoneRaw: fieldErrors.phoneRaw?.[0] || fieldErrors.phoneDialCode?.[0],
    phoneDialCode: fieldErrors.phoneDialCode?.[0],
    password: fieldErrors.password?.[0],
    confirm: fieldErrors.confirm?.[0],
    isChecked: fieldErrors.isChecked?.[0],
  };
};

/* -------------------- SIGN IN -------------------- */

export const signInSchema = z.object({
  email: z
    .string()
    .trim()
    .min(1, "Email is required")
    .regex(emailRegex, "Enter a valid email"),

  password: z
    .string()
    .min(1, "Password is required"),
});

export type SignInFormValues = z.infer<typeof signInSchema>;
export type SignInFormErrors = Partial<Record<keyof SignInFormValues, string>>;

export const getSignInFieldErrors = (
  error: z.ZodError<SignInFormValues>
): SignInFormErrors => {
  const fieldErrors = error.flatten().fieldErrors;

  return {
    email: fieldErrors.email?.[0],
    password: fieldErrors.password?.[0],
  };
};


/* -------------------- FORGOT PASSWORD -------------------- */

export const forgotPasswordSchema = z.object({
  email: z
    .string()
    .trim()
    .min(1, "Email is required")
    .regex(emailRegex, "Enter a valid email"),
});

export type ForgotPasswordFormValues = z.infer<typeof forgotPasswordSchema>;
export type ForgotPasswordFormErrors = Partial<
  Record<keyof ForgotPasswordFormValues, string>
>;

export const getForgotPasswordFieldErrors = (
  error: z.ZodError<ForgotPasswordFormValues>
): ForgotPasswordFormErrors => {
  const fieldErrors = error.flatten().fieldErrors;

  return {
    email: fieldErrors.email?.[0],
  };
};


/* -------------------- PROFILE -------------------- */

export const phoneRegex = /^[0-9+\-\s()]{6,20}$/;
export const gstRegex =
  /^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z]{1}[A-Z0-9]{1}Z[A-Z0-9]{1}$/i;
export const panRegex = /^[A-Z]{5}[0-9]{4}[A-Z]{1}$/i;
export const zipcodeRegex = /^[A-Za-z0-9\- ]{3,12}$/;

export const sanitizePhoneLoose = (value: string) =>
  value.replace(/[^0-9+\-\s()]/g, "");

export const sanitizeAlphaSpace = (value: string) =>
  value.replace(/[^a-zA-Z\s]/g, "");

export const sanitizeAlphaNumSpace = (value: string) =>
  value.replace(/[^a-zA-Z0-9\s&.,\-()/]/g, "");

export const sanitizeUpperAlphaNum = (value: string) =>
  value.toUpperCase().replace(/[^A-Z0-9]/g, "");

export const personalInfoSchema = z.object({
  name: z
    .string()
    .trim()
    .min(1, "Name is required")
    .min(2, "Name must be at least 2 characters")
    .regex(nameRegex, "Name can contain only letters and spaces"),

  phone_number: z
    .string()
    .trim()
    .min(1, "Phone number is required")
    .regex(phoneRegex, "Enter a valid phone number"),
});

export type PersonalInfoFormValues = z.infer<typeof personalInfoSchema>;
export type PersonalInfoFormErrors = Partial<
  Record<keyof PersonalInfoFormValues, string>
>;

export const getPersonalInfoFieldErrors = (
  error: z.ZodError<PersonalInfoFormValues>
): PersonalInfoFormErrors => {
  const fieldErrors = error.flatten().fieldErrors;

  return {
    name: fieldErrors.name?.[0],
    phone_number: fieldErrors.phone_number?.[0],
  };
};

export const companyInfoSchema = z.object({
  brand_name: z
    .string()
    .trim()
    .min(1, "Brand name is required")
    .min(2, "Brand name must be at least 2 characters"),

  company_name: z
    .string()
    .trim()
    .min(1, "Company name is required")
    .min(2, "Company name must be at least 2 characters"),

  annual_sales_range: z
    .string()
    .trim()
    .min(1, "Revenue is required"),

  homeCurrency: z
    .string()
    .trim()
    .min(1, "Home currency is required"),

  gst_no: z
    .string()
    .trim()
    .optional()
    .or(z.literal(""))
    .refine((val) => !val || gstRegex.test(val), {
      message: "Enter a valid GST number",
    }),

  pan_no: z
    .string()
    .trim()
    .optional()
    .or(z.literal(""))
    .refine((val) => !val || panRegex.test(val), {
      message: "Enter a valid PAN number",
    }),

  address_building: z
    .string()
    .trim()
    .min(1, "Building address is required"),

  address_city: z
    .string()
    .trim()
    .min(1, "City is required"),

  address_country: z
    .string()
    .trim()
    .min(1, "Country is required"),

  address_state: z
    .string()
    .trim()
    .min(1, "State is required"),

  address_zipcode: z
    .string()
    .trim()
    .min(1, "Zipcode is required")
    .regex(zipcodeRegex, "Enter a valid zipcode"),
});

export type CompanyInfoFormValues = z.infer<typeof companyInfoSchema>;
export type CompanyInfoFormErrors = Partial<
  Record<keyof CompanyInfoFormValues, string>
>;

export const getCompanyInfoFieldErrors = (
  error: z.ZodError<CompanyInfoFormValues>
): CompanyInfoFormErrors => {
  const fieldErrors = error.flatten().fieldErrors;

  return {
    brand_name: fieldErrors.brand_name?.[0],
    company_name: fieldErrors.company_name?.[0],
    annual_sales_range: fieldErrors.annual_sales_range?.[0],
    homeCurrency: fieldErrors.homeCurrency?.[0],
    gst_no: fieldErrors.gst_no?.[0],
    pan_no: fieldErrors.pan_no?.[0],
    address_building: fieldErrors.address_building?.[0],
    address_city: fieldErrors.address_city?.[0],
    address_country: fieldErrors.address_country?.[0],
    address_state: fieldErrors.address_state?.[0],
    address_zipcode: fieldErrors.address_zipcode?.[0],
  };
};