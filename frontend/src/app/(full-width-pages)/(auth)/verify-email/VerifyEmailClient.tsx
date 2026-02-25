
"use client";

import { useSearchParams, useRouter } from "next/navigation";
import SignInForm from "@/components/auth/SignInForm";
import VerifyEmailModal from "@/components/auth/VerifyEmailModal";

export default function VerifyEmailPage() {
  const searchParams = useSearchParams();
  const router = useRouter();

  const status = searchParams.get("status") ?? "";
  const email = searchParams.get("email") ?? ""; // optional, if you add email to the link

  const handleClose = () => {
    router.push("/signin");
  };

  return (
    <>
      {/* Background: normal sign-in page */}
      <SignInForm />

      {/* Overlay: verify-email modal */}
      <VerifyEmailModal
        status={status}
        email={email}
        onClose={handleClose}
      />
    </>
  );
}
