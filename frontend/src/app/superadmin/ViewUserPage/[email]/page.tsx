import type { Metadata } from "next";
import ViewUserClient from "./ViewUserClient";

type ViewUserPageProps = {
  params: Promise<{
    email: string;
  }>;
};

export async function generateMetadata({
  params,
}: ViewUserPageProps): Promise<Metadata> {
  const { email } = await params;
  const decodedEmail = decodeURIComponent(email || "");

  return {
    // title: decodedEmail
    //   ? `${decodedEmail} | User Profile | Phormula Super Admin`
    //   : "User Profile | Phormula Super Admin",
    title: "User Profile | Phormula Super Admin",
    description:
      "View user details, business journey, members, marketplace settings, stock targets, and account information in Phormula.",
    robots: {
      index: false,
      follow: false,
    },
  };
}

export default function ViewUserPage() {
  return <ViewUserClient />;
}