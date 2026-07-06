import type { Metadata } from "next";
import ViewUserClient from "./ViewUserClient";

type ViewUserPageProps = {
  params: {
    email: string;
  };
};

export async function generateMetadata({
  params,
}: ViewUserPageProps): Promise<Metadata> {
  const email = decodeURIComponent(params.email || "");

  return {
    // title: email
    //   ? `${email} | User Profile | Phormula Super Admin`
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