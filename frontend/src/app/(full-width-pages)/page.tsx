import { DM_Sans, DM_Serif_Display } from "next/font/google";
import LandingPage from "./LandingPage";

const dmSans = DM_Sans({
  subsets: ["latin"],
  weight: ["300", "400", "500", "600", "700", "800"],
  variable: "--font-dm-sans",
});

const dmSerif = DM_Serif_Display({
  subsets: ["latin"],
  weight: ["400"],
  style: ["normal", "italic"],
  variable: "--font-dm-serif",
});

export const metadata = {
  title: "Phormula — AI Financial Intelligence for D2C & Amazon Sellers",
  description:
    "Phormula gives D2C and Amazon founders one AI-powered dashboard for profit, cash flow, inventory, ads, fees, and financial clarity.",
};

export default function Page() {
  return (
    <div className={`${dmSans.variable} ${dmSerif.variable}`}>
      <LandingPage />
    </div>
  );
}