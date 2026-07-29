import AnimatedHeroSection from "@/components/landing-page/AnimatedHeroSection";
import HowItWorks from "@/components/landing-page/HowitWorks";
import { PhormulaBeamHero } from "@/components/landing-page/PhormulaBeamHero";
import PhormulaHero from "@/components/landing-page/PhormulaHero";
import BeforeAfterSection from "@/components/landing-page/BeforeAfterSection";
import DemoSection from "@/components/landing-page/DemoSection";
import FaqSection from "@/components/landing-page/FaqSection";
import LandingFooter from "@/components/landing-page/LandingFooter";
import LandingNavbar from "@/components/landing-page/LandingNavbar";
import MetricStrip from "@/components/landing-page/MetricStrip";
import PricingSection from "@/components/landing-page/PricingSection";
import ProblemSolutionSection from "@/components/landing-page/ProblemSolutionSection";
import WorkflowSection from "@/components/landing-page/WorkflowSection";
import PlatformDashboardSection from "@/components/landing-page/PlatformDashboardSection";

export const metadata = {
  title: "Phormula — AI Financial Intelligence for D2C & Amazon Sellers",
  description: "Phormula gives D2C and Amazon founders one AI-powered dashboard for profit, cash flow, inventory, ads, fees, and financial clarity.",
};

export default function LandingPage() {
  return (
    <div className="flex min-h-screen flex-col overflow-x-clip bg-[#FAFAF7] font-(--font-dm-sans) text-[#273140] scroll-smooth">
      <LandingNavbar />
      <main className="flex-1">
        <AnimatedHeroSection />
        <PlatformDashboardSection/>
        <MetricStrip />
        <ProblemSolutionSection />
        <BeforeAfterSection />
        <HowItWorks />
        <div className=""><PhormulaBeamHero /></div>
        <WorkflowSection />
        <PhormulaHero />
        <PricingSection />
        <FaqSection />
        <DemoSection />
      </main>
      <LandingFooter />
    </div>
  );
}
