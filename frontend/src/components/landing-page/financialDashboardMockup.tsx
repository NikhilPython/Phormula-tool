import Image from "next/image";

export default function FinancialDashboardMockup() {
  return (
    <div className="relative w-full">
      <div className="relative overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-[0_24px_70px_rgba(31,41,55,0.16)]">
        <Image
          src="/images/grid-image/dashboard.png"
          alt="Phormula financial dashboard preview"
          width={1600}
          height={900}
          priority
          className="h-auto w-full object-cover"
        />
      </div>
    </div>
  );
}