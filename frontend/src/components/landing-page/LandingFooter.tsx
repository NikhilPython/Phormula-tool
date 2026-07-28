import Image from "next/image";

const footerLinks = [
  { label: "Platform", href: "#platform" },
  { label: "Features", href: "#features" },
  { label: "Pricing", href: "#pricing" },
  { label: "Contact", href: "#demo" },
];

export default function LandingFooter() {
  return (
    <footer
      className="
        relative w-full shrink-0 overflow-hidden
        border-t border-white/8
        bg-[#080B10]!
        pt-12 text-white/[0.58]
        max-[620px]:pt-9.5
      "
    >
      {/* Background gradient */}
      <div
        aria-hidden="true"
        className="
          pointer-events-none absolute inset-0
          bg-[radial-gradient(circle_at_50%_0%,rgba(94,166,142,0.12),transparent_35%)]
        "
      />

      <div
        className="
          relative z-2 mx-auto flex
          w-[calc(100%-80px)] max-w-370
          items-center justify-between gap-6
          pb-8.5

          max-[820px]:flex-col
          max-[820px]:pb-7.5
          max-[820px]:text-center

          max-[620px]:w-[calc(100%-28px)]
        "
      >
        <Image
          className=""
          src="/images/logo/Logo_Phormula.png"
          alt="Phormula"
          width={132}
          height={36}
        />

        <nav
          aria-label="Footer navigation"
          className="
            flex flex-wrap items-center justify-center gap-7
            max-[620px]:gap-x-5.5
            max-[620px]:gap-y-4
          "
        >
          {footerLinks.map((link) => (
            <a
              key={link.href}
              href={link.href}
              className="
                text-[0.86rem] font-bold text-white/72
                transition-all duration-200
                hover:-translate-y-0.5
                hover:text-green-500
              "
            >
              {link.label}
            </a>
          ))}
        </nav>

        <p
          className="
            whitespace-nowrap text-[0.82rem] text-white/40
            max-[820px]:whitespace-normal
          "
        >
          © 2026 Phormula. All rights reserved.
        </p>
      </div>

      <div
        aria-hidden="true"
        className="
          pointer-events-none relative z-1
          mt-5 w-full select-none
          whitespace-nowrap px-5 pb-4.5
          text-center
          text-[clamp(8rem,19vw,21rem)]
          font-normal leading-[0.82]
          tracking-[-0.075em]
          text-white/7.5

          max-[820px]:text-[clamp(6.2rem,25vw,12rem)]
          max-[820px]:leading-[0.78]

          max-[620px]:mb-[-0.08em]
          max-[620px]:mt-4
          max-[620px]:text-[25vw]
          max-[620px]:tracking-[-0.065em]
        "
      >
        phormula
      </div>
    </footer>
  );
}