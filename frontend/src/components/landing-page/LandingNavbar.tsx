"use client";

import { useEffect, useState } from "react";
import Image from "next/image";
import Link from "next/link";
import { FiMenu, FiX } from "react-icons/fi";
import { containerClass } from "./shared";

const navItems = [
  ["Platform", "#platform"],
  ["Features", "#features"],
  ["How it works", "#workflow"],
  ["Pricing", "#pricing"],
  ["FAQ", "#faq"],
] as const;

export default function LandingNavbar() {
  const [isMenuOpen, setIsMenuOpen] = useState(false);

  const closeMenu = () => {
    setIsMenuOpen(false);
  };

  useEffect(() => {
    const handleResize = () => {
      if (window.innerWidth >= 1024) {
        setIsMenuOpen(false);
      }
    };

    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
    };
  }, []);

  useEffect(() => {
    document.body.style.overflow = isMenuOpen ? "hidden" : "";

    return () => {
      document.body.style.overflow = "";
    };
  }, [isMenuOpen]);

  return (
    <header
      className="
        fixed inset-x-0 top-0 z-[100]
        h-[76px]
        border-b border-[#37455F]/[0.08]
        bg-[#FAFAF7]/[0.92]
        backdrop-blur-[18px]

        lg:h-[60px]
        min-[1700px]:h-[76px]
      "
    >
      <div
        className={`
          ${containerClass}
          relative flex h-full items-center justify-between
        `}
      >
        {/* Mobile and tablet logo - left */}
        <Link
          href="/"
          onClick={closeMenu}
          aria-label="Phormula home"
          className="flex shrink-0 items-center lg:hidden"
        >
          <Image
            src="/images/logo/Logo_Phormula.png"
            alt="Phormula"
            width={132}
            height={36}
            priority
            className="
              h-auto w-[118px] object-contain
              sm:w-[128px]
            "
          />
        </Link>

        {/* Desktop logo */}
        <Link
          href="/"
          aria-label="Phormula home"
          className="hidden shrink-0 items-center lg:flex"
        >
          <Image
            src="/images/logo/Logo_Phormula.png"
            alt="Phormula"
            width={132}
            height={36}
            priority
            className="h-auto w-[162px] dark:hidden"
          />
        </Link>

        {/* Desktop navigation */}
        <nav className="hidden lg:block">
          <ul className="flex list-none items-center gap-7">
            {navItems.map(([label, href]) => (
              <li key={href}>
                <a
                  href={href}
                  className="
                    text-xs font-bold text-[#5A6272]
                    transition-colors hover:text-[#4A8A74]
                    min-[1700px]:text-sm
                  "
                >
                  {label}
                </a>
              </li>
            ))}
          </ul>
        </nav>

        {/* Desktop right actions */}
        <div className="hidden shrink-0 items-center gap-3 lg:flex">
          <Link
            href="#demo"
            className="
              inline-flex items-center justify-center
              whitespace-nowrap rounded-xl
              border-[1.5px] border-[#37455F]/20
              bg-white/70 px-4 py-2.5
              text-xs font-extrabold text-[#37455F]
              transition

              hover:-translate-y-0.5
              hover:border-[#5EA68E]
              hover:text-[#4A8A74]

              min-[1700px]:text-sm
            "
          >
            Book demo
          </Link>

          <Link
            href="/signin"
            className="
              inline-flex items-center justify-center
              whitespace-nowrap rounded-xl
              bg-[#5EA68E] px-4 py-2.5
              text-xs font-extrabold text-[#F8EDCE]
              shadow-[0_10px_28px_rgba(94,166,142,0.28)]
              transition

              hover:-translate-y-0.5
              hover:bg-[#4A8A74]

              min-[1700px]:text-sm
            "
          >
            Get started
          </Link>
        </div>

        {/* Mobile and tablet hamburger - right */}
        <button
          type="button"
          onClick={() => setIsMenuOpen((previous) => !previous)}
          aria-label={
            isMenuOpen
              ? "Close navigation menu"
              : "Open navigation menu"
          }
          aria-expanded={isMenuOpen}
          className="
            inline-flex h-10 w-10 shrink-0
            items-center justify-center
            rounded-xl
            border border-[#37455F]/15
            bg-white/70
            text-[#37455F]
            transition

            hover:border-[#5EA68E]
            hover:text-[#4A8A74]

            lg:hidden
          "
        >
          {isMenuOpen ? (
            <FiX className="h-5 w-5" />
          ) : (
            <FiMenu className="h-5 w-5" />
          )}
        </button>
      </div>

      {/* Mobile and tablet menu */}
      <div
        className={`
          absolute inset-x-0 top-[76px]
          border-b border-[#37455F]/10
          bg-[#FAFAF7]
          shadow-[0_22px_50px_rgba(55,69,95,0.12)]
          transition-all duration-300
          lg:hidden

          ${
            isMenuOpen
              ? "visible translate-y-0 opacity-100"
              : "invisible -translate-y-3 opacity-0"
          }
        `}
      >
        <nav className={`${containerClass} py-5`}>
          <ul className="flex flex-col gap-1">
            {navItems.map(([label, href]) => (
              <li key={href}>
                <a
                  href={href}
                  onClick={closeMenu}
                  className="
                    flex w-full items-center
                    rounded-xl px-4 py-3
                    text-sm font-bold text-[#5A6272]
                    transition-colors

                    hover:bg-[#5EA68E]/10
                    hover:text-[#4A8A74]
                  "
                >
                  {label}
                </a>
              </li>
            ))}
          </ul>

          <div
            className="
              mt-4 grid grid-cols-2 gap-3
              border-t border-[#37455F]/10
              pt-4
              max-[420px]:grid-cols-1
            "
          >
            <Link
              href="/signin"
              onClick={closeMenu}
              className="
                inline-flex items-center justify-center
                rounded-xl
                border-[1.5px] border-[#37455F]/20
                bg-white px-4 py-3
                text-sm font-extrabold text-[#37455F]
                transition

                hover:border-[#5EA68E]
                hover:text-[#4A8A74]
              "
            >
              Book demo
            </Link>

            <Link
              href="/signin"
              onClick={closeMenu}
              className="
                inline-flex items-center justify-center
                rounded-xl
                bg-[#5EA68E] px-4 py-3
                text-sm font-extrabold text-[#F8EDCE]
                transition

                hover:bg-[#4A8A74]
              "
            >
              Get started
            </Link>
          </div>
        </nav>
      </div>

      {/* Mobile menu backdrop */}
      {isMenuOpen && (
        <button
          type="button"
          aria-label="Close navigation menu"
          onClick={closeMenu}
          className="
            fixed inset-x-0 top-[76px] -z-10
            h-[calc(100vh-76px)]
            bg-[#273140]/20
            backdrop-blur-[2px]
            lg:hidden
          "
        />
      )}
    </header>
  );
}