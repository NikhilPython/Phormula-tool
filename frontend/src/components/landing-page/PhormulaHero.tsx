"use client";

import { useEffect, useState } from "react";
import styles from "./PhormulaHero.module.css";

type Tone = "blue" | "green" | "orange";

type Slide = {
  name: string;
  tone: Tone;
  href: string;
  image: string;
};

type SidebarItem = {
  label: string;
  slide: string;
};

type SidebarGroup = {
  title: "LIVE DASHBOARD" | "FINANCIAL METRICS";
  icon: "dashboard" | "metrics";
  items: SidebarItem[];
};

const slides: Slide[] = [
  {
    name: "Daily Pulse",
    tone: "blue",
    href: "#insights-showcase",
    image: "/hero/daily-pulse.png",
  },
  {
    name: "Trends",
    tone: "blue",
    href: "#insights-showcase",
    image:
      "https://vcbvkgmpzppausxwrljp.supabase.co/storage/v1/object/public/brand-images/landing%20page%20image/business%20trends%20landing%20page%20image.png",
  },
  {
    name: "Brand Intel",
    tone: "blue",
    href: "#insights-showcase",
    image:
      "https://vcbvkgmpzppausxwrljp.supabase.co/storage/v1/object/public/brand-images/landing%20page%20image/brand%20%26%20search%20landing%20page%20image.png",
  },
  {
    name: "Advertising",
    tone: "green",
    href: "#growth-showcase",
    image: "/hero/advertising.png",
  },
  {
    name: "Keywords",
    tone: "green",
    href: "#growth-showcase",
    image: "/hero/keywords.png",
  },
  {
    name: "Inventory",
    tone: "orange",
    href: "#features",
    image: "/hero/inventory.png",
  },
  {
    name: "Reviews & Returns",
    tone: "orange",
    href: "#features",
    image: "/hero/reviews-returns.png",
  },
  {
    name: "Catalog",
    tone: "orange",
    href: "#features",
    image: "/hero/catalog.png",
  },
];

const sidebarGroups: SidebarGroup[] = [
  {
    title: "LIVE DASHBOARD",
    icon: "dashboard",
    items: [
      { label: "AI Insights", slide: "Daily Pulse" },
      { label: "MTD Sales", slide: "Trends" },
      { label: "P&L Breakdown", slide: "Brand Intel" },
      { label: "Inventory Insights", slide: "Inventory" },
    ],
  },
  {
    title: "FINANCIAL METRICS",
    icon: "metrics",
    items: [
      { label: "AI Insights", slide: "Daily Pulse" },
      { label: "Finance Dashboard", slide: "Advertising" },
      { label: "P&L Breakdown", slide: "Brand Intel" },
      { label: "Cash Flow", slide: "Keywords" },
      { label: "SKU Journey", slide: "Catalog" },
      { label: "Inventory Insights", slide: "Reviews & Returns" },
    ],
  },
];

const pills = [
  ["Phormula AI Assistant", "#lucy-ai", "violet"],
  ["Keywords", "#growth-showcase", "green"],
  ["Profit Intel", "#insights-showcase", "blue"],
  ["Ad Efficiency", "#growth-showcase", "green"],
  ["Inventory", "#features", "orange"],
] as const;

function SidebarGroupIcon({
  name,
}: {
  name: "dashboard" | "metrics";
}) {
  const props = {
    width: 17,
    height: 17,
    viewBox: "0 0 24 24",
    fill: "none",
    stroke: "currentColor",
    strokeWidth: 1.8,
    strokeLinecap: "round" as const,
    strokeLinejoin: "round" as const,
    "aria-hidden": true,
  };

  if (name === "dashboard") {
    return (
      <svg {...props}>
        <path d="M4 20V10" />
        <path d="M10 20V4" />
        <path d="M16 20v-7" />
        <path d="M22 20H2" />
        <path d="m14 7 3-3 3 3" />
        <path d="M17 4v8" />
      </svg>
    );
  }

  return (
    <svg {...props}>
      <rect x="3" y="3" width="7" height="7" rx="1" />
      <rect x="14" y="3" width="7" height="7" rx="1" />
      <rect x="3" y="14" width="7" height="7" rx="1" />
      <rect x="14" y="14" width="7" height="7" rx="1" />
    </svg>
  );
}

function ChevronIcon({ open }: { open: boolean }) {
  return (
    <svg
      width="14"
      height="14"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="2"
      strokeLinecap="round"
      strokeLinejoin="round"
      className={open ? styles.chevronOpen : styles.chevron}
      aria-hidden="true"
    >
      <path d="m6 9 6 6 6-6" />
    </svg>
  );
}

function LocationIcon() {
  return (
    <svg
      width="17"
      height="17"
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <path d="M20 10c0 5-8 11-8 11S4 15 4 10a8 8 0 1 1 16 0Z" />
      <circle cx="12" cy="10" r="2.5" />
    </svg>
  );
}

export default function PhormulaHero() {
  const [index, setIndex] = useState(1);
  const [paused, setPaused] = useState(false);
  const [failed, setFailed] = useState(false);
  const [openSections, setOpenSections] = useState({
    "LIVE DASHBOARD": true,
    "FINANCIAL METRICS": true,
  });

  const active = slides[index];

  useEffect(() => {
    setFailed(false);
  }, [index]);

  useEffect(() => {
    if (paused) return;
    const timer = window.setInterval(() => {
      setIndex((current) => (current + 1) % slides.length);
    }, 4200);
    return () => window.clearInterval(timer);
  }, [paused]);

  const pick = (name: string) => {
    const next = slides.findIndex((slide) => slide.name === name);

    if (next >= 0) {
      setIndex(next);
    }
  };

  const toggleSection = (title: keyof typeof openSections) => {
    setOpenSections((current) => ({
      ...current,
      [title]: !current[title],
    }));
  };

  return (
    <section className={styles.hero}>
      <div className={styles.background} aria-hidden="true">
        <span />
        <span />
        <span />
      </div>

      <div className={styles.vignette} aria-hidden="true" />

      <div className={styles.content}>
        <div className={`${styles.enter} ${styles.d1}`}>
          <span className={styles.badge}>
            <i />
            Built by 8-figure Amazon sellers, for Amazon operators
          </span>
        </div>

        <h1 className={`${styles.title} ${styles.enter} ${styles.d2}`}>
          Run your Amazon business with{" "}
          <span>AI built on your real data</span>
        </h1>

        <p className={`${styles.copy} ${styles.enter} ${styles.d3}`}>
          Phormula brings your sales, ads, search, inventory, and customer data
          into one system, then helps your team make faster decisions without
          digging through Seller Central, spreadsheets, and disconnected tools.
        </p>

        <div className={`${styles.pills} ${styles.enter} ${styles.d4}`}>
          {pills.map(([label, href, tone]) => (
            <a
              key={label}
              href={href}
              className={`${styles.pill} ${styles[tone]}`}
            >
              <i />
              {label}
            </a>
          ))}
        </div>

        <div
          className={`${styles.showcase} ${styles.enter} ${styles.d6}`}
          onMouseEnter={() => setPaused(true)}
          onMouseLeave={() => setPaused(false)}
          onFocusCapture={() => setPaused(true)}
          onBlurCapture={() => setPaused(false)}
        >
          <div className={styles.showcaseTop}>
            <a
              href={active.href}
              className={`${styles.slideBadge} ${styles[active.tone]}`}
            >
              <i />
              {active.name}
            </a>

            <div
              className={styles.tabs}
              role="tablist"
              aria-label="Hero preview slides"
            >
              {slides.map((slide, slideIndex) => {
                const selected = slideIndex === index;

                return (
                  <button
                    key={slide.name}
                    type="button"
                    role="tab"
                    aria-selected={selected}
                    aria-label={`Show ${slide.name}`}
                    onClick={() => setIndex(slideIndex)}
                    className={selected ? styles.tabActive : styles.tab}
                  >
                    <span
                      className={
                        selected
                          ? `${styles.tabDotActive} ${styles[slide.tone]}`
                          : styles.tabDot
                      }
                    />
                  </button>
                );
              })}
            </div>
          </div>

          <div className={styles.browser}>
            <div className={styles.browserBar}>
              <div className={styles.lights} aria-hidden="true">
                <i />
                <i />
                <i />
              </div>

              <div className={styles.address}>phormula.io</div>
            </div>

            <div className={styles.app}>
              <aside className={styles.sidebar}>
                <div className={styles.sidebarHeader}>
                  <div className={styles.phormulaLogo}>
                    <span className={styles.logoLeftLine} />
                    <span className={styles.logoText}>phormula</span>
                    <sup>∞</sup>
                    <span className={styles.logoRightLine} />
                  </div>

                  <button
                    type="button"
                    className={styles.menuButton}
                    aria-label="Open navigation menu"
                  >
                    <span />
                    <span />
                    <span />
                  </button>
                </div>

                <div className={styles.platformArea}>
                  <div className={styles.platformLabel}>
                    <LocationIcon />
                    <span>Platform</span>
                  </div>

                  <div className={styles.marketplaceSelect}>
                    <span>Amazon US</span>

                    <svg
                      width="13"
                      height="13"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      aria-hidden="true"
                    >
                      <path d="m6 9 6 6 6-6" />
                    </svg>
                  </div>
                </div>

                <nav className={styles.sidebarNavigation}>
                  {sidebarGroups.map((group) => {
                    const isOpen = openSections[group.title];

                    return (
                      <div className={styles.sidebarGroup} key={group.title}>
                        <button
                          type="button"
                          className={styles.sidebarGroupHeader}
                          onClick={() => toggleSection(group.title)}
                          aria-expanded={isOpen}
                        >
                          <span className={styles.sidebarGroupTitle}>
                            <SidebarGroupIcon name={group.icon} />
                            <span>{group.title}</span>
                          </span>

                          <ChevronIcon open={isOpen} />
                        </button>

                        <div
                          className={`${styles.sidebarItems} ${
                            isOpen ? styles.sidebarItemsOpen : ""
                          }`}
                        >
                          <div className={styles.sidebarItemsInner}>
                            {group.items.map((item, itemIndex) => {
                              const selected = item.slide === active.name;

                              return (
                                <button
                                  key={`${group.title}-${item.label}-${itemIndex}`}
                                  type="button"
                                  onClick={() => pick(item.slide)}
                                  className={`${styles.sidebarItem} ${
                                    selected
                                      ? styles.sidebarItemActive
                                      : ""
                                  }`}
                                >
                                  {item.label}
                                </button>
                              );
                            })}
                          </div>
                        </div>
                      </div>
                    );
                  })}
                </nav>
              </aside>

              <main className={styles.preview}>
                <div key={active.name} className={styles.slide}>
                  {!failed ? (
                    <img
                      src={active.image}
                      alt={`${active.name} dashboard`}
                      onError={() => setFailed(true)}
                    />
                  ) : (
                    <div
                      className={`${styles.fallback} ${
                        styles[active.tone]
                      }`}
                    >
                      <span>{active.name}</span>
                      <b>Dashboard preview</b>
                      <small>Add this image inside public/hero.</small>
                    </div>
                  )}
                </div>
              </main>
            </div>

            <div className={styles.progress} aria-hidden="true">
              <span
                key={`${index}-${paused}`}
                className={`${styles.progressFill} ${
                  styles[active.tone]
                } ${paused ? styles.pause : ""}`}
              />
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}
