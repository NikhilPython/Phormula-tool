import React from "react";
import FinancialDashboardMockup from "@/components/landing-page/financialDashboardMockup";
import HowItWorks from "@/components/landing-page/HowitWorks";
import { PhormulaBeamHero } from "@/components/landing-page/PhormulaBeamHero";
import Services from "@/components/landing-page/Services";
import Image from "next/image";
import FaqAccordion from "@/components/landing-page/FaqAccordion";
import Link from "next/link";
import AnimatedHeroSection from "@/components/landing-page/AnimatedHeroSection";

export const metadata = {
  title: "Phormula — AI Financial Intelligence for D2C & Amazon Sellers",
  description:
    "Phormula gives D2C and Amazon founders one AI-powered dashboard for profit, cash flow, inventory, ads, fees, and financial clarity.",
};

export default function LandingPage() {
  return (
    <div className="landing-root">
      <header className="nav">
        <div className="container nav-inner">
          <Image
            className="dark:hidden hidden lg:block"
            src="/images/logo/Logo_Phormula.png"
            alt="Logo"
            width={132}
            height={36}
          />

          <ul className="nav-links">
            <li><a href="#platform">Platform</a></li>
            <li><a href="#features">Features</a></li>
            <li><a href="#workflow">How it works</a></li>
            <li><a href="#pricing">Pricing</a></li>
            <li><a href="#faq">FAQ</a></li>
          </ul>

          <div className="nav-actions">
            <Link className="btn btn-outline" href="/signin">
              Book demo
            </Link>

            <Link className="btn btn-teal !text-yellow-200" href="/signin">
              Get started
            </Link>
          </div>
        </div>
      </header>

      <main>
        {/*HERO*/}
        {/* <section className="hero">
          <div className="container hero-grid">
            <div>
              <div className="eyebrow">AI Financial Intelligence</div>

              <h1 className='!text-7xl'>
                Empowering D2C Founders with{" "}
                <span className="headline-word-rotator !text-5xl">
                  <span>Financial Intelligence.</span>
                  <span>Profit Clarity.</span>
                  <span>Cashflow Control.</span>
                  <span>Inventory Forecasting.</span>
                </span>
              </h1>

              <p className="text-base">
                AI-powered financial clarity that helps founders lead, not just read reports.
              </p>

              <div className="!mt-3">
                <a href="#demo" className="btn btn-primary !text-yellow-200">
                  Book a demo
                </a>
              </div>

              <p className="hero-note">
                Built for operators tired of spreadsheet chaos, delayed reports, and unclear margins.
              </p>
            </div>

            <div className="mockup-wrap">
              <div className="w-full max-w-[920px] scale-[0.95] origin-center">
                <FinancialDashboardMockup />
              </div>
            </div>
          </div>
        </section> */}
        <div>
          <AnimatedHeroSection/>
        </div>

        {/*METRIC STRIP*/}
        <div className="metric-strip">
          <div className="container">
            <div className="metric-strip-grid">
              <div className="strip-item">
                <div className="strip-icon">◎</div>
                <div>
                  <div className="strip-value">1</div>
                  <div className="strip-label">Source of truth</div>
                </div>
              </div>

              <div className="strip-item">
                <div className="strip-icon">↗</div>
                <div>
                  <div className="strip-value">24/7</div>
                  <div className="strip-label">Live financial visibility</div>
                </div>
              </div>

              <div className="strip-item">
                <div className="strip-icon">◷</div>
                <div>
                  <div className="strip-value">CM2</div>
                  <div className="strip-label">Profit-level clarity</div>
                </div>
              </div>

              <div className="strip-item">
                <div className="strip-icon">✦</div>
                <div>
                  <div className="strip-value">∞</div>
                  <div className="strip-label">AI-powered insights</div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/*PROBLEM*/}
       <section id="platform" className="container center">
  <div className="problem-container">
    <div className="problem-header">
      <div className="problem-eyebrow">
        <span className="eyebrow-dot" />
        The Problem
      </div>

      <h2 className="problem-heading">
        From scattered sheets to
        <span> one source of truth</span>
      </h2>

      <p className="problem-description">
        Phormula replaces fragmented reporting and manual follow-ups with one
        live, intelligent workspace for your business.
      </p>
    </div>

    <div className="comparison-labels">
      <div className="comparison-label comparison-label-problem">
        The Problem
      </div>

      <div className="comparison-label comparison-label-solution">
        The Solution
      </div>
    </div>

    <div className="problem-flow">
      {/* Row 1 */}
      <article className="problem-flow-card problem-side-card">
        <div className="problem-card-icon problem-card-icon-red">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            aria-hidden="true"
          >
            <path
              d="M7 3.75h7.5L19 8.25v12H7v-16.5Z"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinejoin="round"
            />
            <path
              d="M14.5 3.75v4.5H19M10 12h6M10 15h6"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
            />
          </svg>
        </div>

        <div className="problem-card-content">
          <span className="problem-card-index">01</span>
          <h3>Scattered Sheets</h3>
          <span className="problem-card-accent problem-card-accent-red" />

          <p>
            Sales, fees, inventory, ads and expenses live across disconnected
            files and reports.
          </p>
        </div>
      </article>

      <div className="problem-connector" aria-hidden="true">
        <span className="connector-line" />
        <span className="connector-arrow">
          <svg viewBox="0 0 24 24" fill="none">
            <path
              d="M5 12h13M14 8l4 4-4 4"
              stroke="currentColor"
              strokeWidth="1.8"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </span>
      </div>

      <article className="problem-flow-card solution-side-card">
        <div className="problem-card-icon problem-card-icon-green">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            aria-hidden="true"
          >
            <rect
              x="3.5"
              y="4.5"
              width="17"
              height="11"
              rx="1.5"
              stroke="currentColor"
              strokeWidth="1.5"
            />
            <path
              d="M8 19.5h8M12 15.5v4"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
            />
          </svg>
        </div>

        <div className="problem-card-content">
          <span className="problem-card-index">01</span>
          <h3>One Live Dashboard</h3>
          <span className="problem-card-accent problem-card-accent-green" />

          <p>
            Finance, inventory, advertising and leadership stay aligned inside
            one centralized workspace.
          </p>
        </div>
      </article>

      {/* Row 2 */}
      <article className="problem-flow-card problem-side-card">
        <div className="problem-card-icon problem-card-icon-red">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            aria-hidden="true"
          >
            <circle
              cx="12"
              cy="13"
              r="7"
              stroke="currentColor"
              strokeWidth="1.5"
            />
            <path
              d="M12 9v4l2.5 1.5M9 3h6M12 3v3"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </div>

        <div className="problem-card-content">
          <span className="problem-card-index">02</span>
          <h3>Delayed Decisions</h3>
          <span className="problem-card-accent problem-card-accent-red" />

          <p>
            By the time reports are cleaned, the opportunity to protect margin
            may already be gone.
          </p>
        </div>
      </article>

      <div className="problem-connector" aria-hidden="true">
        <span className="connector-line" />
        <span className="connector-arrow">
          <svg viewBox="0 0 24 24" fill="none">
            <path
              d="M5 12h13M14 8l4 4-4 4"
              stroke="currentColor"
              strokeWidth="1.8"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </span>
      </div>

      <article className="problem-flow-card solution-side-card">
        <div className="problem-card-icon problem-card-icon-green">
          <svg
            viewBox="0 0 24 24"
            fill="none"
            aria-hidden="true"
          >
            <path
              d="M9 18h6M10 21h4"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
            />
            <path
              d="M8.2 14.5A6 6 0 1 1 15.8 14.5c-.95.7-1.55 1.48-1.8 2.5h-4c-.25-1.02-.85-1.8-1.8-2.5Z"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinejoin="round"
            />
            <path
              d="m10 11 1.3 1.3L14.5 9"
              stroke="currentColor"
              strokeWidth="1.5"
              strokeLinecap="round"
              strokeLinejoin="round"
            />
          </svg>
        </div>

        <div className="problem-card-content">
          <span className="problem-card-index">02</span>
          <h3>Actionable Insights</h3>
          <span className="problem-card-accent problem-card-accent-green" />

          <p>
            AI explains what changed, why it matters and what action can improve
            performance.
          </p>
        </div>
      </article>
    </div>

    <div className="problem-bottom-note">
      <span className="bottom-note-icon">
        <svg viewBox="0 0 24 24" fill="none">
          <path
            d="m12 3 1.25 4.25L17.5 8.5l-4.25 1.25L12 14l-1.25-4.25L6.5 8.5l4.25-1.25L12 3Z"
            fill="currentColor"
          />
          <path
            d="m18.5 14 .7 2.3 2.3.7-2.3.7-.7 2.3-.7-2.3-2.3-.7 2.3-.7.7-2.3Z"
            fill="currentColor"
          />
        </svg>
      </span>

      <p>
        Phormula turns operational chaos into clarity, so every decision is
        faster and better informed.
      </p>
    </div>
  </div>
</section>


        {/*BEFORE / AFTER*/}
        <section id="before-after">
          <div className="container center">
            <div className="eyebrow">The difference</div>
            <h2 className="section-heading">Move From Manual Chaos to Clear Business Control</h2>

            <div className="ba-grid">
              {/*Before*/}
              <div className="ba-card before">
                <div className="ba-heading">
                  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="rgba(255,255,255,0.3)" strokeWidth="2">
                    <circle cx="12" cy="12" r="10" />
                    <line x1="15" y1="9" x2="9" y2="15" />
                    <line x1="9" y1="9" x2="15" y2="15" />
                  </svg>
                  Before Phormula
                </div>

                <ul className="ba-list">
                  <li>
                    <span className="ba-icon">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <line x1="18" y1="6" x2="6" y2="18" />
                        <line x1="6" y1="6" x2="18" y2="18" />
                      </svg>
                    </span>
                    Manual spreadsheet maintenance
                  </li>
                  <li>
                    <span className="ba-icon">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <line x1="18" y1="6" x2="6" y2="18" />
                        <line x1="6" y1="6" x2="18" y2="18" />
                      </svg>
                    </span>
                    Confusion in inventory dispatch
                  </li>
                  <li>
                    <span className="ba-icon">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <line x1="18" y1="6" x2="6" y2="18" />
                        <line x1="6" y1="6" x2="18" y2="18" />
                      </svg>
                    </span>
                    Less time for business, more time spent on calculations
                  </li>
                  <li>
                    <span className="ba-icon">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <line x1="18" y1="6" x2="6" y2="18" />
                        <line x1="6" y1="6" x2="18" y2="18" />
                      </svg>
                    </span>
                    Running after people to gather data
                  </li>
                </ul>
              </div>

              {/*After*/}
              <div className="ba-card after">
                <div className="ba-heading">
                  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="white" strokeWidth="2">
                    <circle cx="12" cy="12" r="10" />
                    <polyline points="9 12 12 15 17 9" />
                  </svg>
                  After Phormula
                </div>

                <ul className="ba-list">
                  <li>
                    <span className="ba-icon">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <polyline points="20 6 9 17 4 12" />
                      </svg>
                    </span>
                    No more spreadsheet chaos
                  </li>
                  <li>
                    <span className="ba-icon">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <polyline points="20 6 9 17 4 12" />
                      </svg>
                    </span>
                    Proper planning for inventory, dispatch, and POs
                  </li>
                  <li>
                    <span className="ba-icon">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <polyline points="20 6 9 17 4 12" />
                      </svg>
                    </span>
                    AI business insights with objective-based recommendations
                  </li>
                  <li>
                    <span className="ba-icon">
                      <svg viewBox="0 0 24 24" fill="none" stroke="currentColor">
                        <polyline points="20 6 9 17 4 12" />
                      </svg>
                    </span>
                    No more follow-ups — your business explained in a single screen
                  </li>
                </ul>
              </div>
            </div>
          </div>
        </section>

        {/*FEATURES*/}

        <HowItWorks />
        <div className='flex flex-col justify-center'>
          <PhormulaBeamHero />
        </div>
        {/* <Services/> */}

        {/*WORKFLOW*/}
        <section id="workflow" className="workflow">
          <div className="container center">
            <div className="eyebrow">How it works</div>
            <h2 className="section-heading">From raw data to founder-ready decisions.</h2>
            <p className="section-copy">
              Phormula helps teams move from manual reporting to an operating rhythm built around clean numbers and clear actions.
            </p>

            <div className="steps">
              <div className="step">
                <div className="step-num">1</div>
                <h3>Connect your business data</h3>
                <p>Bring sales, fees, ads, inventory, expenses, purchase orders, and cash-flow inputs into one system.</p>
              </div>

              <div className="step">
                <div className="step-num">2</div>
                <h3>Reconcile and visualize</h3>
                <p>Phormula turns scattered inputs into dashboards for revenue, CM2 profit, cash flow, inventory, and performance.</p>
              </div>

              <div className="step">
                <div className="step-num">3</div>
                <h3>Act on AI recommendations</h3>
                <p>Get clear alerts and next steps when inventory, fees, margin, or cash flow needs attention.</p>
              </div>
            </div>
          </div>
        </section>

        {/*VALUE*/}
        {/* <section>
      <div className="container value-grid">
        <div className="value-panel">
          <h3>Built for founders who need clarity, not another report.</h3>
          <p>
            Phormula replaces manual spreadsheet maintenance with a financial command center your team can use every week.
          </p>

          <div className="value-list">
            <div>Know real profit by product and period</div>
            <div>Stop chasing finance and operations teams</div>
            <div>Forecast stock before cash gets trapped</div>
            <div>See AI recommendations tied to business goals</div>
          </div>
        </div>

        <div className="value-cards">
          <div className="value-card">
            <div className="icon-box">🎯</div>
            <h3>Protect margin</h3>
            <p>Catch products that drive revenue but quietly damage profit after fees, ads, returns, and costs.</p>
          </div>

          <div className="value-card">
            <div className="icon-box">🔄</div>
            <h3>Align teams</h3>
            <p>Give finance, ads, operations, inventory, and leadership one shared view of business performance.</p>
          </div>

          <div className="value-card">
            <div className="icon-box">🚚</div>
            <h3>Plan replenishment</h3>
            <p>Understand stock coverage, dispatch planning, purchase orders, and cash implications before issues appear.</p>
          </div>

          <div className="value-card">
            <div className="icon-box">⚡</div>
            <h3>Move faster</h3>
            <p>Use clean data and AI summaries to turn weekly reporting into faster operating decisions.</p>
          </div>
        </div>
      </div>
    </section> */}

        {/*PRICING*/}
        <section id="pricing" className="pricing pricing-section">
  <div className="pricing-bg pricing-bg-one" />
  <div className="pricing-bg pricing-bg-two" />

  <div className="container center pricing-container">
    <div className="pricing-header">
      <div className="eyebrow">Pricing</div>

      <h2 className="section-heading">
        Simple pricing. Serious business clarity.
      </h2>

      <p className="section-copy">
        Start with reliable financial reporting, then upgrade when you need
        forecasting, cash-flow intelligence, and AI-powered recommendations.
      </p>

      <div className="pricing-trust-row">
        <span>No setup fee</span>
        <span>Cancel anytime</span>
        <span>Secure Amazon connection</span>
      </div>
    </div>

    <div className="pricing-grid">
      {/* Reporting plan */}
      <article className="price-card pricing-card-standard">
        <div className="price-card-top">
          <div>
            <div className="plan">Reporting Plan</div>
            <h3>Financial visibility</h3>
          </div>

          <div className="plan-icon plan-icon-reporting">
            <svg
              viewBox="0 0 24 24"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              aria-hidden="true"
            >
              <path
                d="M5 19V10M12 19V5M19 19V13"
                stroke="currentColor"
                strokeWidth="1.8"
                strokeLinecap="round"
              />
              <path
                d="M3 19.5H21"
                stroke="currentColor"
                strokeWidth="1.8"
                strokeLinecap="round"
              />
            </svg>
          </div>
        </div>

        <p className="plan-intro">
          Essential reporting for sellers who want clean, reliable numbers
          without spreadsheet dependency.
        </p>

        <div className="price">
          <span className="currency">$</span>
          <span className="amount">50</span>

          <div className="price-meta">
            <span className="period">/month</span>
            <span className="billing-note">Billed monthly</span>
          </div>
        </div>

        <div className="plan-divider" />

        <div className="includes-label">Everything you need to report</div>

        <ul className="price-list">
          <li>Reporting dashboard</li>
          <li>Complete P&amp;L dashboard</li>
          <li>Sales, fees, and performance tracking</li>
          <li>Amazon expense reconciliation</li>
          <li>SKU-level financial visibility</li>
          <li>Clean monthly reporting view</li>
        </ul>

        <a href="/signin" className="btn pricing-btn pricing-btn-outline">
          Start with Reporting
          <span aria-hidden="true">→</span>
        </a>

        <p className="pricing-footnote">
          Best for sellers building a reliable reporting foundation.
        </p>
      </article>

      {/* Growth plan */}
      <article className="price-card featured pricing-card-growth">
        <div className="badge">
          <span className="badge-dot" />
          Most popular
        </div>

        <div className="growth-glow growth-glow-one" />
        <div className="growth-glow growth-glow-two" />

        <div className="price-card-content">
          <div className="price-card-top">
            <div>
              <div className="plan">Growth Plan</div>
              <h3>Growth intelligence</h3>
            </div>

            <div className="plan-icon plan-icon-growth">
              <svg
                viewBox="0 0 24 24"
                fill="none"
                xmlns="http://www.w3.org/2000/svg"
                aria-hidden="true"
              >
                <path
                  d="M4 17L9 12L13 15L20 7"
                  stroke="currentColor"
                  strokeWidth="1.8"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
                <path
                  d="M15 7H20V12"
                  stroke="currentColor"
                  strokeWidth="1.8"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
              </svg>
            </div>
          </div>

          <p className="plan-intro">
            Full financial intelligence for founders who want to forecast,
            protect profitability, and make faster decisions.
          </p>

          <div className="price">
            <span className="currency">$</span>
            <span className="amount">100</span>

            <div className="price-meta">
              <span className="period">/month</span>
              <span className="billing-note">Billed monthly</span>
            </div>
          </div>

          <div className="plan-divider" />

          <div className="includes-label">Everything in Reporting, plus</div>

          <ul className="price-list">
            <li>AI business insights</li>
            <li>Inventory forecasting</li>
            <li>Dispatch and purchase-order reports</li>
            <li>Cash-flow visibility</li>
            <li>CM2-level profit analysis</li>
            <li>Objective-based recommendations</li>
            <li>SKU-level growth and risk actions</li>
          </ul>

          <a href="/signin" className="btn pricing-btn pricing-btn-growth">
            Choose Growth
            <span aria-hidden="true">→</span>
          </a>

          <p className="pricing-footnote">
            Best for founders actively scaling their Amazon business.
          </p>
        </div>
      </article>
    </div>

    <div className="pricing-bottom-note">
      <div className="pricing-bottom-icon">✓</div>

      <p>
        Both plans include secure data handling, onboarding support, and access
        to your historical business data.
      </p>
    </div>
  </div>
</section>

        {/*FAQ*/}
        <section id="faq">
          <div className="container center">
            <div className="eyebrow">FAQ</div>
            <h2 className="section-heading">Questions founders ask before switching.</h2>

            <FaqAccordion />
          </div>
        </section>

        {/*DEMO*/}
        <section id="demo" className="demo">
          <div className="container">
            <div className="demo-card">
              <div>
                <div className="eyebrow">Get started</div>
                <h2>Replace spreadsheet chaos with financial clarity.</h2>
                <p>
                  Tell us about your business and see how Phormula can help you track, reconcile, forecast, and grow from one financial intelligence dashboard.
                </p>

                <div className="demo-points">
                  <div>See true profit beyond revenue</div>
                  <div>Forecast inventory and cash needs</div>
                  <div>Get AI recommendations for better decisions</div>
                </div>
              </div>

              <form className="demo-form" action="#">
                <input type="text" placeholder="Full name" required />
                <input type="email" placeholder="Work email" required />
                <input type="text" placeholder="Company name" />
                <select defaultValue="">
                  <option value="" disabled>Monthly revenue range</option>
                  <option>Under $50K</option>
                  <option>$50K–$250K</option>
                  <option>$250K–$1M</option>
                  <option>$1M+</option>
                </select>
                <textarea placeholder="What are you currently tracking manually?"></textarea>
                <button className="btn btn-primary" type="submit">Request demo</button>
                <div className="form-note">
                  This form is ready for connection to your CRM, email tool, or backend form handler.
                </div>
              </form>
            </div>
          </div>
        </section>
      </main>

      <footer>
        <div className="container footer-inner">
          <Image
            className="dark:hidden hidden lg:block"
            src="/images/logo/Logo_Phormula.png"
            alt="Logo"
            width={132}
            height={36}
          />

          <div className="footer-links">
            <a href="#platform">Platform</a>
            <a href="#features">Features</a>
            <a href="#pricing">Pricing</a>
            <a href="#demo">Contact</a>
          </div>

          <div className="copyright">© 2026 Phormula. All rights reserved.</div>
        </div>
      </footer>
    </div>
  );
}
