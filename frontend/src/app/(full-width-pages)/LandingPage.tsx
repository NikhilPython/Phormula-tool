import React from "react";
import FinancialDashboardMockup from "@/components/landing-page/financialDashboardMockup";
import HowItWorks from "@/components/landing-page/HowitWorks";
import { PhormulaBeamHero } from "@/components/landing-page/PhormulaBeamHero";
import Services from "@/components/landing-page/Services";
import Image from "next/image";
import FaqAccordion from "@/components/landing-page/FaqAccordion";
import Link from "next/link";

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
        <section className="hero">
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
        </section>

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
        <section id="platform" className="problem">
          <div className="container center">
            <div className="eyebrow">The problem</div>

            <h2 className="section-heading">
              From Scattered Sheets to One Source of Truth
            </h2>

            <p className="section-copy">
              Phormula replaces manual tracking, disconnected reports, and constant follow-ups
              with one live dashboard for your Business.
            </p>

            <div className="problem-flow">
              <div className="flow-heading problem-title">The Problem</div>
              <div className="flow-heading solution-title">The Solution</div>

              <div className="flow-card problem-card-new">
                <div className="flow-icon problem-icon">📄</div>

                <div className="flow-content">
                  <h3>Scattered Sheets</h3>
                  <div className="flow-line problem-line"></div>
                  <p>
                    Sales, fees, inventory, ads, expenses, purchase orders, and cash flow
                    live in separate places.
                  </p>
                </div>
              </div>

              <div className="flow-arrow">→</div>

              <div className="flow-card solution-card-new">
                <div className="flow-icon solution-icon">🖥️</div>

                <div className="flow-content">
                  <h3>One Live Dashboard</h3>
                  <div className="flow-line solution-line"></div>
                  <p>
                    Phormula centralizes your numbers so finance, inventory, ads, and
                    leadership stay aligned.
                  </p>
                </div>
              </div>

              <div className="flow-card problem-card-new">
                <div className="flow-icon problem-icon">⏱️</div>

                <div className="flow-content">
                  <h3>Delayed Decisions</h3>
                  <div className="flow-line problem-line"></div>
                  <p>
                    By the time reports are cleaned up, the opportunity to protect margin
                    may already be gone.
                  </p>
                </div>
              </div>

              <div className="flow-arrow">→</div>

              <div className="flow-card solution-card-new">
                <div className="flow-icon solution-icon">💡</div>

                <div className="flow-content">
                  <h3>Actionable Insights</h3>
                  <div className="flow-line solution-line"></div>
                  <p>
                    AI highlights what changed, why it matters, and what action can improve
                    performance.
                  </p>
                </div>
              </div>
            </div>

            <div className="problem-bottom-note">
              <span>✦</span>
              Phormula turns chaos into clarity so you can lead with confidence.
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
        <section id="pricing" className="pricing">
          <div className="pricing-bg pricing-bg-one"></div>
          <div className="pricing-bg pricing-bg-two"></div>

          <div className="container center pricing-container">
            <div className="eyebrow">Pricing</div>

            <h2 className="section-heading">
              Simple plans for growing sellers.
            </h2>

            <p className="section-copy">
              Start with clean reporting or unlock full financial intelligence with
              forecasting, AI insights, and profit recommendations.
            </p>

            <div className="pricing-grid">
              <div className="price-card animate-card">
                <div className="plan">Reporting Plan</div>

                <div className="price">
                  <span className="currency">$</span>
                  <span className="amount">50</span>
                  <span className="period">/month</span>
                </div>

                <p className="price-desc">
                  For sellers who need clean dashboards and better visibility into
                  financial performance.
                </p>

                <ul className="price-list">
                  <li>Reporting dashboard</li>
                  <li>P&amp;L dashboard</li>
                  <li>Sales, fees, and performance tracking</li>
                  <li>Basic business visibility</li>
                  <li>Clean monthly reporting view</li>
                </ul>

                <a href="#demo" className="btn btn-outline">
                  Choose Reporting
                </a>
              </div>

              <div className="price-card featured animate-card delay">
                <div className="badge">Most popular</div>

                <div className="plan">Growth Plan</div>

                <div className="price">
                  <span className="currency">$</span>
                  <span className="amount">100</span>
                  <span className="period">/month</span>
                </div>

                <p className="price-desc">
                  For founders who want forecasting, AI insights, cash-flow visibility,
                  and full profit intelligence.
                </p>

                <ul className="price-list">
                  <li>Everything in Reporting Plan</li>
                  <li>AI business insights</li>
                  <li>Inventory forecasting</li>
                  <li>Dispatch and PO reports</li>
                  <li>Cash-flow visibility</li>
                  <li>CM2-level profit analysis</li>
                  <li>Objective-based recommendations</li>
                </ul>

                <a href="#demo" className="btn btn-teal">
                  Choose Growth
                </a>
              </div>
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
