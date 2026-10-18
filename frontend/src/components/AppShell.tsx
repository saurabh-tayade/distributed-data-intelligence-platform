import type { ReactNode } from 'react';

export function AppShell({ children }: { children: ReactNode }) {
  return <>
    <a className="skip-link" href="#main">Skip to analytics</a>
    <header className="site-header">
      <div className="header-inner">
        <div className="brand">
          <svg className="brand-mark" viewBox="0 0 36 36" fill="none" aria-hidden="true">
            <path d="M7 25V17M18 25V8M29 25V12" stroke="currentColor" strokeWidth="5" />
            <path d="M5 31H31" stroke="currentColor" strokeWidth="2" />
          </svg>
          <span>Content Intelligence<span className="brand-subtitle">DISTRIBUTED DATA PLATFORM</span></span>
        </div>
        <span className="header-section"><span className="header-dot" />Publisher analytics</span>
      </div>
    </header>
    <main id="main" tabIndex={-1}>{children}</main>
    <footer className="site-footer"><span>Content Intelligence <span aria-hidden="true">/</span> Publisher analytics</span>
      <span>One publication. A consistent view.</span></footer>
  </>;
}
