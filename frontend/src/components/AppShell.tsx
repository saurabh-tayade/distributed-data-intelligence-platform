import type { ReactNode } from 'react';

export function AppShell({ children, section = 'Publisher analytics', area = 'analytics' }:
  { children: ReactNode; section?: string; area?: 'analytics' | 'operations' | 'none' }) {
  return <>
    <a className="skip-link" href="#main" onClick={(event) => {
      event.preventDefault(); document.getElementById('main')?.focus();
    }}>Skip to {area === 'none' ? 'content' : area}</a>
    <header className="site-header">
      <div className="header-inner">
        <div className="brand">
          <svg className="brand-mark" viewBox="0 0 36 36" fill="none" aria-hidden="true">
            <path d="M7 25V17M18 25V8M29 25V12" stroke="currentColor" strokeWidth="5" />
            <path d="M5 31H31" stroke="currentColor" strokeWidth="2" />
          </svg>
          <span>Content Intelligence<span className="brand-subtitle">DISTRIBUTED DATA PLATFORM</span></span>
        </div>
        <span className="header-section"><span className="header-dot" />{section}</span>
      </div>
    </header>
    <nav className="workspace-navigation" aria-label="Workspaces">
      <a href="#/analytics" aria-current={area === 'analytics' ? 'page' : undefined}>Analytics</a>
      <a href="#/operations/sources" aria-current={area === 'operations' ? 'page' : undefined}>Operations</a>
    </nav>
    <main id="main" tabIndex={-1}>{children}</main>
    <footer className="site-footer"><span>Content Intelligence <span aria-hidden="true">/</span> {area === 'analytics' ? 'Analytics' : area === 'operations' ? 'Operations' : 'DDIP'} workspace</span>
      <span>{area === 'analytics' ? 'One publication. A consistent view.' : area === 'operations' ? 'Sources and executions. Read-only inspection.' : 'Analytics and Operations.'}</span></footer>
  </>;
}
