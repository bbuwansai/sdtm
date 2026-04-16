"use client";

import Link from "next/link";
import { ArrowRight, Activity, Menu, X } from "lucide-react";
import { useState } from "react";

export function Logo() {
  return (
    <div className="flex select-none items-center gap-3">
      <div className="grid h-10 w-10 place-items-center rounded-2xl border border-[#f0c58d]/20 bg-gradient-to-br from-[#cf9a59]/25 via-[#f4d8b0]/10 to-[#8b5b33]/20 shadow-[0_0_40px_rgba(207,154,89,0.1)] backdrop-blur-xl">
        <Activity className="h-4 w-4 text-[#f4d4a5]" />
      </div>
      <span className="text-xl font-semibold tracking-tight text-white">
        Klin<span className="bg-gradient-to-r from-[#fffaf2] via-[#f4d8b0] to-[#c69458] bg-clip-text text-transparent">AI</span>
      </span>
    </div>
  );
}

const navItems = [
  { href: "/#product", label: "Product" },
  { href: "/#video", label: "Demo" },
  { href: "/platform", label: "Platform" },
  { href: "/privacy", label: "Privacy" },
  { href: "/terms", label: "Terms" },
];

export function SiteHeader() {
  const [open, setOpen] = useState(false);

  return (
    <header className="sticky top-0 z-50 border-b border-white/6 bg-[#050404]/78 backdrop-blur-xl">
      <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4 lg:px-10">
        <Link href="/" aria-label="KlinAI home" onClick={() => setOpen(false)}>
          <Logo />
        </Link>

        <nav className="hidden items-center gap-8 md:flex">
          {navItems.map((item) => (
            <Link key={item.href} className="text-sm text-[#d9cebf] transition hover:text-white" href={item.href}>
              {item.label}
            </Link>
          ))}
        </nav>

        <div className="flex items-center gap-3">
          <Link
            href="/platform"
            className="hidden rounded-2xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-medium text-white shadow-sm md:inline-flex"
          >
            Try live demo
          </Link>
          <a
            href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Demo%20Request"
            className="inline-flex items-center gap-2 rounded-2xl bg-gradient-to-r from-[#f6e1c4] via-[#f3d2a4] to-[#cb9453] px-5 py-2 text-sm font-semibold text-[#1a120a] shadow-[0_12px_50px_rgba(201,148,88,0.18)]"
          >
            Book a demo <ArrowRight className="h-4 w-4" />
          </a>
          <button
            onClick={() => setOpen((v) => !v)}
            className="grid h-10 w-10 place-items-center rounded-2xl border border-white/10 bg-white/5 text-slate-200 md:hidden"
            aria-label="Toggle navigation"
          >
            {open ? <X className="h-4 w-4" /> : <Menu className="h-4 w-4" />}
          </button>
        </div>
      </div>

      {open ? (
        <div className="border-t border-white/6 bg-[#090705]/96 px-6 py-4 md:hidden">
          <nav className="mx-auto flex max-w-7xl flex-col gap-3">
            {navItems.map((item) => (
              <Link
                key={item.href}
                href={item.href}
                className="rounded-xl border border-white/6 bg-white/[0.03] px-4 py-3 text-sm text-[#e3d7c8]"
                onClick={() => setOpen(false)}
              >
                {item.label}
              </Link>
            ))}
          </nav>
        </div>
      ) : null}
    </header>
  );
}
