import Link from "next/link";
import { ArrowRight, Activity, Menu } from "lucide-react";

export function Logo() {
  return (
    <div className="flex select-none items-center gap-3">
      <div className="grid h-10 w-10 place-items-center rounded-2xl border border-emerald-300/20 bg-gradient-to-br from-emerald-400/25 via-cyan-400/20 to-sky-500/25 shadow-[0_0_40px_rgba(34,211,238,0.15)] backdrop-blur-xl">
        <Activity className="h-4 w-4 text-emerald-200" />
      </div>
      <span className="text-xl font-semibold tracking-tight text-white">
        Klin<span className="bg-gradient-to-r from-emerald-200 via-cyan-200 to-sky-300 bg-clip-text text-transparent">AI</span>
      </span>
    </div>
  );
}

export function SiteHeader() {
  return (
    <header className="sticky top-0 z-50 border-b border-white/6 bg-[#020817]/70 backdrop-blur-xl">
      <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4 lg:px-10">
        <Link href="/" aria-label="KlinAI home">
          <Logo />
        </Link>

        <nav className="hidden items-center gap-8 md:flex">
          <Link className="text-sm text-slate-300 transition hover:text-white" href="/#product">
            Product
          </Link>
          <Link className="text-sm text-slate-300 transition hover:text-white" href="/#demo-workspace">
            Demo
          </Link>
          <Link className="text-sm text-slate-300 transition hover:text-white" href="/platform">
            Platform
          </Link>
          <Link className="text-sm text-slate-300 transition hover:text-white" href="/privacy">
            Privacy
          </Link>
          <Link className="text-sm text-slate-300 transition hover:text-white" href="/terms">
            Terms
          </Link>
        </nav>

        <div className="flex items-center gap-3">
          <Link
            href="/platform"
            className="hidden rounded-2xl border border-white/10 bg-white/5 px-4 py-2 text-sm font-medium text-white shadow-sm md:inline-flex"
          >
            Open platform
          </Link>
          <a
            href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Demo%20Request"
            className="inline-flex items-center gap-2 rounded-2xl bg-gradient-to-r from-emerald-400 via-cyan-400 to-sky-500 px-5 py-2 text-sm font-semibold text-slate-950 shadow-[0_12px_50px_rgba(14,165,233,0.25)]"
          >
            Contact sales <ArrowRight className="h-4 w-4" />
          </a>
          <button className="grid h-10 w-10 place-items-center rounded-2xl border border-white/10 bg-white/5 text-slate-200 md:hidden">
            <Menu className="h-4 w-4" />
          </button>
        </div>
      </div>
    </header>
  );
}
