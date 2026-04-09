import Link from "next/link";
import { ArrowRight, Sparkles } from "lucide-react";

export function Logo() {
  return (
    <div className="select-none flex items-center gap-2">
      <div className="grid h-9 w-9 place-items-center rounded-xl bg-gradient-to-br from-purple-500 via-pink-500 to-indigo-500 shadow-lg shadow-purple-500/20">
        <Sparkles className="h-4 w-4 text-white" />
      </div>
      <span className="text-xl font-semibold tracking-tight">
        <span className="text-slate-900">Klin</span>
        <span className="gradient-text">AI</span>
      </span>
    </div>
  );
}

export function SiteHeader() {
  return (
    <header className="relative z-10 mx-auto flex max-w-7xl items-center justify-between px-6 py-5">
      <Link href="/" aria-label="KlinAI home">
        <Logo />
      </Link>

      <nav className="hidden items-center gap-8 md:flex">
        <Link className="text-sm text-slate-700 transition hover:text-slate-900" href="/platform">
          Platform
        </Link>
        <Link className="text-sm text-slate-700 transition hover:text-slate-900" href="/privacy">
          Privacy
        </Link>
        <Link className="text-sm text-slate-700 transition hover:text-slate-900" href="/terms">
          Terms
        </Link>
      </nav>

      <div className="flex items-center gap-3">
        <Link
          href="/platform"
          className="hidden rounded-2xl border border-slate-900/10 bg-white/80 px-4 py-2 text-sm font-medium text-slate-900 shadow-sm md:inline-flex"
        >
          Open demo
        </Link>
        <a
          href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Demo%20Request"
          className="inline-flex items-center gap-2 rounded-2xl bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-500 px-5 py-2 text-sm font-medium text-white shadow-lg shadow-pink-500/20"
        >
          Contact sales <ArrowRight className="h-4 w-4" />
        </a>
      </div>
    </header>
  );
}

export function BackgroundGlow() {
  return (
    <div className="pointer-events-none absolute inset-0 -z-10 overflow-hidden">
      <div
        className="absolute -left-28 -top-40 h-[520px] w-[520px] rounded-full blur-3xl"
        style={{ background: "radial-gradient(circle at 50% 50%, rgba(99,102,241,0.18), transparent 60%)" }}
      />
      <div
        className="absolute right-[-10rem] top-1/3 h-[620px] w-[620px] rounded-full blur-3xl"
        style={{ background: "radial-gradient(circle at 50% 50%, rgba(236,72,153,0.18), transparent 60%)" }}
      />
      <div
        className="absolute bottom-[-10rem] left-1/2 h-[560px] w-[560px] -translate-x-1/2 rounded-full blur-3xl"
        style={{ background: "radial-gradient(circle at 50% 50%, rgba(34,211,238,0.16), transparent 60%)" }}
      />
    </div>
  );
}
