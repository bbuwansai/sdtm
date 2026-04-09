import Link from "next/link";
import type { ComponentType } from "react";
import { ArrowRight, Brain, CheckCircle2, Clock, FileText, FlaskConical, Upload } from "lucide-react";
import { BackgroundGlow, SiteHeader } from "@/components/site-shell";

function FeatureCard({ title, text, icon: Icon }: { title: string; text: string; icon: ComponentType<{ className?: string }> }) {
  return (
    <div className="card-glass p-6 shadow-purple-500/10">
      <Icon className="h-5 w-5 text-purple-600" />
      <h3 className="mt-4 text-lg font-semibold">{title}</h3>
      <p className="mt-2 text-sm text-slate-700">{text}</p>
    </div>
  );
}

export default function HomePage() {
  return (
    <main className="min-h-screen bg-background text-slate-900">
      <SiteHeader />
      <section className="relative mx-auto max-w-7xl overflow-hidden rounded-3xl px-6 pb-24 pt-16" style={{ background: "linear-gradient(180deg, rgba(139,92,246,0.06), rgba(236,72,153,0.06) 35%, rgba(255,255,255,0.6))" }}>
        <BackgroundGlow />
        <div className="mx-auto max-w-4xl text-center">
          <span className="inline-flex items-center gap-2 rounded-full border border-slate-900/10 bg-white/70 px-3 py-1 text-xs text-slate-900/80 backdrop-blur">
            <Brain className="h-4 w-4 text-purple-600" /> AI-first clinical data workflows
          </span>
          <h1 className="mt-6 text-5xl font-semibold leading-tight md:text-6xl">
            Transform raw clinical data into <span className="gradient-text">transparent, regulator-ready outputs</span>
          </h1>
          <p className="mx-auto mt-6 max-w-3xl text-lg text-slate-700 md:text-xl">
            Upload raw files, detect the likely domain, run Layer 1 QC, generate mapping specs, and produce SDTM outputs with every processing step visible during the demo.
          </p>
          <div className="mt-8 flex flex-col items-center justify-center gap-4 sm:flex-row">
            <Link href="/platform" className="inline-flex items-center gap-2 rounded-2xl bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-500 px-6 py-3 text-sm font-medium text-white shadow-lg shadow-pink-500/20">
              Open Platform Demo <ArrowRight className="h-4 w-4" />
            </Link>
            <a href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Pilot%20Discussion" className="rounded-2xl border border-slate-900/10 bg-white/70 px-6 py-3 text-sm font-medium text-slate-900">
              Talk to us
            </a>
          </div>
        </div>

        <div className="mx-auto mt-16 grid max-w-4xl gap-6 md:grid-cols-3">
          <div className="card-glass p-6">
            <div className="flex items-center gap-2 text-sm text-slate-700"><Clock className="h-4 w-4" /> Faster turnarounds</div>
            <div className="mt-2 text-3xl font-semibold"><span className="text-slate-500 line-through">Weeks</span> → Minutes</div>
          </div>
          <div className="card-glass p-6">
            <div className="flex items-center gap-2 text-sm text-slate-700"><CheckCircle2 className="h-4 w-4" /> Transparent audit trail</div>
            <div className="mt-2 text-3xl font-semibold">Every step visible</div>
          </div>
          <div className="card-glass p-6">
            <div className="flex items-center gap-2 text-sm text-slate-700"><Upload className="h-4 w-4" /> Demo-ready workflow</div>
            <div className="mt-2 text-3xl font-semibold">Upload → Review → Download</div>
          </div>
        </div>
      </section>

      <section className="mx-auto mt-14 max-w-7xl px-6">
        <div className="grid gap-6 md:grid-cols-3">
          <FeatureCard title="Layer 1 QC" text="Catch raw-data issues up front, separate human review from standardisable issues, and preserve an audit trail." icon={FlaskConical} />
          <FeatureCard title="Spec generation" text="Build structured mapping packages from the raw source so customers can see how logic becomes a reusable spec." icon={FileText} />
          <FeatureCard title="SDTM outputs" text="Run the final transformation with domain-specific logic and package the outputs for easy download and demo review." icon={CheckCircle2} />
        </div>
      </section>
    </main>
  );
}
