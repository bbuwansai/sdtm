import Link from "next/link";
import { ArrowRight, CheckCircle2, Clock3, ShieldCheck } from "lucide-react";
import AnimatedShaderHero from "@/components/ui/animated-shader-hero";
import { SiteHeader } from "@/components/site-shell";
import { ProductNarrative } from "@/components/sections/home-sections";
import { PlatformDemo } from "@/components/platform-demo";

export default function HomePage() {
  return (
    <main className="min-h-screen bg-background text-foreground">
      <SiteHeader />

      <div className="mx-auto max-w-7xl px-6 pt-6 lg:px-10">
        <AnimatedShaderHero
          trustBadge={{ text: "Built for biometrics, data management, and CRO workflows.", icons: ["✦"] }}
          headline={{
            line1: "Raw clinical data",
            line2: "to traceable SDTM workflows.",
          }}
          subtitle="KlinAI standardizes messy source data, separates human-review items from rule-based transformations, generates mapping specifications, and supports CDISC-aligned SDTM delivery in one reviewable workflow."
          className="shadow-[0_30px_120px_rgba(2,12,21,0.45)]"
        >
          <div className="grid gap-5 lg:grid-cols-[1.15fr_0.85fr]">
            <div className="rounded-[1.75rem] border border-white/10 bg-slate-950/50 p-6 backdrop-blur-xl">
              <div className="flex flex-wrap gap-3 text-sm text-slate-200">
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1">Layer 1 QC</span>
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1">Spec generation</span>
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1">SDTM workflow</span>
                <span className="rounded-full border border-white/10 bg-white/5 px-3 py-1">Traceable outputs</span>
              </div>
              <div className="mt-6 grid gap-4 md:grid-cols-3">
                <div className="rounded-[1.5rem] bg-white/5 p-5">
                  <div className="flex items-center gap-2 text-sm text-slate-300">
                    <Clock3 className="h-4 w-4 text-emerald-300" /> Turnaround
                  </div>
                  <div className="mt-2 text-3xl font-semibold text-white">Minutes, not months</div>
                </div>
                <div className="rounded-[1.5rem] bg-white/5 p-5">
                  <div className="flex items-center gap-2 text-sm text-slate-300">
                    <ShieldCheck className="h-4 w-4 text-cyan-300" /> Reviewability
                  </div>
                  <div className="mt-2 text-3xl font-semibold text-white">Full workflow visibility</div>
                </div>
                <div className="rounded-[1.5rem] bg-white/5 p-5">
                  <div className="flex items-center gap-2 text-sm text-slate-300">
                    <CheckCircle2 className="h-4 w-4 text-sky-300" /> Demo access
                  </div>
                  <div className="mt-2 text-3xl font-semibold text-white">Homepage + platform</div>
                </div>
              </div>
            </div>

            <div className="rounded-[1.75rem] border border-emerald-300/20 bg-gradient-to-b from-emerald-400/10 to-cyan-500/10 p-6 backdrop-blur-xl">
              <div className="text-sm uppercase tracking-[0.18em] text-emerald-200">For prospects and pilots</div>
              <h2 className="mt-4 text-2xl font-semibold text-white">Give clients a clear first impression.</h2>
              <p className="mt-4 text-sm leading-7 text-slate-200">
                The homepage now speaks to outcomes and product value first. The live demo remains accessible on the
                homepage and at <span className="font-medium">klinai.tech/platform</span> for deeper exploration.
              </p>
              <div className="mt-6 flex flex-col gap-3 sm:flex-row">
                <Link
                  href="#demo-workspace"
                  className="inline-flex items-center justify-center gap-2 rounded-2xl bg-white px-5 py-3 text-sm font-semibold text-slate-950"
                >
                  Jump to live demo <ArrowRight className="h-4 w-4" />
                </Link>
                <Link
                  href="/platform"
                  className="inline-flex items-center justify-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-5 py-3 text-sm font-semibold text-white"
                >
                  View full platform
                </Link>
              </div>
            </div>
          </div>
        </AnimatedShaderHero>
      </div>

      <section id="product" className="pt-8">
        <ProductNarrative />
      </section>

      <section className="mx-auto max-w-7xl px-6 py-6 lg:px-10">
        <div className="rounded-[2rem] border border-white/10 bg-gradient-to-r from-slate-950 via-[#071523] to-[#0a1d2c] p-2 shadow-[0_24px_100px_rgba(2,12,21,0.38)]">
          <PlatformDemo compact />
        </div>
      </section>
    </main>
  );
}
