import Link from "next/link";
import type { ComponentType } from "react";
import { ArrowRight, BrainCircuit, CheckCircle2, FileSearch2, FileStack, ShieldCheck, Sparkles, Workflow } from "lucide-react";

function SectionCard({
  title,
  text,
  icon: Icon,
}: {
  title: string;
  text: string;
  icon: ComponentType<{ className?: string }>;
}) {
  return (
    <div className="rounded-[1.75rem] border border-white/10 bg-white/[0.045] p-6 backdrop-blur-xl">
      <Icon className="h-5 w-5 text-[#f2c98f]" />
      <h3 className="mt-4 text-xl font-semibold text-white">{title}</h3>
      <p className="mt-3 text-sm leading-7 text-[#ddd2c4]">{text}</p>
    </div>
  );
}

export function ProductNarrative() {
  return (
    <section className="mx-auto max-w-7xl px-6 py-10 lg:px-10">
      <div className="grid gap-6 lg:grid-cols-3">
        <div className="rounded-[2rem] border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.06),rgba(255,255,255,0.03))] p-7 shadow-[0_20px_100px_rgba(0,0,0,0.24)] lg:col-span-2">
          <div className="flex items-center gap-2 text-sm text-[#f4d4a5]">
            <Sparkles className="h-4 w-4" /> What KlinAI does
          </div>
          <h2 className="mt-4 text-3xl font-semibold tracking-tight text-white md:text-4xl">
            A single workflow from raw data review to SDTM delivery.
          </h2>
          <p className="mt-4 max-w-3xl text-base leading-8 text-[#ddd2c4]">
            KlinAI is built for teams that need to move faster from raw clinical data to reviewable SDTM outputs without losing control.
            It identifies data issues early, separates human-review items from rule-based standardization, generates the mapping
            specification, and keeps the workflow traceable from input to output.
          </p>

          <div className="mt-8 grid gap-5 md:grid-cols-2 xl:grid-cols-4">
            <SectionCard
              title="Identify issues early"
              text="Find missing fields, inconsistent values, partial dates, duplicates, and other data quality issues before they slow down downstream work."
              icon={FileSearch2}
            />
            <SectionCard
              title="Standardize with rules"
              text="Apply rule-based transformations where possible while clearly separating items that still require human review."
              icon={BrainCircuit}
            />
            <SectionCard
              title="Generate the spec"
              text="Create the mapping and transformation specification that explains how raw fields are translated into SDTM outputs."
              icon={FileStack}
            />
            <SectionCard
              title="Keep it reviewable"
              text="Give teams visibility into what changed, what was standardized, what still needs review, and what is ready for output."
              icon={ShieldCheck}
            />
          </div>
        </div>

        <div className="rounded-[2rem] border border-[#f0c58d]/20 bg-[linear-gradient(180deg,rgba(202,151,92,0.14),rgba(255,255,255,0.03))] p-7 text-white shadow-[0_20px_100px_rgba(0,0,0,0.18)]">
          <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs uppercase tracking-[0.2em] text-[#f4d4a5]">
            <Workflow className="h-3.5 w-3.5" /> Demo flow
          </div>
          <ol className="mt-6 space-y-5">
            {[
              "Upload a raw dataset from the homepage or full platform workspace.",
              "Run QC and specification generation to surface issues and create the mapping blueprint.",
              "Upload the reviewed issue file and generate the SDTM output package.",
            ].map((item, index) => (
              <li key={item} className="flex gap-4">
                <div className="mt-0.5 grid h-7 w-7 shrink-0 place-items-center rounded-full bg-white/10 text-sm font-semibold text-[#f4d4a5]">
                  {index + 1}
                </div>
                <p className="text-sm leading-7 text-[#f5eee4]">{item}</p>
              </li>
            ))}
          </ol>

          <div className="mt-8 rounded-[1.5rem] border border-white/10 bg-[#110d09]/60 p-5">
            <div className="flex items-center gap-2 text-sm text-[#f4d4a5]">
              <CheckCircle2 className="h-4 w-4" /> Built for pilot conversations
            </div>
            <p className="mt-3 text-sm leading-7 text-[#e6dbce]">
              Use a sample file or upload your own structured test dataset to evaluate how the workflow behaves on realistic inputs.
            </p>
            <Link
              href="/platform"
              className="mt-5 inline-flex items-center gap-2 rounded-2xl bg-white px-4 py-3 text-sm font-semibold text-slate-950 transition hover:-translate-y-0.5"
            >
              Open full platform <ArrowRight className="h-4 w-4" />
            </Link>
          </div>
        </div>
      </div>
    </section>
  );
}

export function LoomSection() {
  return (
    <section className="mx-auto max-w-7xl px-6 py-6 lg:px-10">
      <div className="rounded-[2rem] border border-white/10 bg-[linear-gradient(180deg,rgba(255,255,255,0.055),rgba(255,255,255,0.03))] p-6 shadow-[0_20px_100px_rgba(0,0,0,0.22)] lg:p-8">
        <div className="max-w-3xl">
          <div className="text-sm uppercase tracking-[0.22em] text-[#f4d4a5]">Product walkthrough</div>
          <h2 className="mt-3 text-3xl font-semibold text-white md:text-4xl">See the workflow in action.</h2>
          <p className="mt-3 text-base leading-8 text-[#ddd2c4]">
            A short walkthrough of how KlinAI handles QC, specification generation, human review, and SDTM output in a single flow.
          </p>
        </div>
        <div className="mt-6 overflow-hidden rounded-[1.75rem] border border-white/10 bg-black shadow-[0_20px_80px_rgba(0,0,0,0.35)]">
          <div style={{ position: "relative", paddingBottom: "64.86161251504213%", height: 0 }}>
            <iframe
              src="https://www.loom.com/embed/26d2085a475e4f3c871fc0720fce120f"
              frameBorder="0"
              allowFullScreen
              className="absolute left-0 top-0 h-full w-full"
              title="KlinAI loom demo"
            />
          </div>
        </div>
      </div>
    </section>
  );
}

export function BottomCta() {
  return (
    <section className="mx-auto max-w-7xl px-6 py-8 lg:px-10">
      <div className="rounded-[2rem] border border-[#f0c58d]/18 bg-[radial-gradient(circle_at_top,rgba(199,145,87,0.18),transparent_28%),linear-gradient(180deg,rgba(255,255,255,0.055),rgba(255,255,255,0.03))] p-8 shadow-[0_24px_120px_rgba(0,0,0,0.24)] lg:p-10">
        <div className="grid gap-6 lg:grid-cols-[1.2fr_0.8fr] lg:items-end">
          <div>
            <div className="text-sm uppercase tracking-[0.2em] text-[#f4d4a5]">About KlinAI</div>
            <h2 className="mt-3 text-3xl font-semibold text-white md:text-4xl">Built for teams that need clinical data delivery to move faster without losing control.</h2>
            <p className="mt-4 max-w-3xl text-base leading-8 text-[#ddd2c4]">
              KlinAI helps transform raw clinical data into reviewable, CDISC-aligned SDTM workflows with clear separation between human-review issues and rule-based standardization. The result is a faster path to output with full visibility into the workflow.
            </p>
          </div>
          <div className="rounded-[1.75rem] border border-white/10 bg-[#110d09]/55 p-6">
            <div className="text-lg font-semibold text-white">Ready for a live walkthrough?</div>
            <p className="mt-3 text-sm leading-7 text-[#e6dbce]">
              Book a demo to see the workflow on sample data and discuss how it can fit biometrics, CRO, or data management teams.
            </p>
            <a
              href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Demo%20Request"
              className="mt-5 inline-flex items-center gap-2 rounded-2xl bg-white px-5 py-3 text-sm font-semibold text-slate-950 transition hover:-translate-y-0.5"
            >
              Book a demo <ArrowRight className="h-4 w-4" />
            </a>
          </div>
        </div>
      </div>
    </section>
  );
}
