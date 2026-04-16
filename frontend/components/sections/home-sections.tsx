import Link from "next/link";
import type { ComponentType } from "react";
import {
  ArrowRight,
  BrainCircuit,
  CheckCircle2,
  FileSearch2,
  FileStack,
  ShieldCheck,
  Sparkles,
  Workflow,
} from "lucide-react";

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
    <div className="rounded-[1.75rem] border border-white/10 bg-white/[0.04] p-6 backdrop-blur-xl">
      <Icon className="h-5 w-5 text-emerald-300" />
      <h3 className="mt-4 text-xl font-semibold text-white">{title}</h3>
      <p className="mt-3 text-sm leading-7 text-slate-300">{text}</p>
    </div>
  );
}

export function ProductNarrative() {
  return (
    <section className="mx-auto max-w-7xl px-6 py-8 lg:px-10">
      <div className="grid gap-6 lg:grid-cols-3">
        <div className="rounded-[2rem] border border-slate-800 bg-slate-950 p-7 shadow-[0_20px_100px_rgba(2,12,21,0.35)] lg:col-span-2">
          <div className="flex items-center gap-2 text-sm text-emerald-300">
            <Sparkles className="h-4 w-4" /> What KlinAI does
          </div>
          <h2 className="mt-4 text-3xl font-semibold tracking-tight text-white md:text-4xl">
            From raw clinical data to QC, mapping specs, and traceable CDISC-aligned SDTM.
          </h2>
          <p className="mt-4 max-w-3xl text-base leading-8 text-slate-300">
            KlinAI helps biometrics, data management, and CRO teams clean and standardize messy source data,
            separate human-review issues from rule-based transformations, generate AI-assisted mapping specifications,
            and progress toward SDTM outputs with full review visibility.
          </p>

          <div className="mt-8 grid gap-5 md:grid-cols-2 xl:grid-cols-4">
            <SectionCard
              title="QC first"
              text="Detect missing fields, inconsistencies, unit mismatches, and logic issues before downstream work begins."
              icon={FileSearch2}
            />
            <SectionCard
              title="Standardize automatically"
              text="Apply mapping and transformation rules where the data can be standardized without manual intervention."
              icon={BrainCircuit}
            />
            <SectionCard
              title="Generate mapping spec"
              text="Create an AI-assisted blueprint that explains how raw fields map into SDTM-ready outputs."
              icon={FileStack}
            />
            <SectionCard
              title="Stay fully traceable"
              text="Keep each step reviewable so teams can see what changed, what needs input, and what is ready for SDTM."
              icon={ShieldCheck}
            />
          </div>
        </div>

        <div className="rounded-[2rem] border border-emerald-400/20 bg-gradient-to-b from-emerald-500/10 to-cyan-500/10 p-7 text-white shadow-[0_20px_100px_rgba(14,165,233,0.18)]">
          <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs uppercase tracking-[0.2em] text-emerald-200">
            <Workflow className="h-3.5 w-3.5" /> Demo flow
          </div>
          <ol className="mt-6 space-y-5">
            {[
              "Upload a raw dataset on the homepage or in the full platform workspace.",
              "Run Layer 1 QC and spec generation to identify human-review items and create the mapping blueprint.",
              "Upload the reviewed issue file back into the same job and generate the SDTM package.",
            ].map((item, index) => (
              <li key={item} className="flex gap-4">
                <div className="mt-0.5 grid h-7 w-7 shrink-0 place-items-center rounded-full bg-white/10 text-sm font-semibold text-emerald-200">
                  {index + 1}
                </div>
                <p className="text-sm leading-7 text-slate-100">{item}</p>
              </li>
            ))}
          </ol>

          <div className="mt-8 rounded-[1.5rem] border border-white/10 bg-slate-950/40 p-5">
            <div className="flex items-center gap-2 text-sm text-emerald-200">
              <CheckCircle2 className="h-4 w-4" /> Built for pilot evaluation
            </div>
            <p className="mt-3 text-sm leading-7 text-slate-200">
              Use a sample file or upload your own structured test dataset. Designed for pilot evaluation with biometrics
              and data management teams.
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
