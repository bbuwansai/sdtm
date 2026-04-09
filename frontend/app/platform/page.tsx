"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";
import {
  ArrowRight,
  CheckCircle2,
  CircleAlert,
  Download,
  FileSpreadsheet,
  FileUp,
  FlaskConical,
  Layers3,
  LoaderCircle,
  ScanSearch,
  ShieldCheck,
  Sparkles,
} from "lucide-react";
import { API_BASE_URL, type JobSummary } from "@/lib/api";
import { BackgroundGlow, SiteHeader } from "@/components/site-shell";

type Detection = {
  domain: string;
  confidence: number;
  matched_columns: string[];
  all_scores: Record<string, number>;
  runner_up?: string;
};

const domainOptions = ["AUTO", "DM", "VS", "LB", "AE"];

function StatusPill({ children }: { children: React.ReactNode }) {
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-slate-900/10 bg-white/75 px-3 py-1 text-xs text-slate-700 backdrop-blur">
      {children}
    </span>
  );
}

export default function PlatformPage() {
  const [file, setFile] = useState<File | null>(null);
  const [selectedDomain, setSelectedDomain] = useState("AUTO");
  const [detection, setDetection] = useState<Detection | null>(null);
  const [job, setJob] = useState<JobSummary | null>(null);
  const [busy, setBusy] = useState(false);
  const pollRef = useRef<number | null>(null);

  useEffect(() => {
    return () => {
      if (pollRef.current) window.clearInterval(pollRef.current);
    };
  }, []);

  const detectedLabel = useMemo(() => {
    if (!detection) return "Ready to analyse uploaded file";
    return `${detection.domain} • ${Math.round(detection.confidence * 100)}% confidence`;
  }, [detection]);

  async function detectDomain() {
    if (!file) return;
    const form = new FormData();
    form.append("file", file);
    const res = await fetch(`${API_BASE_URL}/api/detect-domain`, { method: "POST", body: form });
    if (!res.ok) throw new Error("Domain detection failed");
    const data = await res.json();
    setDetection(data);
  }

  async function fetchJob(jobId: string) {
    const res = await fetch(`${API_BASE_URL}/api/jobs/${jobId}`);
    if (!res.ok) return;
    const data: JobSummary = await res.json();
    setJob(data);
    if (["completed", "failed"].includes(data.status) && pollRef.current) {
      window.clearInterval(pollRef.current);
      pollRef.current = null;
      setBusy(false);
    }
  }

  async function runPipeline() {
    if (!file) return;
    setBusy(true);
    setJob(null);
    const form = new FormData();
    form.append("file", file);
    form.append("domain", selectedDomain);
    const res = await fetch(`${API_BASE_URL}/api/jobs`, { method: "POST", body: form });
    if (!res.ok) {
      setBusy(false);
      throw new Error("Pipeline job could not be created");
    }
    const data = await res.json();
    await fetchJob(data.job_id);
    pollRef.current = window.setInterval(() => void fetchJob(data.job_id), 1500);
  }

  const timeline = job?.timeline?.length
    ? job.timeline
    : [{ time: new Date().toISOString(), level: "info", message: "Upload a file to start the live processing run." }];

  return (
    <main className="min-h-screen bg-background text-slate-900">
      <SiteHeader />

      <section className="relative mx-auto max-w-7xl overflow-hidden rounded-[2rem] px-6 pb-20 pt-14" style={{ background: "linear-gradient(180deg, rgba(139,92,246,0.08), rgba(236,72,153,0.06) 35%, rgba(255,255,255,0.78))" }}>
        <BackgroundGlow />

        <div className="grid items-center gap-10 lg:grid-cols-[1.05fr_0.95fr]">
          <div className="max-w-3xl">
            <StatusPill>
              <Sparkles className="h-4 w-4 text-purple-600" />
              Clinical automation demo
            </StatusPill>

            <h1 className="mt-6 text-5xl font-semibold leading-[0.95] md:text-7xl">
              Upload raw data.
              <br />
              <span className="gradient-text">See the full path to SDTM.</span>
            </h1>

            <p className="mt-6 max-w-2xl text-xl leading-8 text-slate-700">
              A cleaner, more visual demo flow: detect the domain, surface QC findings, show what would go to review,
              and continue automatically into spec generation and final SDTM outputs.
            </p>

            <div className="mt-8 flex flex-wrap gap-4">
              <a
                href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Demo%20Request"
                className="inline-flex items-center gap-2 rounded-2xl bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-500 px-6 py-3 text-sm font-medium text-white shadow-lg shadow-pink-500/20"
              >
                Contact sales <ArrowRight className="h-4 w-4" />
              </a>
              <Link
                href="#demo-workspace"
                className="inline-flex items-center gap-2 rounded-2xl border border-slate-900/10 bg-white/80 px-6 py-3 text-sm font-medium text-slate-900"
              >
                Open live demo
              </Link>
            </div>
          </div>

          <div className="grid gap-5 sm:grid-cols-2 lg:grid-cols-1 xl:grid-cols-2">
            <div className="card-glass p-7">
              <div className="flex items-center gap-2 text-sm text-slate-700">
                <FlaskConical className="h-4 w-4" /> QC narrative
              </div>
              <div className="mt-4 text-4xl font-semibold leading-tight">Raw → Layer 1</div>
              <p className="mt-3 text-base leading-7 text-slate-600">
                Show issue detection, categorisation, and what would normally be routed for human review.
              </p>
            </div>
            <div className="card-glass p-7">
              <div className="flex items-center gap-2 text-sm text-slate-700">
                <FileSpreadsheet className="h-4 w-4" /> Mapping logic
              </div>
              <div className="mt-4 text-4xl font-semibold leading-tight">Spec generation</div>
              <p className="mt-3 text-base leading-7 text-slate-600">
                Build the spec package from raw inputs so the customer sees how the transformation logic becomes reusable.
              </p>
            </div>
            <div className="card-glass p-7 sm:col-span-2 lg:col-span-1 xl:col-span-2">
              <div className="flex items-center gap-2 text-sm text-slate-700">
                <ShieldCheck className="h-4 w-4" /> Final package
              </div>
              <div className="mt-4 text-4xl font-semibold leading-tight">Spec + Layer 1 → SDTM output</div>
              <p className="mt-3 max-w-xl text-base leading-7 text-slate-600">
                Keep the explanation visual and spacious, then let the outputs speak: issue logs, layer 1 files, spec workbook,
                and the final SDTM package.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto mt-12 max-w-7xl px-6">
        <div className="grid gap-6 md:grid-cols-3">
          <div className="card-glass p-8">
            <div className="text-sm text-slate-600">Faster turnarounds</div>
            <div className="mt-4 text-6xl font-semibold leading-none text-slate-400 line-through">Weeks</div>
            <div className="mt-2 text-6xl font-semibold leading-none">Minutes</div>
          </div>
          <div className="card-glass p-8">
            <div className="text-sm text-slate-600">Transparent audit trail</div>
            <div className="mt-4 text-5xl font-semibold leading-tight">Every step visible</div>
          </div>
          <div className="card-glass p-8">
            <div className="text-sm text-slate-600">Demo-ready workflow</div>
            <div className="mt-4 text-5xl font-semibold leading-tight">Upload → Review → Download</div>
          </div>
        </div>
      </section>

      <section id="demo-workspace" className="mx-auto mt-12 max-w-7xl px-6">
        <div className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
          <div className="space-y-6">
            <div className="card-glass p-8">
              <div className="flex items-center gap-3">
                <FileUp className="h-5 w-5 text-purple-600" />
                <h2 className="text-2xl font-semibold">Upload raw data</h2>
              </div>
              <p className="mt-3 max-w-2xl text-base leading-7 text-slate-600">
                Start with the source file. The platform can auto-detect the likely domain or you can choose one manually.
              </p>

              <div className="mt-6 rounded-[2rem] border border-dashed border-slate-300 bg-white/60 p-8">
                <input
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  onChange={(e) => setFile(e.target.files?.[0] ?? null)}
                  className="block w-full text-base text-slate-700"
                />
                <div className="mt-5 flex flex-wrap gap-3 text-sm text-slate-500">
                  <span>Accepted formats: CSV, XLSX, XLS</span>
                  {file ? <span className="font-medium text-slate-800">Selected: {file.name}</span> : null}
                </div>
              </div>
            </div>

            <div className="card-glass p-8">
              <div className="flex items-center gap-3">
                <ScanSearch className="h-5 w-5 text-purple-600" />
                <h2 className="text-2xl font-semibold">Domain detection & run controls</h2>
              </div>
              <div className="mt-6 grid gap-4 lg:grid-cols-[220px_1fr_1fr]">
                <select
                  value={selectedDomain}
                  onChange={(e) => setSelectedDomain(e.target.value)}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-4 text-sm outline-none"
                >
                  {domainOptions.map((option) => (
                    <option key={option} value={option}>
                      {option === "AUTO" ? "Auto detect" : option}
                    </option>
                  ))}
                </select>

                <button
                  disabled={!file}
                  onClick={() => void detectDomain()}
                  className="rounded-2xl border border-slate-900/10 bg-white/80 px-5 py-4 text-sm font-medium disabled:opacity-50"
                >
                  Analyse file
                </button>

                <button
                  disabled={!file || busy}
                  onClick={() => void runPipeline()}
                  className="rounded-2xl bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-500 px-5 py-4 text-sm font-medium text-white shadow-lg disabled:opacity-50"
                >
                  {busy ? "Processing…" : "Run transformation"}
                </button>
              </div>

              <div className="mt-6 grid gap-4 lg:grid-cols-[1.1fr_0.9fr]">
                <div className="rounded-[1.5rem] bg-slate-50 p-6">
                  <div className="text-sm font-medium text-slate-900">Detected domain</div>
                  <div className="mt-2 text-2xl font-semibold text-slate-900">{detectedLabel}</div>
                  {detection?.runner_up ? <div className="mt-3 text-sm text-slate-500">Runner-up: {detection.runner_up}</div> : null}
                </div>
                <div className="rounded-[1.5rem] bg-slate-50 p-6">
                  <div className="text-sm font-medium text-slate-900">Matched columns</div>
                  <div className="mt-2 text-sm leading-7 text-slate-600">
                    {detection?.matched_columns?.length ? detection.matched_columns.join(", ") : "The matched column evidence will appear here after analysis."}
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-6">
            <div className="card-glass p-8">
              <div className="flex items-center gap-3">
                <Layers3 className="h-5 w-5 text-purple-600" />
                <h2 className="text-2xl font-semibold">Live process timeline</h2>
              </div>
              <p className="mt-3 text-base leading-7 text-slate-600">
                This is the part customers should watch: detection, QC, issue handling, spec generation, SDTM preparation, and packaged outputs.
              </p>

              <div className="mt-6 space-y-4">
                {timeline.map((event, index) => (
                  <div key={`${event.time}-${index}`} className="rounded-[1.5rem] bg-slate-50 p-5">
                    <div className="flex gap-4">
                      <div className="mt-0.5">
                        {event.level === "success" ? (
                          <CheckCircle2 className="h-5 w-5 text-emerald-600" />
                        ) : event.level === "error" ? (
                          <CircleAlert className="h-5 w-5 text-red-600" />
                        ) : (
                          <LoaderCircle className="h-5 w-5 text-purple-600" />
                        )}
                      </div>
                      <div>
                        <div className="text-xs uppercase tracking-[0.2em] text-slate-400">{new Date(event.time).toLocaleTimeString()}</div>
                        <div className="mt-2 text-base leading-7 text-slate-800">{event.message}</div>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </div>

            <div className="card-glass p-8">
              <h2 className="text-2xl font-semibold">Outputs</h2>
              <p className="mt-3 text-base leading-7 text-slate-600">
                Layer 1 output, spec package, SDTM output package, and issue files appear here after the run.
              </p>

              <div className="mt-6 grid gap-3">
                {job && Object.keys(job.artifacts).length ? (
                  Object.entries(job.artifacts).map(([label, url]) => (
                    <a
                      key={label}
                      href={`${API_BASE_URL}${url}`}
                      target="_blank"
                      rel="noreferrer"
                      className="flex items-center justify-between rounded-2xl bg-slate-50 px-5 py-4 text-sm text-slate-800 transition hover:bg-slate-100"
                    >
                      <span>{label}</span>
                      <Download className="h-4 w-4" />
                    </a>
                  ))
                ) : (
                  <div className="rounded-[1.5rem] bg-slate-50 p-5 text-sm leading-7 text-slate-600">
                    Run a file to populate the downloadable outputs.
                  </div>
                )}
              </div>

              {job?.metrics && Object.keys(job.metrics).length ? (
                <div className="mt-6 grid gap-3 sm:grid-cols-2">
                  {Object.entries(job.metrics).map(([key, value]) => (
                    <div key={key} className="rounded-[1.5rem] bg-slate-50 p-5">
                      <div className="text-sm text-slate-500">{key}</div>
                      <div className="mt-2 text-2xl font-semibold text-slate-900">{String(value)}</div>
                    </div>
                  ))}
                </div>
              ) : null}

              {job?.error ? <div className="mt-6 rounded-[1.5rem] border border-red-200 bg-red-50 p-5 text-sm text-red-700">{job.error}</div> : null}
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto mt-14 max-w-7xl px-6">
        <div className="card-glass overflow-hidden p-10">
          <div className="grid items-center gap-8 lg:grid-cols-[1fr_auto]">
            <div>
              <div className="text-sm uppercase tracking-[0.2em] text-slate-400">Sales conversation</div>
              <h2 className="mt-3 text-4xl font-semibold leading-tight">Want this presented as a customer-facing pilot on your domain?</h2>
              <p className="mt-4 max-w-3xl text-lg leading-8 text-slate-600">
                Keep the product story clean: upload raw data, watch the platform explain itself, and walk out with issue logs,
                spec files, and SDTM outputs in one sitting.
              </p>
            </div>
            <a
              href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Pilot%20Discussion"
              className="inline-flex items-center justify-center gap-2 rounded-2xl bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-500 px-7 py-4 text-sm font-medium text-white shadow-lg shadow-pink-500/20"
            >
              Contact sales <ArrowRight className="h-4 w-4" />
            </a>
          </div>
        </div>
      </section>
    </main>
  );
}
