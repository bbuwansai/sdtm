"use client";

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import Link from "next/link";
import {
  ArrowRight,
  Copy,
  Download,
  FileSpreadsheet,
  FileUp,
  FlaskConical,
  ScanSearch,
  ShieldCheck,
  Sparkles,
} from "lucide-react";
import { API_BASE_URL, type JobSummary, type TimelineEvent } from "@/lib/api";
import { BackgroundGlow, SiteHeader } from "@/components/site-shell";

type Detection = {
  domain: string;
  matched_columns: string[];
};

type DomainOption = "AUTO" | "DM" | "VS" | "LB" | "AE";

const domainOptions: DomainOption[] = ["AUTO", "DM", "VS", "LB", "AE"];

function StatusPill({ children }: { children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-slate-900/10 bg-white/75 px-3 py-1 text-xs text-slate-700 backdrop-blur">
      {children}
    </span>
  );
}

function levelClass(level: TimelineEvent["level"]) {
  if (level === "success") return "text-emerald-600";
  if (level === "error") return "text-red-600";
  if (level === "warning") return "text-amber-600";
  return "text-purple-600";
}

function levelLabel(level: TimelineEvent["level"]) {
  if (level === "success") return "OK";
  if (level === "error") return "ERR";
  if (level === "warning") return "WARN";
  return "INFO";
}

export default function PlatformPage() {
  const [file, setFile] = useState<File | null>(null);
  const [selectedDomain, setSelectedDomain] = useState<DomainOption>("AUTO");
  const [detection, setDetection] = useState<Detection | null>(null);
  const [job, setJob] = useState<JobSummary | null>(null);
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState(false);
  const [detecting, setDetecting] = useState(false);
  const pollRef = useRef<number | null>(null);

  useEffect(() => {
    return () => {
      if (pollRef.current !== null) {
        window.clearInterval(pollRef.current);
      }
    };
  }, []);

  const timeline: TimelineEvent[] =
    job?.timeline?.length && Array.isArray(job.timeline)
      ? job.timeline
      : [
          {
            time: new Date().toISOString(),
            level: "info",
            message: "Waiting for file upload.",
          },
        ];

  const detectedLabel = useMemo(() => {
    if (selectedDomain !== "AUTO") return selectedDomain;
    if (detecting) return "DETECTING...";
    if (!detection) return file ? "UNKNOWN" : "UNKNOWN";
    return detection.domain || "UNKNOWN";
  }, [detecting, detection, file, selectedDomain]);

  const issueCount = useMemo(
    () => timeline.filter((event) => event.level === "error" || event.level === "warning").length,
    [timeline],
  );

  const logText = useMemo(
    () =>
      timeline
        .map((event) => `[${new Date(event.time).toLocaleTimeString()}] ${levelLabel(event.level)} ${event.message}`)
        .join("\n"),
    [timeline],
  );

  async function detectDomain(nextFile?: File | null) {
    const activeFile = nextFile ?? file;
    if (!activeFile) {
      setDetection(null);
      return;
    }

    if (selectedDomain !== "AUTO") {
      setDetection({
        domain: selectedDomain,
        matched_columns: [],
      });
      return;
    }

    setDetecting(true);

    try {
      const form = new FormData();
      form.append("file", activeFile);

      const res = await fetch(`${API_BASE_URL}/api/detect-domain`, {
        method: "POST",
        body: form,
      });

      if (!res.ok) {
        throw new Error("Domain detection failed");
      }

      const data: unknown = await res.json();
      const parsed = data as Partial<Detection>;

      setDetection({
        domain: parsed.domain ?? "UNKNOWN",
        matched_columns: Array.isArray(parsed.matched_columns) ? parsed.matched_columns : [],
      });
    } catch {
      setDetection({
        domain: "UNKNOWN",
        matched_columns: [],
      });
    } finally {
      setDetecting(false);
    }
  }

  async function fetchJob(jobId: string) {
    const res = await fetch(`${API_BASE_URL}/api/jobs/${jobId}`);
    if (!res.ok) return;

    const data: JobSummary = await res.json();
    setJob(data);

    if ((data.status === "completed" || data.status === "failed") && pollRef.current !== null) {
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
    form.append("domain", selectedDomain === "AUTO" ? detection?.domain ?? "AUTO" : selectedDomain);

    const res = await fetch(`${API_BASE_URL}/api/jobs`, {
      method: "POST",
      body: form,
    });

    if (!res.ok) {
      setBusy(false);
      throw new Error("Pipeline job could not be created");
    }

    const data: { job_id: string } = await res.json();

    await fetchJob(data.job_id);

    pollRef.current = window.setInterval(() => {
      void fetchJob(data.job_id);
    }, 1500);
  }

  async function copyLogs() {
    try {
      await navigator.clipboard.writeText(logText);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1800);
    } catch {
      setCopied(false);
    }
  }

  return (
    <main className="min-h-screen bg-background text-slate-900">
      <SiteHeader />

      <section
        className="relative mx-auto max-w-7xl overflow-hidden rounded-[2rem] px-6 pb-20 pt-14"
        style={{
          background:
            "linear-gradient(180deg, rgba(139,92,246,0.08), rgba(236,72,153,0.06) 35%, rgba(255,255,255,0.78))",
        }}
      >
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
              <span className="gradient-text">Watch one clean run console.</span>
            </h1>

            <p className="mt-6 max-w-2xl text-xl leading-8 text-slate-700">
              Keep the customer view simple: upload the file, auto-detect the domain, run the pipeline,
              and see every update in one compact place.
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
                Show meaningful QC findings without dumping the user into an overwhelming list of cards.
              </p>
            </div>
            <div className="card-glass p-7">
              <div className="flex items-center gap-2 text-sm text-slate-700">
                <FileSpreadsheet className="h-4 w-4" /> Mapping logic
              </div>
              <div className="mt-4 text-4xl font-semibold leading-tight">Spec generation</div>
              <p className="mt-3 text-base leading-7 text-slate-600">
                Build the spec package from raw inputs so the customer sees the transformation logic turn into reusable metadata.
              </p>
            </div>
            <div className="card-glass p-7 sm:col-span-2 lg:col-span-1 xl:col-span-2">
              <div className="flex items-center gap-2 text-sm text-slate-700">
                <ShieldCheck className="h-4 w-4" /> Final package
              </div>
              <div className="mt-4 text-4xl font-semibold leading-tight">One run view, full output set</div>
              <p className="mt-3 max-w-xl text-base leading-7 text-slate-600">
                Keep the run readable in a single console, then let the downloads speak: issue logs, layer 1 files,
                spec workbook, and SDTM outputs.
              </p>
            </div>
          </div>
        </div>
      </section>

      <section id="demo-workspace" className="mx-auto mt-12 max-w-7xl px-6">
        <div className="grid gap-6 xl:grid-cols-[0.95fr_1.05fr]">
          <div className="space-y-6">
            <div className="card-glass p-8">
              <div className="flex items-center gap-3">
                <FileUp className="h-5 w-5 text-purple-600" />
                <h2 className="text-2xl font-semibold">Upload raw data</h2>
              </div>
              <p className="mt-3 max-w-2xl text-base leading-7 text-slate-600">
                Upload the source file. Domain detection runs automatically after file selection, or you can force a specific domain manually.
              </p>

              <div className="mt-6 rounded-[2rem] border border-dashed border-slate-300 bg-white/60 p-8">
                <input
                  type="file"
                  accept=".csv,.xlsx,.xls"
                  onChange={(e) => {
                    const nextFile = e.target.files?.[0] ?? null;
                    setFile(nextFile);
                    setJob(null);
                    setDetection(null);

                    if (nextFile && selectedDomain === "AUTO") {
                      void detectDomain(nextFile);
                    }
                  }}
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
                <h2 className="text-2xl font-semibold">Run controls</h2>
              </div>

              <div className="mt-6 grid gap-4 lg:grid-cols-[220px_1fr]">
                <select
                  value={selectedDomain}
                  onChange={(e) => {
                    const next = e.target.value as DomainOption;
                    setSelectedDomain(next);

                    if (next === "AUTO") {
                      void detectDomain();
                    } else {
                      setDetection({ domain: next, matched_columns: [] });
                    }
                  }}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-4 text-sm outline-none"
                >
                  {domainOptions.map((option) => (
                    <option key={option} value={option}>
                      {option === "AUTO" ? "Auto detect" : option}
                    </option>
                  ))}
                </select>

                <button
                  disabled={!file || busy || (selectedDomain === "AUTO" && !detection) || detecting}
                  onClick={() => void runPipeline()}
                  className="rounded-2xl bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-500 px-5 py-4 text-sm font-medium text-white shadow-lg disabled:opacity-50"
                >
                  {busy ? "Processing…" : detecting ? "Detecting domain…" : "Run transformation"}
                </button>
              </div>

              <div className="mt-6 grid gap-4 md:grid-cols-2">
                <div className="rounded-[1.5rem] bg-slate-50 p-6">
                  <div className="text-sm font-medium text-slate-900">Detected domain</div>
                  <div className="mt-2 text-2xl font-semibold text-slate-900">{detectedLabel}</div>
                </div>
                <div className="rounded-[1.5rem] bg-slate-50 p-6">
                  <div className="text-sm font-medium text-slate-900">Matched columns</div>
                  <div className="mt-2 text-sm leading-7 text-slate-600 max-h-32 overflow-auto">
                    {detection?.matched_columns?.length
                      ? detection.matched_columns.join(", ")
                      : "Matched-column evidence will appear here automatically after file upload."}
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div className="space-y-6">
            <div className="card-glass p-8">
              <div className="flex items-center justify-between gap-4">
                <div>
                  <h2 className="text-2xl font-semibold">Run console</h2>
                  <p className="mt-2 text-base leading-7 text-slate-600">
                    One compact place for the full run: detection, QC, spec generation, SDTM creation, and errors when they happen.
                  </p>
                </div>
                <button
                  onClick={() => void copyLogs()}
                  className="inline-flex items-center gap-2 rounded-2xl border border-slate-900/10 bg-white px-4 py-3 text-sm font-medium text-slate-800"
                >
                  <Copy className="h-4 w-4" /> {copied ? "Copied" : "Copy logs"}
                </button>
              </div>

              <div className="mt-6 grid gap-3 sm:grid-cols-3">
                <div className="rounded-[1.5rem] bg-slate-50 p-5">
                  <div className="text-sm text-slate-500">Status</div>
                  <div className="mt-2 text-xl font-semibold text-slate-900">{job?.status ?? (busy ? "running" : "idle")}</div>
                </div>
                <div className="rounded-[1.5rem] bg-slate-50 p-5">
                  <div className="text-sm text-slate-500">Current step</div>
                  <div className="mt-2 text-xl font-semibold text-slate-900">{job?.current_step ?? "Waiting"}</div>
                </div>
                <div className="rounded-[1.5rem] bg-slate-50 p-5">
                  <div className="text-sm text-slate-500">Flagged log lines</div>
                  <div className="mt-2 text-xl font-semibold text-slate-900">{issueCount}</div>
                </div>
              </div>

              <div className="mt-6 overflow-hidden rounded-[1.5rem] border border-slate-200 bg-[#0b1020] shadow-inner">
                <div className="flex items-center justify-between border-b border-white/10 px-5 py-3 text-xs uppercase tracking-[0.18em] text-slate-400">
                  <span>Live process output</span>
                  <span>{timeline.length} lines</span>
                </div>
                <div className="max-h-[28rem] overflow-auto px-5 py-4 font-mono text-sm leading-7 text-slate-100">
                  {timeline.map((event, index) => (
                    <div
                      key={`${event.time}-${index}`}
                      className="grid grid-cols-[72px_52px_1fr] gap-3 border-b border-white/5 py-2 last:border-b-0"
                    >
                      <span className="text-slate-500">{new Date(event.time).toLocaleTimeString()}</span>
                      <span className={levelClass(event.level)}>{levelLabel(event.level)}</span>
                      <span className="whitespace-pre-wrap break-words">{event.message}</span>
                    </div>
                  ))}
                </div>
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

              {job?.error ? (
                <div className="mt-6 rounded-[1.5rem] border border-red-200 bg-red-50 p-5 text-sm text-red-700">
                  {job.error}
                </div>
              ) : null}
            </div>
          </div>
        </div>
      </section>

      <section className="mx-auto mt-14 max-w-7xl px-6">
        <div className="card-glass overflow-hidden p-10">
          <div className="grid items-center gap-8 lg:grid-cols-[1fr_auto]">
            <div>
              <div className="text-sm uppercase tracking-[0.2em] text-slate-400">Sales conversation</div>
              <h2 className="mt-3 text-4xl font-semibold leading-tight">Need a cleaner customer-facing run on your domain?</h2>
              <p className="mt-4 max-w-3xl text-lg leading-8 text-slate-600">
                Keep the product story tight: upload raw data, watch the console update in one place, and leave with issue logs, spec files, and SDTM outputs.
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
