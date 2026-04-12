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
  const [reviewedHumanFile, setReviewedHumanFile] = useState<File | null>(null);
  const [phase, setPhase] = useState<"layer1_spec" | "sdtm">("layer1_spec");
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

  async function runPipeline(nextPhase?: "layer1_spec" | "sdtm") {
    const activePhase = nextPhase ?? phase;

    if (activePhase === "layer1_spec") {
      if (!file) return;

      setBusy(true);
      setPhase("layer1_spec");
      setJob(null);

      const form = new FormData();
      form.append("file", file);
      form.append("domain", selectedDomain === "AUTO" ? detection?.domain ?? "AUTO" : selectedDomain);
      form.append("phase", "layer1_spec");

      const res = await fetch(`${API_BASE_URL}/api/jobs`, {
        method: "POST",
        body: form,
      });

      if (!res.ok) {
        setBusy(false);
        throw new Error("Layer 1 + Spec job could not be created");
      }

      const data: { job_id: string } = await res.json();
      await fetchJob(data.job_id);

      pollRef.current = window.setInterval(() => {
        void fetchJob(data.job_id);
      }, 1500);
      return;
    }

    const existingJobId = (job as (JobSummary & { job_id?: string }) | null)?.job_id;
    if (!existingJobId) {
      throw new Error("Run Layer 1 + Spec first so an existing job workspace is available.");
    }
    if (!reviewedHumanFile) {
      throw new Error("Please upload the reviewed human issue file before running SDTM.");
    }

    setBusy(true);
    setPhase("sdtm");

    const form = new FormData();
    form.append("reviewed_human_file", reviewedHumanFile);

    const res = await fetch(`${API_BASE_URL}/api/jobs/${existingJobId}/run-sdtm`, {
      method: "POST",
      body: form,
    });

    if (!res.ok) {
      setBusy(false);
      throw new Error("SDTM job could not be started");
    }

    await fetchJob(existingJobId);

    pollRef.current = window.setInterval(() => {
      void fetchJob(existingJobId);
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

  const existingJobId = (job as (JobSummary & { job_id?: string }) | null)?.job_id;

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
              <span className="gradient-text">Review human issues. Then run SDTM.</span>
            </h1>

            <p className="mt-6 max-w-2xl text-xl leading-8 text-slate-700">
              Keep the workflow believable: first run Layer 1 and spec, download the human-review issue log, update it
              externally, then upload the reviewed file back into the same job for SDTM generation.
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
                Surface the human-review issue log immediately so the user can download it before SDTM.
              </p>
            </div>
            <div className="card-glass p-7">
              <div className="flex items-center gap-2 text-sm text-slate-700">
                <FileSpreadsheet className="h-4 w-4" /> Mapping logic
              </div>
              <div className="mt-4 text-4xl font-semibold leading-tight">Spec generation</div>
              <p className="mt-3 text-base leading-7 text-slate-600">
                Keep spec generation in phase 1 so the metadata package is ready when SDTM starts.
              </p>
            </div>
            <div className="card-glass p-7 sm:col-span-2 lg:col-span-1 xl:col-span-2">
              <div className="flex items-center gap-2 text-sm text-slate-700">
                <ShieldCheck className="h-4 w-4" /> Final package
              </div>
              <div className="mt-4 text-4xl font-semibold leading-tight">Same job → SDTM</div>
              <p className="mt-3 max-w-xl text-base leading-7 text-slate-600">
                The SDTM phase runs against the same existing job workspace after the reviewed human issue log is
                uploaded back in.
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
                <h2 className="text-2xl font-semibold">Upload files</h2>
              </div>
              <p className="mt-3 max-w-2xl text-base leading-7 text-slate-600">
                Upload the raw source file first. After phase 1 completes, upload the reviewed human issue log to the
                same job and run SDTM.
              </p>

              <div className="mt-6 rounded-[2rem] border border-dashed border-slate-300 bg-white/60 p-8">
                <label className="mb-3 block text-sm font-medium text-slate-700">Raw data file</label>
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

              <div className="mt-6 rounded-[2rem] border border-dashed border-slate-300 bg-white/60 p-8">
                <label className="mb-3 block text-sm font-medium text-slate-700">
                  Reviewed human issue log file
                </label>
                <input
                  type="file"
                  accept=".csv"
                  onChange={(e) => {
                    const nextFile = e.target.files?.[0] ?? null;
                    setReviewedHumanFile(nextFile);
                  }}
                  className="block w-full text-base text-slate-700"
                />
                <div className="mt-5 flex flex-wrap gap-3 text-sm text-slate-500">
                  <span>Upload this only after phase 1 completes.</span>
                  {reviewedHumanFile ? (
                    <span className="font-medium text-slate-800">Selected: {reviewedHumanFile.name}</span>
                  ) : null}
                </div>
              </div>
            </div>

            <div className="card-glass p-8">
              <div className="flex items-center gap-3">
                <ScanSearch className="h-5 w-5 text-purple-600" />
                <h2 className="text-2xl font-semibold">Run controls</h2>
              </div>

              <div className="mt-6">
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
                  className="w-full rounded-2xl border border-slate-200 bg-white px-4 py-4 text-sm outline-none"
                >
                  {domainOptions.map((option) => (
                    <option key={option} value={option}>
                      {option === "AUTO" ? "Auto detect" : option}
                    </option>
                  ))}
                </select>
              </div>

              <div className="mt-4 grid gap-3 sm:grid-cols-2">
                <button
                  disabled={!file || busy || (selectedDomain === "AUTO" && !detection) || detecting}
                  onClick={() => {
                    setPhase("layer1_spec");
                    void runPipeline("layer1_spec");
                  }}
                  className="rounded-2xl bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-500 px-5 py-4 text-sm font-medium text-white shadow-lg disabled:opacity-50"
                >
                  {busy && phase === "layer1_spec" ? "Processing…" : detecting ? "Detecting domain…" : "Run Layer 1 + Spec"}
                </button>

                <button
                  disabled={!existingJobId || !reviewedHumanFile || busy}
                  onClick={() => {
                    setPhase("sdtm");
                    void runPipeline("sdtm");
                  }}
                  className="rounded-2xl border border-slate-900/10 bg-white px-5 py-4 text-sm font-medium text-slate-900 shadow-lg disabled:opacity-50"
                >
                  {busy && phase === "sdtm" ? "Processing…" : "Run SDTM"}
                </button>
              </div>

              <div className="mt-6 grid gap-4 md:grid-cols-2">
                <div className="rounded-[1.5rem] bg-slate-50 p-6">
                  <div className="text-sm font-medium text-slate-900">Detected domain</div>
                  <div className="mt-2 text-2xl font-semibold text-slate-900">{detectedLabel}</div>
                </div>
                <div className="rounded-[1.5rem] bg-slate-50 p-6">
                  <div className="text-sm font-medium text-slate-900">Active job id</div>
                  <div className="mt-2 break-all text-sm leading-7 text-slate-600">
                    {existingJobId ?? "Run Layer 1 + Spec to create a reusable job workspace."}
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
                    One compact place for the full run: detection, Layer 1 QC, spec generation, then SDTM on the same
                    job once the reviewed human log is uploaded.
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
                After phase 1, download the human review issue log and spec package here. After phase 2, the SDTM
                output package will appear here for the same job.
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
                    Run Layer 1 + Spec to generate the human-review file first. Then upload the reviewed file and run
                    SDTM against the same job.
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
              <h2 className="mt-3 text-4xl font-semibold leading-tight">Need a cleaner customer-facing workflow?</h2>
              <p className="mt-4 max-w-3xl text-lg leading-8 text-slate-600">
                Keep the story tight: upload raw data, review Layer 1 human issues, re-upload the reviewed log, then
                produce SDTM outputs inside the same job.
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
