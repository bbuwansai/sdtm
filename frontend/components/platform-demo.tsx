"use client";

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import Link from "next/link";
import { ArrowRight, Copy, Download, FileSpreadsheet, FileUp, ScanSearch, ShieldCheck, Sparkles } from "lucide-react";
import { API_BASE_URL, type JobSummary, type TimelineEvent } from "@/lib/api";

type Detection = {
  domain: string;
  matched_columns: string[];
};

type DomainOption = "AUTO" | "DM" | "VS" | "LB" | "AE";

const domainOptions: DomainOption[] = ["AUTO", "DM", "VS", "LB", "AE"];

function StatusPill({ children }: { children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-[#efe6db] backdrop-blur">
      {children}
    </span>
  );
}

function levelClass(level: TimelineEvent["level"]) {
  if (level === "success") return "text-[#f4d4a5]";
  if (level === "error") return "text-rose-300";
  if (level === "warning") return "text-amber-300";
  return "text-[#f4d4a5]";
}

function levelLabel(level: TimelineEvent["level"]) {
  if (level === "success") return "OK";
  if (level === "error") return "ERR";
  if (level === "warning") return "WARN";
  return "INFO";
}

export function PlatformDemo({ compact = false }: { compact?: boolean }) {
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
      if (pollRef.current !== null) window.clearInterval(pollRef.current);
    };
  }, []);

  const timeline: TimelineEvent[] =
    job?.timeline?.length && Array.isArray(job.timeline)
      ? job.timeline
      : [{ time: new Date().toISOString(), level: "info", message: "Waiting for file upload." }];

  const detectedLabel = useMemo(() => {
    if (selectedDomain !== "AUTO") return selectedDomain;
    if (detecting) return "DETECTING...";
    if (!detection) return file ? "UNKNOWN" : "UNKNOWN";
    return detection.domain || "UNKNOWN";
  }, [detecting, detection, file, selectedDomain]);

  const logText = useMemo(
    () => timeline.map((event) => `[${new Date(event.time).toLocaleTimeString()}] ${levelLabel(event.level)} ${event.message}`).join("\n"),
    [timeline],
  );

  async function detectDomain(nextFile?: File | null) {
    const activeFile = nextFile ?? file;
    if (!activeFile) {
      setDetection(null);
      return;
    }

    if (selectedDomain !== "AUTO") {
      setDetection({ domain: selectedDomain, matched_columns: [] });
      return;
    }

    setDetecting(true);
    try {
      const form = new FormData();
      form.append("file", activeFile);
      const res = await fetch(`${API_BASE_URL}/api/detect-domain`, { method: "POST", body: form });
      if (!res.ok) throw new Error("Domain detection failed");
      const data = (await res.json()) as Partial<Detection>;
      setDetection({
        domain: data.domain ?? "UNKNOWN",
        matched_columns: Array.isArray(data.matched_columns) ? data.matched_columns : [],
      });
    } catch {
      setDetection({ domain: "UNKNOWN", matched_columns: [] });
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
      const res = await fetch(`${API_BASE_URL}/api/jobs`, { method: "POST", body: form });
      if (!res.ok) {
        setBusy(false);
        throw new Error("Layer 1 + Spec job could not be created");
      }
      const data = (await res.json()) as { job_id: string };
      await fetchJob(data.job_id);
      pollRef.current = window.setInterval(() => void fetchJob(data.job_id), 1500);
      return;
    }

    const existingJobId = (job as (JobSummary & { job_id?: string }) | null)?.job_id;
    if (!existingJobId || !reviewedHumanFile) return;
    setBusy(true);
    setPhase("sdtm");
    const form = new FormData();
    form.append("reviewed_human_file", reviewedHumanFile);
    const res = await fetch(`${API_BASE_URL}/api/jobs/${existingJobId}/run-sdtm`, { method: "POST", body: form });
    if (!res.ok) {
      setBusy(false);
      throw new Error("SDTM job could not be started");
    }
    await fetchJob(existingJobId);
    pollRef.current = window.setInterval(() => void fetchJob(existingJobId), 1500);
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
  const issueCount = timeline.filter((event) => event.level === "error" || event.level === "warning").length;

  return (
    <section id="demo-workspace" className={compact ? "" : "mx-auto max-w-7xl px-6 py-10 lg:px-10"}>
      {!compact ? (
        <div className="mb-8 flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div className="max-w-3xl">
            <StatusPill>
              <Sparkles className="h-4 w-4 text-[#f4d4a5]" /> Live product demo
            </StatusPill>
            <h2 className="mt-4 text-3xl font-semibold tracking-tight text-white md:text-4xl">
              Try the workflow directly from the site.
            </h2>
            <p className="mt-3 text-base leading-8 text-[#ddd2c4]">
              Upload a dataset, run Layer 1 QC and spec generation, then continue into the SDTM step from the same job.
            </p>
          </div>
          <Link
            href="/platform"
            className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-5 py-3 text-sm font-semibold text-white backdrop-blur transition hover:bg-white/10"
          >
            Full-screen platform <ArrowRight className="h-4 w-4" />
          </Link>
        </div>
      ) : null}

      <div className={`grid gap-6 ${compact ? "xl:grid-cols-[0.9fr_1.1fr]" : "xl:grid-cols-[0.95fr_1.05fr]"}`}>
        <div className="space-y-6">
          <div className="rounded-[2rem] border border-white/10 bg-white/[0.045] p-7 backdrop-blur-xl">
            <div className="flex items-center gap-3">
              <FileUp className="h-5 w-5 text-[#f4d4a5]" />
              <h3 className="text-2xl font-semibold text-white">Upload files</h3>
            </div>
            <p className="mt-3 text-sm leading-7 text-[#ddd2c4]">
              Start with the raw source file. After phase 1 completes, upload the reviewed human issue log and continue into SDTM.
            </p>
            <div className="mt-5 flex flex-wrap gap-2 text-xs uppercase tracking-[0.18em] text-[#f4d4a5]">
              {['Upload', 'QC + Spec', 'Review', 'SDTM'].map((step) => (
                <span key={step} className="rounded-full border border-white/10 bg-white/5 px-3 py-2">
                  {step}
                </span>
              ))}
            </div>

            <div className="mt-6 rounded-[1.75rem] border border-dashed border-white/15 bg-[#110d09]/55 p-6">
              <label className="mb-3 block text-sm font-medium text-[#efe6db]">Raw data file</label>
              <input
                type="file"
                accept=".csv,.xlsx,.xls"
                onChange={(e) => {
                  const nextFile = e.target.files?.[0] ?? null;
                  setFile(nextFile);
                  setJob(null);
                  setDetection(null);
                  if (nextFile && selectedDomain === "AUTO") void detectDomain(nextFile);
                }}
                className="block w-full text-sm text-[#ddd2c4] file:mr-4 file:rounded-xl file:border-0 file:bg-emerald-400 file:px-4 file:py-2 file:font-medium file:text-[#1a120a]"
              />
              <div className="mt-4 text-sm text-[#b8afa2]">Accepted: CSV, XLSX, XLS</div>
              {file ? <div className="mt-2 text-sm font-medium text-white">Selected: {file.name}</div> : null}
            </div>

            <div className="mt-5 rounded-[1.75rem] border border-dashed border-white/15 bg-[#110d09]/55 p-6">
              <label className="mb-3 block text-sm font-medium text-[#efe6db]">Reviewed human issue log file</label>
              <input
                type="file"
                accept=".csv"
                onChange={(e) => setReviewedHumanFile(e.target.files?.[0] ?? null)}
                className="block w-full text-sm text-[#ddd2c4] file:mr-4 file:rounded-xl file:border-0 file:bg-white file:px-4 file:py-2 file:font-medium file:text-[#1a120a]"
              />
              <div className="mt-4 text-sm text-[#b8afa2]">Upload this after phase 1 completes.</div>
              {reviewedHumanFile ? <div className="mt-2 text-sm font-medium text-white">Selected: {reviewedHumanFile.name}</div> : null}
            </div>
          </div>

          <div className="rounded-[2rem] border border-white/10 bg-white/[0.045] p-7 backdrop-blur-xl">
            <div className="flex items-center gap-3">
              <ScanSearch className="h-5 w-5 text-[#f4d4a5]" />
              <h3 className="text-2xl font-semibold text-white">Run controls</h3>
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
                className="w-full rounded-2xl border border-white/10 bg-slate-950/60 px-4 py-4 text-sm text-white outline-none"
              >
                {domainOptions.map((option) => (
                  <option key={option} value={option}>
                    {option === "AUTO" ? "Auto detect" : option}
                  </option>
                ))}
              </select>
            </div>

            <div className="mt-5 grid gap-3 sm:grid-cols-2">
              <button
                disabled={!file || busy || (selectedDomain === "AUTO" && !detection) || detecting}
                onClick={() => {
                  setPhase("layer1_spec");
                  void runPipeline("layer1_spec");
                }}
                className="rounded-2xl bg-gradient-to-r from-[#f6e1c4] via-[#f3d2a4] to-[#cb9453] px-5 py-4 text-sm font-semibold text-[#1a120a] shadow-lg disabled:opacity-50"
              >
                {busy && phase === "layer1_spec" ? "Processing…" : detecting ? "Detecting domain…" : "Run Layer 1 + Spec"}
              </button>
              <button
                disabled={!existingJobId || !reviewedHumanFile || busy}
                onClick={() => {
                  setPhase("sdtm");
                  void runPipeline("sdtm");
                }}
                className="rounded-2xl border border-white/10 bg-white/5 px-5 py-4 text-sm font-semibold text-white disabled:opacity-50"
              >
                {busy && phase === "sdtm" ? "Processing…" : "Run SDTM"}
              </button>
            </div>

            <div className="mt-6 grid gap-4 md:grid-cols-2">
              <div className="rounded-[1.5rem] bg-[#110d09]/55 p-5">
                <div className="text-sm text-[#b8afa2]">Detected domain</div>
                <div className="mt-2 text-2xl font-semibold text-white">{detectedLabel}</div>
              </div>
              <div className="rounded-[1.5rem] bg-[#110d09]/55 p-5">
                <div className="text-sm text-[#b8afa2]">Active job id</div>
                <div className="mt-2 break-all text-sm leading-7 text-[#ddd2c4]">
                  {existingJobId ?? "Run Layer 1 + Spec to create a reusable job workspace."}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="rounded-[2rem] border border-white/10 bg-white/[0.045] p-7 backdrop-blur-xl">
            <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
              <div>
                <h3 className="text-2xl font-semibold text-white">Run console</h3>
                <p className="mt-3 text-sm leading-7 text-[#ddd2c4]">
                  One place for detection, Layer 1 QC, spec generation, and SDTM progression in the same job.
                </p>
                <div className={`mt-4 rounded-2xl border px-4 py-3 text-sm ${busy ? 'border-[#f0c58d]/25 bg-[#cb9453]/10 text-[#f5eee4]' : job?.status === 'failed' ? 'border-rose-400/20 bg-rose-400/10 text-rose-100' : job?.status === 'completed' ? 'border-emerald-300/20 bg-emerald-300/10 text-emerald-50' : 'border-white/10 bg-white/[0.04] text-[#efe6db]'}`}>
                  {busy
                    ? 'Run in progress. Keep this page open while logs and artifacts update.'
                    : job?.status === 'failed'
                      ? 'The latest run failed. Review the console output and try again.'
                      : job?.status === 'completed'
                        ? 'Latest run completed. Review outputs and continue to the next step if needed.'
                        : 'Ready to start. Upload a file, confirm the domain, and launch the workflow.'}
                </div>
              </div>
              <button
                onClick={() => void copyLogs()}
                className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-medium text-white"
              >
                <Copy className="h-4 w-4" /> {copied ? "Copied" : "Copy logs"}
              </button>
            </div>

            <div className="mt-6 grid gap-3 sm:grid-cols-3">
              <div className="rounded-[1.5rem] bg-[#110d09]/55 p-5">
                <div className="text-sm text-[#b8afa2]">Status</div>
                <div className="mt-2 text-xl font-semibold text-white">{job?.status ?? (busy ? "running" : "idle")}</div>
              </div>
              <div className="rounded-[1.5rem] bg-[#110d09]/55 p-5">
                <div className="text-sm text-[#b8afa2]">Current step</div>
                <div className="mt-2 text-xl font-semibold text-white">{job?.current_step ?? "Waiting"}</div>
              </div>
              <div className="rounded-[1.5rem] bg-[#110d09]/55 p-5">
                <div className="text-sm text-[#b8afa2]">Flagged log lines</div>
                <div className="mt-2 text-xl font-semibold text-white">{issueCount}</div>
              </div>
            </div>

            <div className="mt-6 overflow-hidden rounded-[1.5rem] border border-white/10 bg-[#0d0906] shadow-inner">
              <div className="flex items-center justify-between border-b border-white/10 px-5 py-3 text-xs uppercase tracking-[0.18em] text-[#b8afa2]">
                <span>Live process output</span>
                <span>{timeline.length} lines</span>
              </div>
              <div className={`${compact ? "max-h-[22rem]" : "max-h-[28rem]"} overflow-auto px-5 py-4 font-mono text-sm leading-7 text-[#f5eee4]`}>
                {timeline.map((event, index) => (
                  <div key={`${event.time}-${index}`} className="grid grid-cols-[72px_52px_1fr] gap-3 border-b border-white/5 py-2 last:border-b-0">
                    <span className="text-[#8f8579]">{new Date(event.time).toLocaleTimeString()}</span>
                    <span className={levelClass(event.level)}>{levelLabel(event.level)}</span>
                    <span className="whitespace-pre-wrap break-words">{event.message}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="rounded-[2rem] border border-white/10 bg-white/[0.045] p-7 backdrop-blur-xl">
            <div className="flex items-center gap-3">
              <ShieldCheck className="h-5 w-5 text-[#f4d4a5]" />
              <h3 className="text-2xl font-semibold text-white">Outputs</h3>
            </div>
            <p className="mt-3 text-sm leading-7 text-[#ddd2c4]">
              Download the human review issue log, spec package, and final SDTM artifacts from the same workspace.
            </p>

            <div className="mt-6 grid gap-5">
              {job && Object.keys(job.artifacts).length ? (
                ([
                  {
                    title: 'QC + review artifacts',
                    items: Object.entries(job.artifacts).filter(([label]) => /issue|clean|spec|review|layer/i.test(label)),
                  },
                  {
                    title: 'SDTM + final outputs',
                    items: Object.entries(job.artifacts).filter(([label]) => /sdtm|exception|final|define|xpt/i.test(label)),
                  },
                  {
                    title: 'Other downloads',
                    items: Object.entries(job.artifacts).filter(([label]) => !/issue|clean|spec|review|layer|sdtm|exception|final|define|xpt/i.test(label)),
                  },
                ] as const)
                  .filter((group) => group.items.length)
                  .map((group) => (
                    <div key={group.title} className="rounded-[1.5rem] border border-white/10 bg-[#110d09]/40 p-4">
                      <div className="mb-3 text-xs uppercase tracking-[0.18em] text-[#f4d4a5]">{group.title}</div>
                      <div className="grid gap-3">
                        {group.items.map(([label, url]) => (
                          <a
                            key={label}
                            href={`${API_BASE_URL}${url}`}
                            target="_blank"
                            rel="noreferrer"
                            className="flex items-center justify-between rounded-2xl bg-[#110d09]/55 px-5 py-4 text-sm text-[#f5eee4] transition hover:bg-[#17110c]"
                          >
                            <span>{label}</span>
                            <Download className="h-4 w-4" />
                          </a>
                        ))}
                      </div>
                    </div>
                  ))
              ) : (
                <div className="rounded-[1.5rem] bg-[#110d09]/55 p-5 text-sm leading-7 text-[#ddd2c4]">
                  Run a job to see downloadable artifacts here.
                </div>
              )}
            </div>

            {compact ? (
              <div className="mt-6 rounded-[1.5rem] border border-[#f0c58d]/20 bg-[#cb9453]/10 p-5 text-sm leading-7 text-[#efe6db]">
                Need more room for the workflow? Use the dedicated platform page for the same demo with a larger workspace.
                <div>
                  <Link href="/platform" className="mt-4 inline-flex items-center gap-2 font-semibold text-[#f4d4a5]">
                    Open klinai.tech/platform <ArrowRight className="h-4 w-4" />
                  </Link>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </section>
  );
}
