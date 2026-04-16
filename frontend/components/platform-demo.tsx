"use client";

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import Link from "next/link";
import { ArrowRight, CheckCircle2, CircleAlert, Copy, Download, FileSpreadsheet, FileUp, ScanSearch, ShieldCheck, Sparkles } from "lucide-react";
import { API_BASE_URL, type JobSummary, type TimelineEvent } from "@/lib/api";

type Detection = {
  domain: string;
  matched_columns: string[];
};

type DomainOption = "AUTO" | "DM" | "VS" | "LB" | "AE";

const domainOptions: DomainOption[] = ["AUTO", "DM", "VS", "LB", "AE"];

function StatusPill({ children }: { children: ReactNode }) {
  return (
    <span className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/5 px-3 py-1 text-xs text-slate-200 backdrop-blur">
      {children}
    </span>
  );
}

function StepPill({ label, active, complete }: { label: string; active?: boolean; complete?: boolean }) {
  const tone = complete
    ? "border-emerald-400/30 bg-emerald-400/10 text-emerald-200"
    : active
      ? "border-cyan-400/30 bg-cyan-400/10 text-cyan-200"
      : "border-white/10 bg-white/5 text-slate-300";

  return <div className={`rounded-full border px-3 py-2 text-xs font-medium ${tone}`}>{label}</div>;
}

function artifactGroup(label: string) {
  const value = label.toLowerCase();
  if (value.includes("spec")) return "Specification";
  if (value.includes("issue") || value.includes("human") || value.includes("clean") || value.includes("duplicate")) return "QC and review";
  if (value.includes("sdtm") || value.includes("define") || value.includes("xpt")) return "SDTM outputs";
  return "Other files";
}

function levelClass(level: TimelineEvent["level"]) {
  if (level === "success") return "text-emerald-300";
  if (level === "error") return "text-rose-300";
  if (level === "warning") return "text-amber-300";
  return "text-cyan-300";
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
  const groupedArtifacts = job?.artifacts
    ? Object.entries(job.artifacts).reduce<Record<string, Array<[string, string]>>>((acc, entry) => {
        const group = artifactGroup(entry[0]);
        acc[group] = [...(acc[group] ?? []), entry];
        return acc;
      }, {})
    : {};

  const banner = job?.status === "failed"
    ? { tone: "border-rose-400/20 bg-rose-400/10 text-rose-100", icon: CircleAlert, title: "Run failed", body: job.error ?? "Check the run console for the exact error and retry once the file or inputs are corrected." }
    : job?.status === "completed"
      ? { tone: "border-emerald-400/20 bg-emerald-400/10 text-emerald-100", icon: CheckCircle2, title: "Run completed", body: phase === "sdtm" ? "Your SDTM step finished successfully. Review the grouped artifacts below." : "Phase 1 completed. Review the outputs, then upload the reviewed human issue log to continue." }
      : busy
        ? { tone: "border-cyan-400/20 bg-cyan-400/10 text-cyan-100", icon: Sparkles, title: "Run in progress", body: phase === "sdtm" ? "Generating SDTM outputs from the current job workspace." : "Processing Layer 1 QC and spec generation for the uploaded file." }
        : null;

  return (
    <section id="demo-workspace" className={compact ? "" : "mx-auto max-w-7xl px-6 py-10 lg:px-10"}>
      {!compact ? (
        <div className="mb-8 flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div className="max-w-3xl">
            <StatusPill>
              <Sparkles className="h-4 w-4 text-emerald-300" /> Live product demo
            </StatusPill>
            <h2 className="mt-4 text-3xl font-semibold tracking-tight text-white md:text-4xl">
              Try the workflow directly from the site.
            </h2>
            <p className="mt-3 text-base leading-8 text-slate-300">
              Use a sample file or upload your own structured test dataset. Designed for pilot evaluation with biometrics and data management teams.
            </p>
            <div className="mt-5 flex flex-wrap gap-3">
              <StepPill label="Upload" active={!existingJobId && !busy} complete={!!file} />
              <StepPill label="QC + Spec" active={phase === "layer1_spec" && busy} complete={!!existingJobId} />
              <StepPill label="Review" active={!!existingJobId && !reviewedHumanFile && !busy} complete={!!reviewedHumanFile} />
              <StepPill label="SDTM" active={phase === "sdtm" && busy} complete={job?.status === "completed" && phase === "sdtm"} />
            </div>
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
          <div className="rounded-[2rem] border border-white/10 bg-white/[0.04] p-7 backdrop-blur-xl">
            <div className="flex items-center gap-3">
              <FileUp className="h-5 w-5 text-emerald-300" />
              <h3 className="text-2xl font-semibold text-white">Upload files</h3>
            </div>
            <p className="mt-3 text-sm leading-7 text-slate-300">
              Start with the raw source file. After phase 1 completes, upload the reviewed human issue log and continue into SDTM.
            </p>

            <div className="mt-6 rounded-[1.75rem] border border-dashed border-white/15 bg-slate-950/40 p-6">
              <label className="mb-3 block text-sm font-medium text-slate-200">Raw data file</label>
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
                className="block w-full text-sm text-slate-300 file:mr-4 file:rounded-xl file:border-0 file:bg-emerald-400 file:px-4 file:py-2 file:font-medium file:text-slate-950"
              />
              <div className="mt-4 text-sm text-slate-400">Accepted: CSV, XLSX, XLS</div>
              {file ? <div className="mt-2 text-sm font-medium text-white">Selected: {file.name}</div> : null}
            </div>

            <div className="mt-5 rounded-[1.75rem] border border-dashed border-white/15 bg-slate-950/40 p-6">
              <label className="mb-3 block text-sm font-medium text-slate-200">Reviewed human issue log file</label>
              <input
                type="file"
                accept=".csv"
                onChange={(e) => setReviewedHumanFile(e.target.files?.[0] ?? null)}
                className="block w-full text-sm text-slate-300 file:mr-4 file:rounded-xl file:border-0 file:bg-white file:px-4 file:py-2 file:font-medium file:text-slate-950"
              />
              <div className="mt-4 text-sm text-slate-400">Upload this after phase 1 completes.</div>
              {reviewedHumanFile ? <div className="mt-2 text-sm font-medium text-white">Selected: {reviewedHumanFile.name}</div> : null}
            </div>
          </div>

          <div className="rounded-[2rem] border border-white/10 bg-white/[0.04] p-7 backdrop-blur-xl">
            <div className="flex items-center gap-3">
              <ScanSearch className="h-5 w-5 text-cyan-300" />
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
              <div className="space-y-3">
              <button
                disabled={!file || busy || (selectedDomain === "AUTO" && !detection) || detecting}
                onClick={() => {
                  setPhase("layer1_spec");
                  void runPipeline("layer1_spec");
                }}
                className="w-full rounded-2xl bg-gradient-to-r from-emerald-400 via-cyan-400 to-sky-500 px-5 py-4 text-sm font-semibold text-slate-950 shadow-lg disabled:cursor-not-allowed disabled:opacity-50"
              >
                {busy && phase === "layer1_spec" ? "Processing…" : detecting ? "Detecting domain…" : "Run Layer 1 + Spec"}
              </button>
              {!file ? <p className="text-xs text-slate-400">Upload a raw dataset first.</p> : null}
              </div>
              <div className="space-y-3">
              <button
                disabled={!existingJobId || !reviewedHumanFile || busy}
                onClick={() => {
                  setPhase("sdtm");
                  void runPipeline("sdtm");
                }}
                className="w-full rounded-2xl border border-white/10 bg-white/5 px-5 py-4 text-sm font-semibold text-white disabled:cursor-not-allowed disabled:opacity-50"
              >
                {busy && phase === "sdtm" ? "Processing…" : "Run SDTM"}
              </button>
              {!existingJobId ? <p className="text-xs text-slate-400">Complete Layer 1 + Spec first to create the job workspace.</p> : !reviewedHumanFile ? <p className="text-xs text-slate-400">Upload the reviewed human issue log to enable SDTM.</p> : null}
              </div>
            </div>

            <div className="mt-6 grid gap-4 md:grid-cols-2">
              <div className="rounded-[1.5rem] bg-slate-950/50 p-5">
                <div className="text-sm text-slate-400">Detected domain</div>
                <div className="mt-2 text-2xl font-semibold text-white">{detectedLabel}</div>
              </div>
              <div className="rounded-[1.5rem] bg-slate-950/50 p-5">
                <div className="text-sm text-slate-400">Active job id</div>
                <div className="mt-2 break-all text-sm leading-7 text-slate-300">
                  {existingJobId ?? "Run Layer 1 + Spec to create a reusable job workspace."}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="rounded-[2rem] border border-white/10 bg-white/[0.04] p-7 backdrop-blur-xl">
            <div className="flex flex-col gap-4 md:flex-row md:items-start md:justify-between">
              <div>
                <h3 className="text-2xl font-semibold text-white">Run console</h3>
                <p className="mt-3 text-sm leading-7 text-slate-300">
                  One place for detection, Layer 1 QC, spec generation, and SDTM progression in the same job.
                </p>
              </div>
              <button
                onClick={() => void copyLogs()}
                className="inline-flex items-center gap-2 rounded-2xl border border-white/10 bg-white/5 px-4 py-3 text-sm font-medium text-white"
              >
                <Copy className="h-4 w-4" /> {copied ? "Copied" : "Copy logs"}
              </button>
            </div>

            {banner ? (
              <div className={`mt-6 flex items-start gap-3 rounded-[1.5rem] border p-4 ${banner.tone}`}>
                <banner.icon className="mt-0.5 h-5 w-5 shrink-0" />
                <div>
                  <div className="font-semibold">{banner.title}</div>
                  <div className="mt-1 text-sm leading-6 opacity-90">{banner.body}</div>
                </div>
              </div>
            ) : null}

            <div className="mt-6 grid gap-3 sm:grid-cols-3">
              <div className="rounded-[1.5rem] bg-slate-950/50 p-5">
                <div className="text-sm text-slate-400">Status</div>
                <div className="mt-2 text-xl font-semibold text-white">{job?.status ?? (busy ? "running" : "idle")}</div>
              </div>
              <div className="rounded-[1.5rem] bg-slate-950/50 p-5">
                <div className="text-sm text-slate-400">Current step</div>
                <div className="mt-2 text-xl font-semibold text-white">{job?.current_step ?? "Waiting"}</div>
              </div>
              <div className="rounded-[1.5rem] bg-slate-950/50 p-5">
                <div className="text-sm text-slate-400">Flagged log lines</div>
                <div className="mt-2 text-xl font-semibold text-white">{issueCount}</div>
              </div>
            </div>

            <div className="mt-6 overflow-hidden rounded-[1.5rem] border border-white/10 bg-[#020617] shadow-inner">
              <div className="flex items-center justify-between border-b border-white/10 px-5 py-3 text-xs uppercase tracking-[0.18em] text-slate-400">
                <span>Live process output</span>
                <span>{timeline.length} lines</span>
              </div>
              <div className={`${compact ? "max-h-[22rem]" : "max-h-[28rem]"} overflow-auto px-5 py-4 font-mono text-sm leading-7 text-slate-100`}>
                {timeline.map((event, index) => (
                  <div key={`${event.time}-${index}`} className="grid grid-cols-[72px_52px_1fr] gap-3 border-b border-white/5 py-2 last:border-b-0">
                    <span className="text-slate-500">{new Date(event.time).toLocaleTimeString()}</span>
                    <span className={levelClass(event.level)}>{levelLabel(event.level)}</span>
                    <span className="whitespace-pre-wrap break-words">{event.message}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          <div className="rounded-[2rem] border border-white/10 bg-white/[0.04] p-7 backdrop-blur-xl">
            <div className="flex items-center gap-3">
              <ShieldCheck className="h-5 w-5 text-emerald-300" />
              <h3 className="text-2xl font-semibold text-white">Outputs</h3>
            </div>
            <p className="mt-3 text-sm leading-7 text-slate-300">
              Download the human review issue log, spec package, and final SDTM artifacts from the same workspace.
            </p>

            <div className="mt-6 grid gap-4">
              {job && Object.keys(job.artifacts).length ? (
                Object.entries(groupedArtifacts).map(([group, items]) => (
                  <div key={group} className="rounded-[1.5rem] bg-slate-950/50 p-4">
                    <div className="mb-3 text-sm font-semibold text-white">{group}</div>
                    <div className="grid gap-3">
                      {items.map(([label, url]) => (
                        <a
                          key={label}
                          href={`${API_BASE_URL}${url}`}
                          target="_blank"
                          rel="noreferrer"
                          className="flex items-center justify-between rounded-2xl border border-white/8 bg-slate-900/70 px-5 py-4 text-sm text-slate-100 transition hover:bg-slate-900"
                        >
                          <span>{label}</span>
                          <Download className="h-4 w-4" />
                        </a>
                      ))}
                    </div>
                  </div>
                ))
              ) : (
                <div className="rounded-[1.5rem] bg-slate-950/50 p-5 text-sm leading-7 text-slate-300">
                  Run a job to see downloadable artifacts here.
                </div>
              )}
            </div>

            {compact ? (
              <div className="mt-6 rounded-[1.5rem] border border-cyan-400/20 bg-cyan-400/5 p-5 text-sm leading-7 text-slate-200">
                Need more room for the workflow? Use the dedicated platform page for the same demo with a larger workspace.
                <div>
                  <Link href="/platform" className="mt-4 inline-flex items-center gap-2 font-semibold text-cyan-200">
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
