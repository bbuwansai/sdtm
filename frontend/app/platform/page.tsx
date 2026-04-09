"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { CheckCircle2, ChevronRight, CircleAlert, Download, Eye, FileUp, LoaderCircle, ScanSearch, Sparkles } from "lucide-react";
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
    if (!detection) return "No file analysed yet";
    return `${detection.domain} (${Math.round(detection.confidence * 100)}% confidence)`;
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

  return (
    <main className="min-h-screen bg-background text-slate-900">
      <SiteHeader />
      <section className="relative mx-auto max-w-7xl overflow-hidden rounded-3xl px-6 pb-16 pt-12" style={{ background: "linear-gradient(180deg, rgba(139,92,246,0.06), rgba(236,72,153,0.06) 35%, rgba(255,255,255,0.6))" }}>
        <BackgroundGlow />
        <div className="mx-auto max-w-4xl text-center">
          <span className="inline-flex items-center gap-2 rounded-full border border-slate-900/10 bg-white/70 px-3 py-1 text-xs text-slate-900/80 backdrop-blur">
            <Sparkles className="h-4 w-4 text-purple-600" /> Live processing timeline for customer demos
          </span>
          <h1 className="mt-5 text-4xl font-semibold md:text-5xl">Raw data in. <span className="gradient-text">Layer 1, Spec, and SDTM</span> out.</h1>
          <p className="mx-auto mt-4 max-w-3xl text-lg text-slate-700">The demo is designed to feel real: it detects the domain, shows the issue-finding steps clearly, surfaces what would normally go to human review, and then continues automatically so the customer sees the full outcome.</p>
        </div>
      </section>

      <section className="mx-auto mt-10 grid max-w-7xl gap-6 px-6 lg:grid-cols-[1.1fr_0.9fr]">
        <div className="space-y-6">
          <div className="card-glass p-6">
            <div className="flex items-center gap-3">
              <FileUp className="h-5 w-5 text-purple-600" />
              <h2 className="text-xl font-semibold">Upload raw data</h2>
            </div>
            <div className="mt-4 rounded-3xl border border-dashed border-slate-300 bg-white/50 p-6">
              <input
                type="file"
                accept=".csv,.xlsx,.xls"
                onChange={(e) => setFile(e.target.files?.[0] ?? null)}
                className="block w-full text-sm text-slate-700"
              />
              <p className="mt-3 text-sm text-slate-600">Accepted formats: CSV, XLSX, XLS</p>
              {file ? <p className="mt-2 text-sm font-medium text-slate-900">Selected: {file.name}</p> : null}
            </div>
          </div>

          <div className="card-glass p-6">
            <div className="flex items-center gap-3">
              <ScanSearch className="h-5 w-5 text-purple-600" />
              <h2 className="text-xl font-semibold">Domain detection</h2>
            </div>
            <div className="mt-4 flex flex-col gap-4 sm:flex-row sm:items-center">
              <select value={selectedDomain} onChange={(e) => setSelectedDomain(e.target.value)} className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm outline-none">
                {domainOptions.map((option) => <option key={option} value={option}>{option === "AUTO" ? "Auto detect" : option}</option>)}
              </select>
              <button disabled={!file} onClick={() => void detectDomain()} className="rounded-2xl border border-slate-900/10 bg-white/80 px-5 py-3 text-sm font-medium disabled:opacity-50">
                Analyse file
              </button>
              <button disabled={!file || busy} onClick={() => void runPipeline()} className="rounded-2xl bg-gradient-to-r from-purple-500 via-pink-500 to-indigo-500 px-5 py-3 text-sm font-medium text-white shadow-lg disabled:opacity-50">
                {busy ? "Processing…" : "Run transformation"}
              </button>
            </div>
            <div className="mt-5 rounded-2xl bg-slate-50 p-4 text-sm">
              <div className="font-medium text-slate-900">Detected domain</div>
              <div className="mt-1 text-slate-700">{detectedLabel}</div>
              {detection?.matched_columns?.length ? (
                <div className="mt-3 text-slate-600">Matched columns: {detection.matched_columns.join(", ")}</div>
              ) : null}
              {detection?.runner_up ? <div className="mt-2 text-slate-500">Runner-up match: {detection.runner_up}</div> : null}
            </div>
          </div>

          <div className="card-glass p-6">
            <div className="flex items-center gap-3">
              <Eye className="h-5 w-5 text-purple-600" />
              <h2 className="text-xl font-semibold">Recommended demo flow</h2>
            </div>
            <div className="mt-4 space-y-3 text-sm text-slate-700">
              <div className="rounded-2xl bg-slate-50 p-4">For the demo, do <span className="font-semibold">not stop for manual editing after Layer 1</span>. That slows the story and makes the product look unfinished.</div>
              <div className="rounded-2xl bg-slate-50 p-4">Instead, show that the platform found the real issues, split them into standardizable vs human-review items, and then continued automatically with the clean path.</div>
              <div className="rounded-2xl bg-slate-50 p-4">This mirrors the real-world workflow, but compresses it into a clean automation narrative that customers can understand in one sitting.</div>
            </div>
          </div>

          <div className="card-glass p-6">
            <h2 className="text-xl font-semibold">What customers will see</h2>
            <div className="mt-4 grid gap-3 text-sm text-slate-700 md:grid-cols-2">
              {[
                "Classifying the uploaded file into DM, VS, LB, or AE",
                "Processing QC for the raw data",
                "Finding and categorizing issue logs",
                "Showing what would go to human review",
                "Generating the spec package from raw data",
                "Preparing SDTM inputs from raw + Layer 1 + spec",
                "Building final SDTM output files",
                "Packaging downloadable results",
              ].map((item) => (
                <div key={item} className="flex items-start gap-2 rounded-2xl bg-slate-50 p-3"><ChevronRight className="mt-0.5 h-4 w-4 text-purple-600" />{item}</div>
              ))}
            </div>
          </div>
        </div>

        <div className="space-y-6">
          <div className="card-glass p-6">
            <h2 className="text-xl font-semibold">Live process timeline</h2>
            <div className="mt-5 space-y-3">
              {(job?.timeline?.length ? job.timeline : [{ time: new Date().toISOString(), level: "info", message: "No job started yet." }]).map((event, index) => (
                <div key={`${event.time}-${index}`} className="flex gap-3 rounded-2xl bg-slate-50 p-4">
                  <div className="mt-0.5">
                    {event.level === "success" ? <CheckCircle2 className="h-5 w-5 text-emerald-600" /> : event.level === "error" ? <CircleAlert className="h-5 w-5 text-red-600" /> : <LoaderCircle className="h-5 w-5 text-purple-600" />}
                  </div>
                  <div>
                    <div className="text-xs uppercase tracking-wide text-slate-500">{new Date(event.time).toLocaleTimeString()}</div>
                    <div className="mt-1 text-sm text-slate-800">{event.message}</div>
                  </div>
                </div>
              ))}
            </div>
          </div>

          <div className="card-glass p-6">
            <h2 className="text-xl font-semibold">Outputs</h2>
            <div className="mt-4 grid gap-3">
              {job && Object.keys(job.artifacts).length ? Object.entries(job.artifacts).map(([label, url]) => (
                <a key={label} href={`${API_BASE_URL}${url}`} target="_blank" rel="noreferrer" className="flex items-center justify-between rounded-2xl bg-slate-50 px-4 py-3 text-sm text-slate-800 hover:bg-slate-100">
                  <span>{label}</span>
                  <Download className="h-4 w-4" />
                </a>
              )) : <div className="rounded-2xl bg-slate-50 p-4 text-sm text-slate-600">Layer 1 output, spec package, SDTM output package, and issue files will appear here.</div>}
            </div>
            {job?.metrics && Object.keys(job.metrics).length ? (
              <div className="mt-5 grid grid-cols-2 gap-3 text-sm">
                {Object.entries(job.metrics).map(([key, value]) => (
                  <div key={key} className="rounded-2xl bg-slate-50 p-4">
                    <div className="text-slate-500">{key}</div>
                    <div className="mt-1 font-semibold text-slate-900">{String(value)}</div>
                  </div>
                ))}
              </div>
            ) : null}
            {job?.error ? <div className="mt-4 rounded-2xl border border-red-200 bg-red-50 p-4 text-sm text-red-700">{job.error}</div> : null}
          </div>
        </div>
      </section>
    </main>
  );
}
