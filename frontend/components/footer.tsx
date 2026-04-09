export function Footer() {
  return (
    <footer className="mx-auto mt-24 max-w-7xl px-6 pb-10 pt-6 text-sm text-slate-600">
      <div className="rounded-3xl border border-slate-900/10 bg-white/70 px-6 py-6 shadow-xl shadow-slate-200/60 backdrop-blur-xl">
        <div className="flex flex-col gap-6 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="text-base font-semibold text-slate-900">KlinAI</div>
            <p className="mt-2 max-w-2xl text-slate-600">
              Clinical data automation for transparent QC, spec generation, and SDTM workflows.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-5">
            <a href="/platform" className="hover:text-slate-900">
              Platform
            </a>
            <a href="/privacy" className="hover:text-slate-900">
              Privacy
            </a>
            <a href="/terms" className="hover:text-slate-900">
              Terms
            </a>
            <a href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Demo%20Request" className="font-medium text-slate-900">
              Contact sales
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}
