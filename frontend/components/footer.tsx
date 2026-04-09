export function Footer() {
  return (
    <footer className="mx-auto mt-16 max-w-7xl px-6 pb-10 pt-6 text-sm text-slate-600">
      <div className="rounded-3xl border border-slate-900/10 bg-white/70 px-6 py-5 backdrop-blur-xl">
        <div className="flex flex-col gap-4 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="text-base font-semibold text-slate-900">KlinAI</div>
            <p className="mt-1 max-w-2xl">Clinical data automation for transparent QC, mapping specs, and SDTM generation.</p>
          </div>
          <div className="flex flex-wrap items-center gap-4">
            <a href="/platform" className="hover:text-slate-900">Platform</a>
            <a href="/privacy" className="hover:text-slate-900">Privacy</a>
            <a href="/terms" className="hover:text-slate-900">Terms</a>
            <a href="mailto:bhuwan@klinai.tech" className="hover:text-slate-900">Contact</a>
          </div>
        </div>
      </div>
    </footer>
  );
}
