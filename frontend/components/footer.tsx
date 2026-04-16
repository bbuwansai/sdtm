import Link from "next/link";

export function Footer() {
  return (
    <footer className="mx-auto mt-24 max-w-7xl px-6 pb-10 pt-6 lg:px-10">
      <div className="rounded-[2rem] border border-white/10 bg-white/[0.04] px-6 py-8 shadow-[0_20px_80px_rgba(2,12,21,0.3)] backdrop-blur-xl">
        <div className="flex flex-col gap-8 md:flex-row md:items-center md:justify-between">
          <div>
            <div className="text-lg font-semibold text-white">KlinAI</div>
            <p className="mt-3 max-w-2xl text-sm leading-7 text-slate-300">
              AI-native clinical data automation for QC, specification generation, and traceable SDTM workflows.
            </p>
          </div>

          <div className="flex flex-wrap items-center gap-5 text-sm text-slate-300">
            <Link href="/">Home</Link>
            <Link href="/platform">Platform</Link>
            <Link href="/privacy">Privacy</Link>
            <Link href="/terms">Terms</Link>
            <a href="mailto:bhuwan@klinai.tech?subject=KlinAI%20Demo%20Request" className="font-medium text-white">
              Contact sales
            </a>
          </div>
        </div>
      </div>
    </footer>
  );
}
