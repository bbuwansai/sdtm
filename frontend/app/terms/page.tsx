import { SiteHeader } from "@/components/site-shell";

export default function TermsPage() {
  return (
    <main className="min-h-screen">
      <SiteHeader />
      <section className="mx-auto max-w-4xl px-6 py-16">
        <div className="card-glass p-8">
          <h1 className="text-3xl font-semibold">Terms</h1>
          <p className="mt-4 text-slate-700">This demo is intended for product evaluation only. Before production use, add final legal terms, data handling language, security commitments, and service-level expectations.</p>
        </div>
      </section>
    </main>
  );
}
