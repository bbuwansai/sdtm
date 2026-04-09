import { SiteHeader } from "@/components/site-shell";

export default function PrivacyPage() {
  return (
    <main className="min-h-screen">
      <SiteHeader />
      <section className="mx-auto max-w-4xl px-6 py-16">
        <div className="card-glass p-8">
          <h1 className="text-3xl font-semibold">Privacy</h1>
          <p className="mt-4 text-slate-700">This demo stores uploaded files only for processing and review. For production use, replace local job storage with secure object storage, retention controls, and your final privacy policy language.</p>
        </div>
      </section>
    </main>
  );
}
