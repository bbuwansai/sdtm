import { PlatformDemo } from "@/components/platform-demo";
import { SiteHeader } from "@/components/site-shell";

export default function PlatformPage() {
  return (
    <main className="min-h-screen bg-background text-foreground">
      <SiteHeader />
      <section className="mx-auto max-w-7xl px-6 pb-6 pt-10 lg:px-10">
        <div className="rounded-[2rem] border border-white/10 bg-gradient-to-b from-[#06111d] via-[#071827] to-[#020817] p-2 shadow-[0_30px_120px_rgba(2,12,21,0.45)]">
          <PlatformDemo />
        </div>
      </section>
    </main>
  );
}
