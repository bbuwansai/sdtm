"use client";

import type { ReactNode } from "react";

interface HeroProps {
  trustBadge?: {
    text: string;
    icons?: string[];
  };
  headline: {
    line1: string;
    line2: string;
  };
  subtitle: string;
  className?: string;
  children?: ReactNode;
}

export default function AnimatedShaderHero({ trustBadge, headline, subtitle, className = "", children }: HeroProps) {
  return (
    <section className={`relative isolate overflow-hidden border-b border-white/6 bg-[#090705] ${className}`}>
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_18%_22%,rgba(188,120,55,0.34),transparent_26%),radial-gradient(circle_at_58%_16%,rgba(247,214,173,0.14),transparent_18%),radial-gradient(circle_at_86%_28%,rgba(94,54,24,0.28),transparent_22%),linear-gradient(140deg,#0b0705_5%,#1a1008_42%,#090705_100%)]" />
      <div className="absolute inset-0 opacity-70">
        <div className="absolute left-[-12%] top-[18%] h-56 w-[58%] rounded-full bg-[radial-gradient(circle,rgba(255,229,194,0.55)_0%,rgba(255,182,92,0.14)_36%,transparent_72%)] blur-3xl animate-pulse" />
        <div className="absolute left-[10%] top-[44%] h-px w-[52%] bg-gradient-to-r from-transparent via-[#ffd7a6] to-transparent opacity-95 shadow-[0_0_22px_rgba(255,215,166,0.9)]" />
        <div className="absolute left-[8%] top-[53%] h-px w-[44%] bg-gradient-to-r from-transparent via-[#fff1dc] to-transparent opacity-80 shadow-[0_0_18px_rgba(255,241,220,0.7)]" />
        <div className="absolute right-[-8%] top-[10%] h-[24rem] w-[30rem] rounded-full border border-[#e2b47d]/10 bg-[radial-gradient(circle,rgba(136,90,52,0.25)_0%,transparent_62%)] blur-2xl" />
      </div>

      <div className="relative mx-auto max-w-[1600px] px-6 pb-14 pt-16 sm:px-8 lg:px-14 lg:pb-18 lg:pt-24">
        <div className="soft-panel rounded-[2.75rem] px-6 py-8 sm:px-8 lg:px-12 lg:py-12">
          <div className="max-w-4xl">
            {trustBadge ? (
              <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/[0.045] px-5 py-2 text-sm text-[#f7ead8] shadow-[0_8px_40px_rgba(0,0,0,0.18)]">
                {trustBadge.icons?.length ? <span className="text-[#fde1b7]">{trustBadge.icons.join(" ")}</span> : null}
                <span>{trustBadge.text}</span>
              </div>
            ) : null}

            <h1 className="mt-8 max-w-5xl text-5xl font-semibold leading-[0.95] tracking-[-0.05em] sm:text-6xl lg:text-[6.2rem]">
              <span className="glass-text block">{headline.line1}</span>
              <span className="glass-text block">{headline.line2}</span>
            </h1>

            <p className="mt-7 max-w-3xl text-lg leading-8 text-[#f0e7db] sm:text-[1.35rem] sm:leading-9">
              {subtitle}
            </p>
          </div>

          {children ? <div className="mt-10">{children}</div> : null}
        </div>
      </div>
    </section>
  );
}
