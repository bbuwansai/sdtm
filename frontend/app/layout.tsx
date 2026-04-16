import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import { Footer } from "@/components/footer";
import { CustomCursor } from "@/components/custom-cursor";

const geistSans = Geist({ variable: "--font-geist-sans", subsets: ["latin"] });
const geistMono = Geist_Mono({ variable: "--font-geist-mono", subsets: ["latin"] });

export const metadata: Metadata = {
  title: "KlinAI | Raw Clinical Data to Traceable SDTM Workflows",
  description:
    "KlinAI helps biometrics and clinical data teams standardize raw source data, generate specs, and run traceable SDTM workflows.",
  metadataBase: new URL("https://klinai.tech"),
  openGraph: {
    title: "KlinAI",
    description:
      "Raw clinical data to traceable, CDISC-aligned SDTM workflows with QC, specification generation, and live demo access.",
    url: "https://klinai.tech",
    siteName: "KlinAI",
    type: "website",
  },
  robots: { index: true, follow: true },
};

export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en">
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased`}>
        <CustomCursor />
        {children}
        <Footer />
      </body>
    </html>
  );
}
