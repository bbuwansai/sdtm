export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000";

export type JobSummary = {
  job_id: string;
  status: string;
  domain?: string | null;
  domain_confidence?: number | null;
  filename?: string | null;
  error?: string | null;
  current_step?: string | null;
  created_at?: string;
  updated_at?: string;
  timeline: TimelineEvent[];
  artifacts: Record<string, string>;
  metrics: Record<string, number | string>;
};

export type TimelineEvent = {
  time: string;
  level: "info" | "success" | "warning" | "error";
  message: string;
};
