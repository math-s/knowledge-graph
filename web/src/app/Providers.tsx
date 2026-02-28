"use client";

import { LangProvider } from "@/lib/LangContext";
import type { ReactNode } from "react";

export default function Providers({ children }: { children: ReactNode }) {
  return <LangProvider>{children}</LangProvider>;
}
