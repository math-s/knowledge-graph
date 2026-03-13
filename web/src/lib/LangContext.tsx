"use client";

import {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  type ReactNode,
} from "react";
import type { Lang } from "./types";

const STORAGE_KEY = "knowledge-graph-lang";
const STORAGE_KEY_COMPARE = "knowledge-graph-compare-lang";
const STORAGE_KEY_SIDE_BY_SIDE = "knowledge-graph-side-by-side";
const DEFAULT_LANG: Lang = "en";
const DEFAULT_COMPARE_LANG: Lang = "la";

interface LangContextValue {
  lang: Lang;
  setLang: (lang: Lang) => void;
  /** Side-by-side mode: show preferred + comparison language in parallel */
  sideBySide: boolean;
  setSideBySide: (enabled: boolean) => void;
  /** The comparison language shown in side-by-side mode */
  compareLang: Lang;
  setCompareLang: (lang: Lang) => void;
}

const LangContext = createContext<LangContextValue | null>(null);

const VALID_LANGS: Lang[] = ["la", "en", "pt", "el"];

function getInitialLang(): Lang {
  if (typeof window === "undefined") return DEFAULT_LANG;
  const stored = localStorage.getItem(STORAGE_KEY);
  if (stored && VALID_LANGS.includes(stored as Lang)) {
    return stored as Lang;
  }
  return DEFAULT_LANG;
}

function getInitialCompareLang(): Lang {
  if (typeof window === "undefined") return DEFAULT_COMPARE_LANG;
  const stored = localStorage.getItem(STORAGE_KEY_COMPARE);
  if (stored && VALID_LANGS.includes(stored as Lang)) {
    return stored as Lang;
  }
  return DEFAULT_COMPARE_LANG;
}

function getInitialSideBySide(): boolean {
  if (typeof window === "undefined") return false;
  return localStorage.getItem(STORAGE_KEY_SIDE_BY_SIDE) === "true";
}

export function LangProvider({ children }: { children: ReactNode }) {
  const [lang, setLangState] = useState<Lang>(DEFAULT_LANG);
  const [compareLang, setCompareLangState] = useState<Lang>(DEFAULT_COMPARE_LANG);
  const [sideBySide, setSideBySideState] = useState(false);

  // Hydrate from localStorage on mount
  useEffect(() => {
    setLangState(getInitialLang());
    setCompareLangState(getInitialCompareLang());
    setSideBySideState(getInitialSideBySide());
  }, []);

  const setLang = useCallback((newLang: Lang) => {
    setLangState(newLang);
    if (typeof window !== "undefined") {
      localStorage.setItem(STORAGE_KEY, newLang);
    }
  }, []);

  const setCompareLang = useCallback((newLang: Lang) => {
    setCompareLangState(newLang);
    if (typeof window !== "undefined") {
      localStorage.setItem(STORAGE_KEY_COMPARE, newLang);
    }
  }, []);

  const setSideBySide = useCallback((enabled: boolean) => {
    setSideBySideState(enabled);
    if (typeof window !== "undefined") {
      localStorage.setItem(STORAGE_KEY_SIDE_BY_SIDE, String(enabled));
    }
  }, []);

  return (
    <LangContext.Provider
      value={{ lang, setLang, sideBySide, setSideBySide, compareLang, setCompareLang }}
    >
      {children}
    </LangContext.Provider>
  );
}

export function useLang(): LangContextValue {
  const ctx = useContext(LangContext);
  if (!ctx) throw new Error("useLang must be used within a LangProvider");
  return ctx;
}
