"use client";

import React, { createContext, useContext, useState } from "react";

interface TabsContextValue {
  value: string;
  onValueChange: (value: string) => void;
}

const TabsContext = createContext<TabsContextValue | null>(null);

function useTabs() {
  const context = useContext(TabsContext);
  if (!context) throw new Error("Tabs components must be used within <Tabs>");
  return context;
}

interface TabsProps {
  defaultValue?: string;
  value?: string;
  onValueChange?: (value: string) => void;
  children: React.ReactNode;
  className?: string;
}

export function Tabs({ defaultValue, value, onValueChange, children, className = "" }: TabsProps) {
  const [internalValue, setInternalValue] = useState(defaultValue || "");
  const actualValue = value !== undefined ? value : internalValue;
  
  return (
    <TabsContext.Provider value={{ value: actualValue, onValueChange: onValueChange || setInternalValue }}>
      <div className={className}>{children}</div>
    </TabsContext.Provider>
  );
}

export function TabsList({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <div className={`inline-flex h-10 items-center justify-center rounded-md bg-gray-100 p-1 ${className}`}>{children}</div>;
}

export function TabsTrigger({ value, children, className = "" }: { value: string; children: React.ReactNode; className?: string }) {
  const { value: selected, onValueChange } = useTabs();
  const isActive = selected === value;
  
  return (
    <button
      onClick={() => onValueChange(value)}
      className={`inline-flex items-center justify-center whitespace-nowrap rounded-sm px-3 py-1.5 text-sm font-medium ring-offset-white transition-all focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gray-400 focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50 ${isActive ? "bg-white text-gray-900 shadow-sm" : "text-gray-500 hover:text-gray-900"} ${className}`}
    >
      {children}
    </button>
  );
}

export function TabsContent({ value, children, className = "" }: { value: string; children: React.ReactNode; className?: string }) {
  const { value: selected } = useTabs();
  if (selected !== value) return null;
  
  return <div className={`mt-2 ring-offset-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gray-400 focus-visible:ring-offset-2 ${className}`}>{children}</div>;
}
