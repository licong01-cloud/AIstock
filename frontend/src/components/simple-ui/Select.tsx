"use client";

import React, { createContext, useContext, useState } from "react";

interface SelectContextValue {
  value: string;
  onValueChange: (value: string) => void;
  open: boolean;
  setOpen: (open: boolean) => void;
}

const SelectContext = createContext<SelectContextValue | null>(null);

function useSelect() {
  const context = useContext(SelectContext);
  if (!context) throw new Error("Select components must be used within <Select>");
  return context;
}

interface SelectProps {
  value?: string;
  onValueChange?: (value: string) => void;
  children: React.ReactNode;
}

export function Select({ value, onValueChange, children }: SelectProps) {
  const [internalValue, setInternalValue] = useState("");
  const [open, setOpen] = useState(false);
  const actualValue = value !== undefined ? value : internalValue;
  
  return (
    <SelectContext.Provider value={{ value: actualValue, onValueChange: onValueChange || setInternalValue, open, setOpen }}>
      <div className="relative">{children}</div>
    </SelectContext.Provider>
  );
}

export function SelectTrigger({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  const { open, setOpen } = useSelect();
  return (
    <button
      onClick={() => setOpen(!open)}
      className={`flex h-10 w-full items-center justify-between rounded-md border border-gray-300 bg-white px-3 py-2 text-sm placeholder:text-gray-500 focus:outline-none focus:ring-2 focus:ring-gray-400 disabled:cursor-not-allowed disabled:opacity-50 ${className}`}
    >
      {children}
    </button>
  );
}

export function SelectValue({ placeholder = "Select..." }: { placeholder?: string }) {
  const { value } = useSelect();
  return <span className={value ? "" : "text-gray-500"}>{value || placeholder}</span>;
}

export function SelectContent({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  const { open, setOpen } = useSelect();
  if (!open) return null;
  
  return (
    <>
      <div className="fixed inset-0 z-50" onClick={() => setOpen(false)} />
      <div className={`absolute z-50 mt-1 max-h-60 w-full overflow-auto rounded-md border border-gray-200 bg-white py-1 text-sm shadow-lg ${className}`}>
        {children}
      </div>
    </>
  );
}

export function SelectItem({ value, children, className = "" }: { value: string; children: React.ReactNode; className?: string }) {
  const { value: selected, onValueChange, setOpen } = useSelect();
  const isSelected = selected === value;
  
  return (
    <button
      onClick={() => {
        onValueChange(value);
        setOpen(false);
      }}
      className={`relative flex w-full cursor-pointer select-none items-center rounded-sm py-1.5 pl-8 pr-2 text-sm outline-none hover:bg-gray-100 focus:bg-gray-100 ${isSelected ? "bg-gray-100" : ""} ${className}`}
    >
      {isSelected && <span className="absolute left-2 flex h-3.5 w-3.5 items-center justify-center">✓</span>}
      {children}
    </button>
  );
}
