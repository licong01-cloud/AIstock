"use client";

import React, { createContext, useContext, useState } from "react";

interface DialogContextValue {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const DialogContext = createContext<DialogContextValue | null>(null);

function useDialog() {
  const context = useContext(DialogContext);
  if (!context) throw new Error("Dialog components must be used within <Dialog>");
  return context;
}

interface DialogProps {
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
  children: React.ReactNode;
}

export function Dialog({ open, onOpenChange, children }: DialogProps) {
  const [internalOpen, setInternalOpen] = useState(false);
  const actualOpen = open !== undefined ? open : internalOpen;
  
  return (
    <DialogContext.Provider value={{ open: actualOpen, onOpenChange: onOpenChange || setInternalOpen }}>
      {children}
    </DialogContext.Provider>
  );
}

export function DialogContent({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  const { open } = useDialog();
  if (!open) return null;
  
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      <div className="fixed inset-0 bg-black/50" onClick={() => useDialog().onOpenChange(false)} />
      <div className={`relative z-50 w-full max-w-lg rounded-lg border border-gray-200 bg-white p-6 shadow-lg ${className}`}>
        {children}
      </div>
    </div>
  );
}

export function DialogHeader({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <div className={`flex flex-col space-y-1.5 text-center sm:text-left ${className}`}>{children}</div>;
}

export function DialogTitle({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <h2 className={`text-lg font-semibold leading-none tracking-tight ${className}`}>{children}</h2>;
}

export function DialogDescription({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <p className={`text-sm text-gray-500 ${className}`}>{children}</p>;
}

export function DialogFooter({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  return <div className={`flex flex-col-reverse sm:flex-row sm:justify-end sm:space-x-2 ${className}`}>{children}</div>;
}
