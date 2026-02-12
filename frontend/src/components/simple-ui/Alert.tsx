"use client";

import React from "react";

interface AlertProps extends React.HTMLAttributes<HTMLDivElement> {
  children: React.ReactNode;
  className?: string;
}

export function Alert({ children, className = "", ...props }: AlertProps) {
  return (
    <div
      role="alert"
      className={`relative w-full rounded-lg border border-gray-200 p-4 [&>svg~*]:pl-7 [&>svg+div]:translate-y-[-3px] [&>svg]:absolute [&>svg]:left-4 [&>svg]:top-4 [&>svg]:text-gray-950 ${className}`}
      {...props}
    >
      {children}
    </div>
  );
}

export function AlertDescription({ children, className = "", ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div className={`text-sm [&_p]:leading-relaxed ${className}`} {...props}>
      {children}
    </div>
  );
}
