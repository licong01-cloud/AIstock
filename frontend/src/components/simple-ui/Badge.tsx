"use client";

import React from "react";

interface BadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  children: React.ReactNode;
  variant?: "default" | "secondary" | "outline" | "destructive";
}

export function Badge({ children, variant = "default", className = "", ...props }: BadgeProps) {
  const baseStyles = "inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-medium";
  const variantStyles = {
    default: "bg-blue-100 text-blue-800",
    secondary: "bg-gray-100 text-gray-800",
    outline: "border border-gray-200 text-gray-800",
    destructive: "bg-red-100 text-red-800",
  };

  return (
    <span className={`${baseStyles} ${variantStyles[variant]} ${className}`} {...props}>
      {children}
    </span>
  );
}
