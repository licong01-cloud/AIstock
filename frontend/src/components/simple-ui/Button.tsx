"use client";

import React from "react";
import styles from "./button.module.css";

type ButtonVariant = "primary" | "secondary" | "outline" | "ghost" | "destructive";
type ButtonSize = "small" | "default" | "large";

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant;
  size?: ButtonSize;
  children: React.ReactNode;
}

export function Button({
  variant = "primary",
  size = "default",
  children,
  className = "",
  disabled,
  ...props
}: ButtonProps) {
  const variantClass = styles[variant] || styles.primary;
  const sizeClass = size === "default" ? "" : styles[size];
  
  return (
    <button
      className={`${styles.button} ${variantClass} ${sizeClass} ${className}`}
      disabled={disabled}
      {...props}
    >
      {children}
    </button>
  );
}
