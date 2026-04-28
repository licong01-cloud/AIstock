"use client";

import { useState } from "react";

export default function ConfirmAction({
  label,
  confirmText,
  onConfirm,
  danger = false,
  disabled = false,
  testId,
}: {
  label: string;
  confirmText: string;
  onConfirm: () => void | Promise<void>;
  danger?: boolean;
  disabled?: boolean;
  testId?: string;
}) {
  const [armed, setArmed] = useState(false);
  const [text, setText] = useState("");
  const [running, setRunning] = useState(false);
  const canConfirm = text === confirmText && !running && !disabled;

  if (!armed) {
    return (
      <button className={danger ? "pv2-button pv2-button-danger" : "pv2-button"} data-testid={testId} disabled={disabled} onClick={() => setArmed(true)} type="button">
        {label}
      </button>
    );
  }

  return (
    <div className="pv2-confirm-box">
      <div className="pv2-help">请输入 <code>{confirmText}</code> 确认执行。</div>
      <input className="pv2-input" data-testid={testId ? `${testId}-input` : undefined} value={text} onChange={(event) => setText(event.target.value)} placeholder={confirmText} />
      <div className="pv2-row-actions">
        <button
          className={danger ? "pv2-button pv2-button-danger" : "pv2-button"}
          data-testid={testId ? `${testId}-confirm` : undefined}
          disabled={!canConfirm}
          onClick={async () => {
            setRunning(true);
            try {
              await onConfirm();
              setArmed(false);
              setText("");
            } finally {
              setRunning(false);
            }
          }}
          type="button"
        >
          {running ? "执行中..." : "确认"}
        </button>
        <button className="pv2-button pv2-button-ghost" data-testid={testId ? `${testId}-cancel` : undefined} onClick={() => { setArmed(false); setText(""); }} type="button">取消</button>
      </div>
    </div>
  );
}
