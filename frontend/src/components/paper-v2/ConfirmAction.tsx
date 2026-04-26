"use client";

import { useState } from "react";

export default function ConfirmAction({
  label,
  confirmText,
  onConfirm,
  danger = false,
  disabled = false,
}: {
  label: string;
  confirmText: string;
  onConfirm: () => void | Promise<void>;
  danger?: boolean;
  disabled?: boolean;
}) {
  const [armed, setArmed] = useState(false);
  const [text, setText] = useState("");
  const [running, setRunning] = useState(false);
  const canConfirm = text === confirmText && !running && !disabled;

  if (!armed) {
    return (
      <button className={danger ? "pv2-button pv2-button-danger" : "pv2-button"} disabled={disabled} onClick={() => setArmed(true)} type="button">
        {label}
      </button>
    );
  }

  return (
    <div className="pv2-confirm-box">
      <div className="pv2-help">请输入 <code>{confirmText}</code> 确认执行。</div>
      <input className="pv2-input" value={text} onChange={(event) => setText(event.target.value)} placeholder={confirmText} />
      <div className="pv2-row-actions">
        <button
          className={danger ? "pv2-button pv2-button-danger" : "pv2-button"}
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
        <button className="pv2-button pv2-button-ghost" onClick={() => { setArmed(false); setText(""); }} type="button">取消</button>
      </div>
    </div>
  );
}
