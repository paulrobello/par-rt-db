import type { ReactNode } from "react";
import styles from "./ui.module.css";

/** Spec-sheet placard: a mono, uppercase, tracked micro-label captioning a section. */
export function Placard({ children }: { children: ReactNode }) {
  return <p className="placard">{children}</p>;
}

export type Status = "ok" | "warn" | "error" | "idle";

const LAMP: Record<Status, string> = {
  ok: styles.lamp_ok,
  warn: styles.lamp_warn,
  error: styles.lamp_error,
  idle: styles.lamp_idle,
};

export function StatusLamp({ status, label }: { status: Status; label?: string }) {
  return (
    <span className={styles.lamp}>
      <span className={`${styles.lampDot} ${LAMP[status]}`} />
      {label && <span className={styles.lampLabel}>{label}</span>}
    </span>
  );
}

export function Spinner({ label }: { label?: string }) {
  return (
    <span className={styles.spinner} role="status" aria-label={label ?? "loading"}>
      <span className={styles.spinnerRing} />
      {label && <span className={styles.spinnerLabel}>{label}</span>}
    </span>
  );
}

type ButtonVariant = "primary" | "secondary" | "danger";

type ButtonProps = {
  variant?: ButtonVariant;
  type?: "button" | "submit";
  onClick?: () => void;
  disabled?: boolean;
  children: ReactNode;
};

export function Button({
  variant = "secondary",
  type = "button",
  onClick,
  disabled,
  children,
}: ButtonProps) {
  const variantClass =
    variant === "primary" ? styles.btn_primary : variant === "danger" ? styles.btn_danger : "";
  return (
    <button
      type={type}
      onClick={onClick}
      disabled={disabled}
      className={`${styles.btn} ${variantClass}`}
    >
      {children}
    </button>
  );
}

export function Field({
  label,
  value,
  onChange,
  type = "text",
  placeholder,
  mono,
  secret,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  type?: string;
  placeholder?: string;
  mono?: boolean;
  secret?: boolean;
}) {
  return (
    <label className={styles.field}>
      <span className={styles.fieldLabel}>{label}</span>
      <input
        className={`${styles.input} ${mono ? styles.inputMono : ""}`}
        type={secret ? "password" : type}
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        autoComplete={secret ? "off" : undefined}
        spellCheck={false}
      />
    </label>
  );
}

export type ConnectionState = "idle" | "connecting" | "connected" | "reconnecting" | "closed";

const PULSE_TONE = {
  connected: styles.pulse_live,
  connecting: styles.pulse_transit,
  reconnecting: styles.pulse_transit,
  closed: styles.pulse_warn,
  idle: styles.pulse_idle,
} as const;

export function ConnectionPulse({ state }: { state: ConnectionState }) {
  return (
    <span className={styles.pulse} title={`connection: ${state}`}>
      <span
        className={`${styles.pulseDot} ${PULSE_TONE[state]} ${
          state === "connected" ? styles.pulseBreathes : ""
        }`}
      />
      <span className={styles.pulseLabel}>{state}</span>
    </span>
  );
}
