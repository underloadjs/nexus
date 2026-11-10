// Presentational primitives shared by every panel (no data logic here).
//
// The dashboard intentionally has no component library: these five small
// pieces define the whole visual language, so a reviewer can read the CSS in
// one file and every panel stays structurally identical.
import type { PropsWithChildren, ReactNode } from 'react';

// Semantic tones reused for chips, stat values and edge colours. Unknown
// hub values (a newer enum member) map to 'neutral' — never an error.
export type Tone = 'neutral' | 'ok' | 'warn' | 'bad' | 'info';

export interface PanelProps extends PropsWithChildren {
    title: string;
    // Right-aligned badge (usually a row count: "4 pools", "12 rows").
    count?: number | string;
    // Short explanation shown under the title — every panel gets one so the
    // meaning of a number is never guesswork.
    hint?: ReactNode;
    // Optional controls (filter inputs, toggles) rendered in the header.
    actions?: ReactNode;
}

// One card of the dashboard grid.
export function Panel({ title, count, hint, actions, children }: PanelProps) {
    return (
        <section className="panel">
            <header className="panel-head">
                <div className="panel-heading">
                    <h2>{title}</h2>
                    {hint ? <p className="panel-hint">{hint}</p> : null}
                </div>
                <div className="panel-side">
                    {count === undefined ? null : <span className="panel-count">{count}</span>}
                    {actions ?? null}
                </div>
            </header>
            <div className="panel-body">{children}</div>
        </section>
    );
}

// Friendly zero-state. Every panel distinguishes "nothing yet" from
// "something is wrong", so an empty hub never looks like a broken UI.
export function EmptyState({ message, hint }: { message: string; hint?: string }) {
    return (
        <div className="empty">
            <span className="empty-message">{message}</span>
            {hint ? <span className="empty-hint">{hint}</span> : null}
        </div>
    );
}

export interface StatCardProps {
    label: string;
    value: number | string;
    tone?: Tone;
    // Secondary line (e.g. "2 ready / 0 loading").
    sub?: string;
}

// Single headline number on the Overview panel.
export function StatCard({ label, value, tone = 'neutral', sub }: StatCardProps) {
    return (
        <div className={`stat-card tone-${tone}`}>
            <span className="stat-label">{label}</span>
            <span className="stat-value">{value}</span>
            {sub ? <span className="stat-sub">{sub}</span> : null}
        </div>
    );
}

// Small coloured pill for enum-ish state (link, engine, phase, capacity).
export function StateChip({ label, tone = 'neutral', title }: { label: string; tone?: Tone; title?: string }) {
    return (
        <span className={`chip tone-${tone}`} title={title}>
            {label}
        </span>
    );
}
