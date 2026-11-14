// One shared "now" clock for every relative timestamp in the dashboard.
//
// The poll already re-renders once a second while connected, but ages must
// keep advancing while polling is failing or backed off (that is exactly when
// "last updated 47s ago" matters most), so the ticker is independent.
import { useEffect, useState } from 'react';

export const TICK_INTERVAL_MS = 1000;

export function useNowTick(intervalMs: number = TICK_INTERVAL_MS): number {
    const [nowMs, setNowMs] = useState(() => Date.now());
    useEffect(() => {
        // The interval never needs to restart (intervalMs is a constant in
        // practice), and setNowMs always re-renders the tree that reads ages.
        const timer = setInterval(() => setNowMs(Date.now()), intervalMs);
        return () => clearInterval(timer);
    }, [intervalMs]);
    return nowMs;
}
