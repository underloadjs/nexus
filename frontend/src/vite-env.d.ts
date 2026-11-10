/// <reference types="vite/client" />

// Vite client types supply the ambient module declarations for asset imports
// (e.g. the `import './styles.css'` side-effect import in main.tsx), which
// `tsc --noEmit` would otherwise reject. Same convention as the
// distribution/* apps. No `__APP_VERSION__` define here: the monitor shows
// no version chrome, so nothing ambient is declared.
