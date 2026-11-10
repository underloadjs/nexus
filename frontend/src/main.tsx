// Browser entry point for the Nexus Monitor bundle.
//
// StrictMode is kept on purpose: in development it mounts the poll effect
// twice, which exercises the abort/cleanup path (a cancelled first attempt
// must resolve silently) before that code ever ships to Pages.
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import { App } from './App';
import './styles.css';

const container = document.getElementById('root');

if (!container) {
    // index.html always provides #root; a missing node means the bundle was
    // served from a page that is not ours (wrong base path on Pages), which
    // is far easier to diagnose from the console than from a blank screen.
    throw new Error('Nexus Monitor: #root not found — check the deployment base path');
}

createRoot(container).render(
    <StrictMode>
        <App />
    </StrictMode>
);
