import React from 'react';
import { createRoot } from 'react-dom/client';
import App from './App.jsx';
import SettingsAdminApp from './apps/SettingsAdminApp.jsx';
import './index.css';

const container = document.getElementById('react-root') || document.getElementById('root');
if (container) {
  const root = createRoot(container);
  const appKey = String(container.getAttribute('data-react-app') || '').trim().toLowerCase();
  if (appKey === 'settings-admin') {
    root.render(<SettingsAdminApp />);
  } else {
    root.render(<App />);
  }
}
