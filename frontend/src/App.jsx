import React from 'react';

export default function App() {
  return (
    <section className="react-card">
      <div className="react-card-header">
        <h2>Live resource pulse</h2>
        <span className="react-pill">React</span>
      </div>
      <p>
        This widget is rendered by React (Vite). Use it for real-time charts,
        incident feeds, or collaboration modules.
      </p>
      <div className="react-grid">
        <div>
          <div className="react-metric">312</div>
          <div className="react-label">Active connections</div>
        </div>
        <div>
          <div className="react-metric">1.3s</div>
          <div className="react-label">P95 latency</div>
        </div>
        <div>
          <div className="react-metric">0</div>
          <div className="react-label">Open incidents</div>
        </div>
      </div>
    </section>
  );
}
