import React, { useEffect, useState } from "react";
import axios from "axios";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts";

const logTypes = [
  "android-traces",
  "zookeeper-traces",
  "spark-traces",
  "bgl-traces",
  "openstack-traces",
];

export default function Dashboard() {
  const [logType, setLogType] = useState("android-traces");
  const [anomalies, setAnomalies] = useState([]);
  const [loading, setLoading] = useState(false);
  const [typeFilter, setTypeFilter] = useState("all");
  const [recFilter, setRecFilter] = useState("all");
  const [selectedAnomaly, setSelectedAnomaly] = useState(null);

  useEffect(() => {
    fetchAnomalies();
  }, [logType]);

  const fetchAnomalies = async () => {
    setLoading(true);
    try {
      const res = await axios.get(`http://localhost:8000/anomalies/${logType}`);
      setAnomalies(res.data);
    } catch (err) {
      console.error("API hatası:", err);
      setAnomalies([]);
    }
    setLoading(false);
  };

  const filtered = anomalies.filter((item) => {
    const typeMatch = typeFilter === "all" || item.type === typeFilter;
    const recMatch = recFilter === "all" || item.rec === recFilter;
    return typeMatch && recMatch;
  });

  const uniqueTypes = [...new Set(anomalies.map((a) => a.type))];
  const uniqueRecs = [...new Set(anomalies.map((a) => a.rec))];

  const exportToCSV = () => {
    const headers = ["trace_id", "event", "score", "rec", "type", "reason"];
    const rows = filtered.map(row =>
      headers.map(key => `"${row[key] ?? ''}"`).join(",")
    );
    const csvContent = [headers.join(","), ...rows].join("\n");
    const blob = new Blob([csvContent], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.setAttribute("href", url);
    link.setAttribute("download", `${logType}_anomalies.csv`);
    link.click();
  };

  return (
    <div className="container py-5">
      <h1 className="mb-4 display-5 fw-bold text-center">Anomaly Dashboard</h1>

      <div className="row g-3 mb-4">
        <div className="col-md-4">
          <label className="form-label fw-semibold">Log Type:</label>
          <select className="form-select" value={logType} onChange={(e) => setLogType(e.target.value)}>
            {logTypes.map((type) => (
              <option key={type} value={type}>{type}</option>
            ))}
          </select>
        </div>
        <div className="col-md-4">
          <label className="form-label fw-semibold">Filter by Type:</label>
          <select className="form-select" value={typeFilter} onChange={(e) => setTypeFilter(e.target.value)}>
            <option value="all">All</option>
            {uniqueTypes.map((t) => (
              <option key={t} value={t}>{t}</option>
            ))}
          </select>
        </div>
        <div className="col-md-4">
          <label className="form-label fw-semibold">Filter by Rec:</label>
          <select className="form-select" value={recFilter} onChange={(e) => setRecFilter(e.target.value)}>
            <option value="all">All</option>
            {uniqueRecs.map((r) => (
              <option key={r} value={r}>{r}</option>
            ))}
          </select>
        </div>
      </div>

      {loading ? (
        <div className="text-center py-5">
          <div className="spinner-border text-primary" role="status"></div>
        </div>
      ) : (
        <div className="table-responsive">
          <table className="table table-striped">
            <thead>
              <tr>
                <th>Trace ID</th>
                <th>Event</th>
                <th>Score</th>
                <th>Rec</th>
                <th>Type</th>
                <th>Reason</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((row, i) => (
                <tr key={i} onClick={() => setSelectedAnomaly(row)} style={{ cursor: "pointer" }}>
                  <td>{row.trace_id}</td>
                  <td>{row.event}</td>
                  <td>{row.score.toFixed(4)}</td>
                  <td><span className="badge bg-secondary">{row.rec}</span></td>
                  <td><span className="badge bg-info text-light">{row.type}</span></td>
                  <td>{row.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {filtered.length > 0 && (
        <div className="mt-5">
          <div className="d-flex justify-content-between align-items-center mb-3">
            <h2 className="h5 fw-bold">Score Distribution</h2>
            <button className="btn btn-outline-primary btn-sm" onClick={exportToCSV}>⬇ Export CSV</button>
          </div>
          <ResponsiveContainer width="100%" height={300}>
            <BarChart data={filtered}>
              <XAxis dataKey="trace_id" hide />
              <YAxis domain={["auto", "auto"]} />
              <Tooltip />
              <Bar dataKey="score" fill="#0d6efd" />
            </BarChart>
          </ResponsiveContainer>
        </div>
      )}

      {selectedAnomaly && (
        <div className="modal show d-block" tabIndex="-1" role="dialog">
          <div className="modal-dialog modal-dialog-centered" role="document">
            <div className="modal-content">
              <div className="modal-header">
                <h5 className="modal-title">Anomaly Details</h5>
                <button type="button" className="btn-close" onClick={() => setSelectedAnomaly(null)}></button>
              </div>
              <div className="modal-body">
                <p><strong>Trace ID:</strong> {selectedAnomaly.trace_id}</p>
                <p><strong>Event:</strong> {selectedAnomaly.event}</p>
                <p><strong>Score:</strong> {selectedAnomaly.score.toFixed(6)}</p>
                <p><strong>Recommendation:</strong> {selectedAnomaly.rec}</p>
                <p><strong>Type:</strong> {selectedAnomaly.type}</p>
                <p><strong>Reason:</strong> {selectedAnomaly.reason}</p>
              </div>
              <div className="modal-footer">
                <button className="btn btn-secondary" onClick={() => setSelectedAnomaly(null)}>Close</button>
              </div>
            </div>
          </div>
        </div>
      )}

      <footer className="text-center mt-5 pt-4 border-top small text-muted">
        <p>Developed by <a href="https://www.linkedin.com/in/berkan-parlak-375886220/" target="_blank" rel="noreferrer" className="text-decoration-none fw-semibold">Berkan Parlak</a> • <a href="https://github.com/berkanparlak" target="_blank" rel="noreferrer" className="text-decoration-none">GitHub</a></p>
        <p>This dashboard analyzes anomalies from various system logs using AI.</p>
      </footer>
    </div>
  );
}
