import { useState, useEffect, useRef } from "react";
import {
  BarChart, Bar, LineChart, Line, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell
} from "recharts";
import { uploadCSV, runQuery, getHistory, exportResults } from "./api";

const CHART_COLORS = ["#6366f1","#f59e0b","#10b981","#ef4444","#8b5cf6","#06b6d4"];

function formatValue(v) {
  if (typeof v === "number") {
    if (v > 1000) return "$" + v.toLocaleString("en-US", { maximumFractionDigits: 0 });
    return v.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }
  return v;
}

export default function App() {
  const [session, setSession]         = useState(null); // { session_id, schema, sample, row_count }
  const [messages, setMessages]       = useState([]);
  const [suggestions, setSuggestions] = useState([]);
  const [history, setHistoryList]     = useState([]);
  const [input, setInput]             = useState("");
  const [loading, setLoading]         = useState(false);
  const [uploading, setUploading]     = useState(false);
  const [showHistory, setShowHistory] = useState(false);
  const bottomRef  = useRef(null);
  const inputRef   = useRef(null);
  const fileRef    = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, loading]);

  // ── Upload CSV ────────────────────────────────────────────────────────
  async function handleUpload(e) {
    const file = e.target.files?.[0];
    if (!file) return;
    setUploading(true);
    setMessages([]);
    setHistoryList([]);
    try {
      const result = await uploadCSV(file);
      setSession({
        session_id: result.session_id,
        schema:     result.schema,
        sample:     result.sample,
        row_count:  result.row_count,
      });
      setSuggestions(result.suggestions || []);
      setMessages([{
        role: "assistant", type: "welcome",
        text: `✅ Loaded "${file.name}" — ${result.row_count.toLocaleString()} rows, ${result.schema.length} columns. Ask me anything about your data!`
      }]);
    } catch (err) {
      setMessages([{ role: "assistant", type: "error", text: "Upload failed: " + err.message }]);
    }
    setUploading(false);
  }

  // ── Fetch history ─────────────────────────────────────────────────────
  async function fetchHistory() {
    if (!session) return;
    try {
      const h = await getHistory(session.session_id);
      setHistoryList(h);
    } catch (_) {}
  }

  // ── Submit question ───────────────────────────────────────────────────
  async function handleSubmit(question) {
    const q = (question || input).trim();
    if (!q || loading || !session) return;
    setInput("");
    setLoading(true);
    setMessages(prev => [...prev, { role: "user", type: "user", text: q }]);

    try {
      const result = await runQuery(
        session.session_id, q,
        session.schema, session.sample, session.row_count
      );
      setMessages(prev => [...prev, { role: "assistant", type: "result", question: q, ...result }]);
      if (result.suggested_followups?.length) {
        setSuggestions(result.suggested_followups);
      }
      fetchHistory();
    } catch (err) {
      setMessages(prev => [...prev, { role: "assistant", type: "error", text: err.message }]);
    }
    setLoading(false);
    inputRef.current?.focus();
  }

  // ── Export ────────────────────────────────────────────────────────────
  async function handleExport(format, msg) {
    try {
      await exportResults(format, msg.question, msg.sql, msg.summary, msg.rows || []);
    } catch (err) {
      alert("Export failed: " + err.message);
    }
  }

  // ── Chart renderer ────────────────────────────────────────────────────
  function renderChart(msg) {
    const { rows, chart_type, x_key, y_key, chart_title } = msg;
    if (!rows?.length || chart_type === "none" || !x_key || !y_key) return null;
    const data = rows.slice(0, 12).map(r => ({
      ...r,
      [y_key]: typeof r[y_key] === "number" ? parseFloat(r[y_key].toFixed(2)) : r[y_key]
    }));
    const tickStyle  = { fill: "#94a3b8", fontSize: 11, fontFamily: "monospace" };
    const tooltipStyle = { background: "#0f172a", border: "1px solid #1e293b", borderRadius: 8, fontFamily: "monospace", fontSize: 12 };
    const commonProps  = { data, margin: { top: 8, right: 16, left: 8, bottom: 48 } };

    if (chart_type === "pie") return (
      <div style={{ textAlign: "center" }}>
        <p style={s.chartTitle}>{chart_title}</p>
        <ResponsiveContainer width="100%" height={220}>
          <PieChart>
            <Pie data={data} dataKey={y_key} nameKey={x_key} cx="50%" cy="50%" outerRadius={80}
              label={({ name, percent }) => `${name} ${(percent*100).toFixed(0)}%`} labelLine={false}>
              {data.map((_,i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
            </Pie>
            <Tooltip formatter={v => formatValue(v)} contentStyle={tooltipStyle} />
          </PieChart>
        </ResponsiveContainer>
      </div>
    );

    if (chart_type === "line") return (
      <div>
        <p style={s.chartTitle}>{chart_title}</p>
        <ResponsiveContainer width="100%" height={200}>
          <LineChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis dataKey={x_key} tick={tickStyle} angle={-30} textAnchor="end" interval={0} />
            <YAxis tick={tickStyle} tickFormatter={v => v > 999 ? `$${(v/1000).toFixed(0)}k` : v} />
            <Tooltip formatter={v => formatValue(v)} contentStyle={tooltipStyle} />
            <Line type="monotone" dataKey={y_key} stroke="#6366f1" strokeWidth={2.5}
              dot={{ fill: "#6366f1", r: 4 }} activeDot={{ r: 6 }} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    );

    return (
      <div>
        <p style={s.chartTitle}>{chart_title}</p>
        <ResponsiveContainer width="100%" height={200}>
          <BarChart {...commonProps}>
            <CartesianGrid strokeDasharray="3 3" stroke="#1e293b" />
            <XAxis dataKey={x_key} tick={tickStyle} angle={-30} textAnchor="end" interval={0} />
            <YAxis tick={tickStyle} tickFormatter={v => v > 999 ? `$${(v/1000).toFixed(0)}k` : v} />
            <Tooltip formatter={v => formatValue(v)} contentStyle={tooltipStyle} />
            <Bar dataKey={y_key} radius={[4,4,0,0]}>
              {data.map((_,i) => <Cell key={i} fill={CHART_COLORS[i % CHART_COLORS.length]} />)}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    );
  }

  // ── Table renderer ────────────────────────────────────────────────────
  function renderTable(rows) {
    if (!rows?.length) return <p style={{ color: "#64748b", fontSize: 13 }}>No results.</p>;
    const cols = Object.keys(rows[0]);
    return (
      <div style={{ overflowX: "auto", marginTop: 10 }}>
        <table style={{ width: "100%", borderCollapse: "collapse", fontFamily: "monospace", fontSize: 12 }}>
          <thead>
            <tr>{cols.map(c => (
              <th key={c} style={{ padding: "6px 12px", background: "#1e293b", color: "#94a3b8",
                textAlign: "left", textTransform: "uppercase", fontSize: 11,
                borderBottom: "1px solid #334155", letterSpacing: "0.05em" }}>{c}</th>
            ))}</tr>
          </thead>
          <tbody>
            {rows.slice(0,10).map((row,i) => (
              <tr key={i} style={{ borderBottom: "1px solid #1e293b",
                background: i % 2 === 0 ? "transparent" : "#0f172a33" }}>
                {cols.map(c => (
                  <td key={c} style={{ padding: "6px 12px", color: "#e2e8f0" }}>{formatValue(row[c])}</td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
        {rows.length > 10 && (
          <p style={{ color: "#475569", fontSize: 11, marginTop: 6, fontFamily: "monospace" }}>
            Showing 10 of {rows.length} rows
          </p>
        )}
      </div>
    );
  }

  // ── Styles ────────────────────────────────────────────────────────────
  const s = {
    root: { minHeight: "100vh", width: "100vw", background: "#020617", color: "#e2e8f0",
        fontFamily: "'Segoe UI', sans-serif", display: "flex", flexDirection: "column" },
    header:      { borderBottom: "1px solid #0f172a", padding: "14px 24px",
                   display: "flex", alignItems: "center", gap: 12,
                   background: "#020617", position: "sticky", top: 0, zIndex: 10 },
    logo:        { width: 32, height: 32, background: "linear-gradient(135deg,#6366f1,#8b5cf6)",
                   borderRadius: 8, display: "flex", alignItems: "center",
                   justifyContent: "center", fontSize: 16 },
    title:       { fontSize: 17, fontWeight: 700, color: "#f1f5f9" },
    headerRight: { marginLeft: "auto", display: "flex", gap: 10, alignItems: "center" },
    uploadBtn:   { fontSize: 12, background: "#6366f1", color: "#fff", border: "none",
                   padding: "6px 14px", borderRadius: 8, cursor: "pointer", fontWeight: 600 },
    histBtn:     { fontSize: 12, background: "#1e293b", color: "#94a3b8", border: "1px solid #334155",
                   padding: "6px 14px", borderRadius: 8, cursor: "pointer" },
    layout:      { display: "flex", flex: 1, overflow: "hidden" },
    // ── History sidebar
    sidebar:     { width: 260, background: "#0a0f1e", borderRight: "1px solid #1e293b",
                   overflowY: "auto", padding: "16px 12px", flexShrink: 0 },
    sideTitle:   { fontSize: 11, color: "#475569", fontFamily: "monospace",
                   textTransform: "uppercase", letterSpacing: "0.08em", marginBottom: 12 },
    histItem:    { padding: "8px 10px", borderRadius: 8, cursor: "pointer", marginBottom: 6,
                   background: "#0f172a", border: "1px solid #1e293b" },
    histQ:       { fontSize: 12, color: "#94a3b8", marginBottom: 2,
                   overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" },
    histMeta:    { fontSize: 10, color: "#334155", fontFamily: "monospace" },
    // ── Main chat
    main: { flex: 1, display: "flex", flexDirection: "column", overflowY: "auto",
        padding: "24px 16px 180px", background: "#020617" },
    chatInner:   { maxWidth: 760, width: "100%", margin: "0 auto", display: "flex",
                   flexDirection: "column", gap: 16 },
    userBubble:  { alignSelf: "flex-end", background: "#6366f1", color: "#fff",
                   padding: "10px 16px", borderRadius: "18px 18px 4px 18px",
                   maxWidth: "80%", fontSize: 14, lineHeight: 1.5 },
    aiBubble:    { alignSelf: "flex-start", background: "#0f172a", border: "1px solid #1e293b",
                   padding: "16px 18px", borderRadius: "4px 18px 18px 18px",
                   width: "100%", fontSize: 14, lineHeight: 1.6 },
    errorBubble: { alignSelf: "flex-start", background: "#1c0a0a", border: "1px solid #7f1d1d",
                   padding: "12px 16px", borderRadius: 12, color: "#fca5a5",
                   fontSize: 13, fontFamily: "monospace" },
    sqlBlock:    { background: "#020617", border: "1px solid #1e293b", borderRadius: 8,
                   padding: "10px 14px", fontFamily: "monospace", fontSize: 12,
                   color: "#7dd3fc", overflowX: "auto", margin: "10px 0", whiteSpace: "pre-wrap" },
    label:       { fontSize: 11, color: "#475569", fontFamily: "monospace",
                   letterSpacing: "0.08em", textTransform: "uppercase", marginBottom: 4 },
    summary:     { color: "#cbd5e1", fontSize: 14, lineHeight: 1.7, marginTop: 10,
                   borderLeft: "3px solid #6366f1", paddingLeft: 12 },
    divider:     { height: 1, background: "#1e293b", margin: "12px 0" },
    exportRow:   { display: "flex", gap: 8, marginTop: 12 },
    exportBtn:   { fontSize: 11, background: "#1e293b", border: "1px solid #334155",
                   color: "#94a3b8", padding: "4px 12px", borderRadius: 6,
                   cursor: "pointer", fontFamily: "monospace" },
    chartTitle:  { fontFamily: "monospace", fontSize: 11, color: "#64748b",
                   marginBottom: 8, letterSpacing: "0.05em", textTransform: "uppercase" },
    // ── Input bar
    inputBar:    { position: "fixed", bottom: 0, left: 0, right: 0,
                   background: "linear-gradient(to top, #020617 80%, transparent)",
                   padding: "16px 16px 24px" },
    inputInner:  { maxWidth: 760, margin: "0 auto" },
    suggestions: { display: "flex", gap: 8, flexWrap: "wrap", marginBottom: 10 },
    suggBtn:     { fontSize: 12, background: "#0f172a", border: "1px solid #1e293b",
                   color: "#94a3b8", padding: "5px 12px", borderRadius: 20,
                   cursor: "pointer", fontFamily: "monospace", transition: "all 0.15s" },
    inputRow:    { display: "flex", gap: 10, background: "#0f172a",
                   border: "1px solid #1e293b", borderRadius: 14, padding: "8px 8px 8px 16px" },
    input:       { flex: 1, background: "transparent", border: "none", outline: "none",
                   color: "#f1f5f9", fontSize: 14 },
    sendBtn:     { background: "#6366f1", color: "#fff", border: "none", borderRadius: 10,
                   padding: "8px 18px", cursor: "pointer", fontWeight: 600, fontSize: 14 },
  };

  // ── Render ────────────────────────────────────────────────────────────
  return (
    <>
      <link href="https://fonts.googleapis.com/css2?family=Sora:wght@400;600;700&display=swap" rel="stylesheet" />
      <div style={s.root}>

        {/* Header */}
        <div style={s.header}>
          <div style={s.logo}>🔍</div>
          <span style={s.title}>QueryMind</span>
          {session && (
            <span style={{ fontSize: 12, color: "#475569", fontFamily: "monospace", marginLeft: 8 }}>
              {session.row_count.toLocaleString()} rows · {session.schema.length} columns
            </span>
          )}
          <div style={s.headerRight}>
            {session && (
              <button style={s.histBtn} onClick={() => { setShowHistory(h => !h); fetchHistory(); }}>
                {showHistory ? "Hide History" : "Query History"}
              </button>
            )}
            <button style={s.uploadBtn} onClick={() => fileRef.current?.click()}>
              {uploading ? "Uploading…" : session ? "Upload New CSV" : "Upload CSV"}
            </button>
            <input ref={fileRef} type="file" accept=".csv" style={{ display: "none" }} onChange={handleUpload} />
          </div>
        </div>

        <div style={s.layout}>

          {/* History Sidebar */}
          {showHistory && (
            <div style={s.sidebar}>
              <div style={s.sideTitle}>Query History</div>
              {history.length === 0 && (
                <p style={{ color: "#334155", fontSize: 12, fontFamily: "monospace" }}>No queries yet.</p>
              )}
              {history.map((h, i) => (
                <div key={i} style={s.histItem}
                  onClick={() => handleSubmit(h.question)}>
                  <div style={s.histQ}>{h.question}</div>
                  <div style={s.histMeta}>{h.chart_type} · {h.row_count} rows</div>
                  <div style={{ ...s.histMeta, marginTop: 2 }}>
                    {new Date(h.timestamp).toLocaleTimeString()}
                  </div>
                </div>
              ))}
            </div>
          )}

          {/* Chat Area */}
          <div style={s.main}>
            <div style={s.chatInner}>

              {/* Welcome / no session */}
              {!session && !uploading && (
                <div style={{ textAlign: "center", marginTop: 80, color: "#334155" }}>
                  <div style={{ fontSize: 48, marginBottom: 16 }}>🔍</div>
                  <div style={{ fontSize: 20, fontWeight: 700, color: "#475569", marginBottom: 8 }}>
                    Welcome to QueryMind
                  </div>
                  <div style={{ fontSize: 14, color: "#334155" }}>
                    Upload a CSV file to get started
                  </div>
                  <button style={{ ...s.uploadBtn, marginTop: 24, padding: "10px 28px", fontSize: 14 }}
                    onClick={() => fileRef.current?.click()}>
                    Upload CSV File
                  </button>
                </div>
              )}

              {uploading && (
                <div style={{ textAlign: "center", marginTop: 80, color: "#475569" }}>
                  <div style={{ fontSize: 32 }}>⏳</div>
                  <div style={{ marginTop: 12, fontFamily: "monospace" }}>Uploading and analyzing your CSV…</div>
                </div>
              )}

              {/* Messages */}
              {messages.map((msg, i) => {
                if (msg.type === "user") return (
                  <div key={i} style={s.userBubble}>{msg.text}</div>
                );
                if (msg.type === "welcome") return (
                  <div key={i} style={s.aiBubble}>
                    <span style={{ color: "#94a3b8" }}>{msg.text}</span>
                  </div>
                );
                if (msg.type === "error") return (
                  <div key={i} style={s.errorBubble}>⚠️ {msg.text}</div>
                );
                if (msg.type === "result") {
                  const hasChart = msg.chart_type !== "none" && msg.x_key && msg.y_key && msg.rows?.length;
                  return (
                    <div key={i} style={s.aiBubble}>
                      <div style={s.label}>Generated SQL</div>
                      <div style={s.sqlBlock}>{msg.sql}</div>

                      {hasChart && (
                        <>
                          <div style={s.label}>Visualization</div>
                          {renderChart(msg)}
                        </>
                      )}

                      <div style={{ marginTop: hasChart ? 16 : 0 }}>
                        <div style={s.label}>Results ({msg.rows?.length} row{msg.rows?.length !== 1 ? "s" : ""})</div>
                        {renderTable(msg.rows)}
                      </div>

                      <div style={s.divider} />
                      <div style={s.label}>Insight</div>
                      <div style={s.summary}>{msg.summary}</div>

                      <div style={s.exportRow}>
                        <button style={s.exportBtn} onClick={() => handleExport("csv", msg)}>
                          ↓ Export CSV
                        </button>
                        <button style={s.exportBtn} onClick={() => handleExport("pdf", msg)}>
                          ↓ Export PDF
                        </button>
                      </div>
                    </div>
                  );
                }
                return null;
              })}

              {loading && (
                <div style={{ ...s.aiBubble, color: "#475569" }}>
                  <span style={{ fontFamily: "monospace", fontSize: 13 }}>
                    ⟳ Translating to SQL and running query…
                  </span>
                </div>
              )}

              <div ref={bottomRef} />
            </div>
          </div>
        </div>

        {/* Input Bar */}
        {session && (
          <div style={s.inputBar}>
            <div style={s.inputInner}>
              {suggestions.length > 0 && (
                <div style={s.suggestions}>
                  {suggestions.map((sg, i) => (
                    <button key={i} style={s.suggBtn}
                      onMouseEnter={e => { e.target.style.borderColor="#6366f1"; e.target.style.color="#6366f1"; }}
                      onMouseLeave={e => { e.target.style.borderColor="#1e293b"; e.target.style.color="#94a3b8"; }}
                      onClick={() => handleSubmit(sg)}>
                      {sg}
                    </button>
                  ))}
                </div>
              )}
              <div style={s.inputRow}>
                <input ref={inputRef} style={s.input} value={input}
                  onChange={e => setInput(e.target.value)}
                  onKeyDown={e => e.key === "Enter" && handleSubmit()}
                  placeholder="Ask anything about your data…"
                  disabled={loading} />
                <button style={{ ...s.sendBtn, opacity: loading ? 0.5 : 1 }}
                  onClick={() => handleSubmit()} disabled={loading}>
                  {loading ? "…" : "Run →"}
                </button>
              </div>
            </div>
          </div>
        )}

      </div>
    </>
  );
}
