const BASE = import.meta.env.VITE_API_URL;
 
export async function uploadCSV(file) {
  const form = new FormData();
  form.append('file', file);
  const res = await fetch(`${BASE}/upload`,
      { method: 'POST', body: form });
  return res.json();
}
 
export async function runQuery(
    sessionId, question, schema, sample, rowCount) {
  const res = await fetch(`${BASE}/query`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      session_id: sessionId, question,
      schema, sample, row_count: rowCount
    })
  });
  return res.json();
}
 
export async function getHistory(sessionId) {
  const res = await fetch(`${BASE}/history/${sessionId}`);
  return res.json();
}
 
export async function exportResults(
    format, question, sql, summary, rows) {
  const res = await fetch(`${BASE}/export`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ format, question, sql, summary, rows })
  });
  const blob = await res.blob();
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  a.href     = url;
  a.download = format === 'csv' ? 'results.csv' : 'report.pdf';
  a.click();
}

