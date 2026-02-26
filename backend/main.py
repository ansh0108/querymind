from export import export_csv, export_pdf
from llm import nl_to_sql, summarize, suggest_initial
from csv_handler import load_csv, run_query
from db import init_db, save_query, get_history
from fastapi import FastAPI, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
from pydantic import BaseModel
from dotenv import load_dotenv
import os

load_dotenv()  # reads your .env file


app = FastAPI()
app.add_middleware(CORSMiddleware,
                   allow_origins=['*'], allow_methods=['*'], allow_headers=['*'])
init_db()

# ── Upload CSV ───────────────────────────────────────────────────


@app.post('/upload')
async def upload(file: UploadFile):
    contents = await file.read()
    result = load_csv(contents, file.filename)
    suggestions = suggest_initial(result['schema'], result['sample'])
    return {**result, 'suggestions': suggestions}

# ── Run a query ───────────────────────────────────────────────────


class QueryRequest(BaseModel):
    session_id: str
    question:   str
    schema:     list
    sample:     list
    row_count:  int


@app.post('/query')
async def query(req: QueryRequest):
    parsed = nl_to_sql(req.question, req.schema,
                       req.sample, req.row_count)
    rows = run_query(req.session_id, parsed['sql'])
    summary = summarize(req.question, parsed['sql'], rows)
    save_query(req.session_id, req.question, parsed['sql'],
               summary, len(rows), parsed['chart_type'])
    return {**parsed, 'rows': rows[:500],
            'total_rows': len(rows), 'summary': summary}

# ── Query history ─────────────────────────────────────────────────


@app.get('/history/{session_id}')
def history(session_id: str):
    return get_history(session_id)

# ── Export ────────────────────────────────────────────────────────


class ExportRequest(BaseModel):
    format:   str
    question: str
    sql:      str
    summary:  str
    rows:     list


@app.post('/export')
def export(req: ExportRequest):
    if req.format == 'csv':
        data = export_csv(req.rows)
        return Response(data, media_type='text/csv',
                        headers={'Content-Disposition':
                                 'attachment; filename=results.csv'})
    elif req.format == 'pdf':
        data = export_pdf(req.question, req.sql,
                          req.summary, req.rows)
        return Response(data, media_type='application/pdf',
                        headers={'Content-Disposition':
                                 'attachment; filename=report.pdf'})
    raise HTTPException(400, 'Invalid format')
