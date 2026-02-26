# QueryMind — Natural Language Analytics Assistant

Upload any CSV and ask questions in plain English.
AI converts your question to SQL, runs it, and visualizes the results.

## Tech Stack
- **Frontend**: React + Vite + Recharts
- **Backend**: FastAPI + DuckDB
- **AI**: Groq (LLaMA 3.3 70B)
- **Export**: PDF + CSV

## Features
- Upload any CSV file
- Ask questions in plain English
- Auto-generated charts
- Query history
- Export results to PDF or CSV
- Context-aware follow-up suggestions

## How to Run
1. Clone the repo: `git clone https://github.com/ansh0108/querymind`
2. Add `backend/.env` with your `GROQ_API_KEY`
3. Backend: `cd backend && pip install -r requirements.txt && uvicorn main:app --reload`
4. Frontend: `cd frontend && npm install && npm run dev`
5. Open http://localhost:5173


## Built By
**Ansh Dasrapuria** — MS Information Management, UIUC (May 2026)  
[LinkedIn](https://linkedin.com/in/your-profile) · [GitHub](https://github.com/anshdasrapuria)
