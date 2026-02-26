from groq import Groq
import json
import os
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv('GROQ_API_KEY'))


def build_system_prompt(schema, sample, row_count):
    return f"""You are a DuckDB SQL analyst.
The user uploaded a CSV as a table called 'data'.

Schema: {json.dumps(schema, indent=2)}
Sample (first 3 rows): {json.dumps(sample, indent=2)}
Total rows: {row_count}

Respond ONLY with this JSON and nothing else — no extra text, no markdown backticks:
{{
  "sql": "SELECT ...",
  "explanation": "one sentence",
  "chart_type": "bar or line or pie or none",
  "x_key": "column name or null",
  "y_key": "column name or null",
  "chart_title": "short title",
  "suggested_followups": ["q1", "q2", "q3"]
}}

Rules:
- Always query the table named 'data'
- Use DuckDB SQL syntax
- Alias all aggregated columns clearly (e.g. AS total_revenue)
- suggested_followups must be 3 natural follow-up questions"""


def nl_to_sql(question, schema, sample, row_count):
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[
            {"role": "system", "content": build_system_prompt(
                schema, sample, row_count)},
            {"role": "user", "content": question}
        ],
        temperature=0
    )
    raw = resp.choices[0].message.content
    raw = raw.replace('```json', '').replace('```', '').strip()
    return json.loads(raw)


def summarize(question, sql, rows):
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content":
                   f"Question: {question}\nSQL: {sql}\n"
                   f"Results: {json.dumps(rows[:20])}\n"
                   "Write 2-3 sentence plain-English business insight. Be specific with numbers."
                   }]
    )
    return resp.choices[0].message.content


def suggest_initial(schema, sample):
    resp = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content":
                   f"Schema: {json.dumps(schema)}\n"
                   f"Sample: {json.dumps(sample[:2])}\n"
                   "Return ONLY a JSON array of 6 strings. Each string is a question a business user would ask about this data. "
                   "Example format: [\"What is the total revenue?\", \"Which category sells most?\"] "
                   "No markdown, no extra text, no objects, just a flat JSON array of 6 strings."
                   }]
    )
    raw = resp.choices[0].message.content
    raw = raw.replace('```json', '').replace('```', '').strip()
    result = json.loads(raw)
    # Make absolutely sure it's a flat list of strings
    flat = []
    for item in result:
        if isinstance(item, dict):
            flat.append(list(item.values())[0])
        else:
            flat.append(str(item))
    return flat
