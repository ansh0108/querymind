import io
import csv
from reportlab.lib.pagesizes import letter
from reportlab.platypus import (SimpleDocTemplate, Table,
                                TableStyle, Paragraph, Spacer)
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib import colors


def export_csv(rows: list) -> bytes:
    if not rows:
        return b''
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue().encode()


def export_pdf(question, sql, summary, rows) -> bytes:
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    story = []

    story.append(Paragraph('QueryMind Report', styles['Title']))
    story.append(Spacer(1, 12))
    story.append(Paragraph(f'<b>Question:</b> {question}',
                           styles['Normal']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f'<b>SQL:</b> {sql}',
                           styles['Code']))
    story.append(Spacer(1, 6))
    story.append(Paragraph(f'<b>Insight:</b> {summary}',
                           styles['Normal']))
    story.append(Spacer(1, 12))

    if rows:
        cols = list(rows[0].keys())
        data = [cols] + [[str(r.get(c, '')) for c in cols]
                         for r in rows[:50]]
        t = Table(data)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#4F46E5')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('FONTSIZE', (0, 0), (-1, -1), 8),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1),
             [colors.white, colors.HexColor('#EEF2FF')]),
        ]))
        story.append(t)

    doc.build(story)
    return buffer.getvalue()
