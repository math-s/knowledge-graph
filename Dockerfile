FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy API code
COPY api/ api/

# Copy the database
COPY data/knowledge-graph.db data/knowledge-graph.db

ENV KG_DB_PATH=/app/data/knowledge-graph.db

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
