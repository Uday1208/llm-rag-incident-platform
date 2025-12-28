FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY apps/preprocessor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY apps/preprocessor .

# Copy shared modules from ingestor
COPY apps/ingestor/modules ./ingestor/modules

# Environment defaults
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV PORT=8000

# Run the application
EXPOSE 8000
CMD ["python", "-m", "main"]
