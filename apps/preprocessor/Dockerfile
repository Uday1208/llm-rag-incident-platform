FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Copy shared modules from ingestor
COPY ../ingestor/modules ./ingestor/modules

# Environment defaults
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=INFO
ENV PORT=8000

# Run the application
EXPOSE 8000
CMD ["python", "-m", "main"]
