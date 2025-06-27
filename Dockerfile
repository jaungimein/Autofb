FROM python:3.11-slim

# Set work directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements (create requirements.txt if not present)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot code
COPY . .

# Expose FastAPI port (change if needed)
EXPOSE 8000

# Start by running update.py, then bot.py
CMD ["sh", "-c", "python update.py && python bot.py"]
