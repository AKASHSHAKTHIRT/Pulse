# Start with Python 3.11
FROM python:3.11-slim

# Set working directory inside the box
WORKDIR /app

# Copy requirements first
COPY requirements.txt .

# Install dependencies
RUN pip install -r requirements.txt

# Copy your files into the box
COPY server.py .
COPY core.py .
COPY static/ ./static/

# Open port 5000
EXPOSE 4000

# Command to start the app
CMD ["python", "server.py"]