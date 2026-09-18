FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt requirements-dashboard.txt ./
RUN pip install --no-cache-dir -r requirements-dashboard.txt

EXPOSE 8501
CMD ["streamlit", "run", "dashboard.py", "--server.address=0.0.0.0", "--server.port=8501"]
