# F. Application: one Streamlit app with three tabs (sheet: Tổng quan / Phân tích / Vận hành)
FROM python:3.11-slim
RUN pip install --no-cache-dir streamlit==1.40.2 clickhouse-connect==0.8.18 \
      psycopg2-binary==2.9.10 pandas==2.2.3 altair==5.5.0 \
      google-genai \
      chromadb
WORKDIR /app
COPY . .
EXPOSE 8501
HEALTHCHECK CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--browser.gatherUsageStats=false"]
