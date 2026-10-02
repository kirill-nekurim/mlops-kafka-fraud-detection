FROM python:3.12-slim

WORKDIR /app

# конфиг streamlit (лимит на размер файла)
COPY .streamlit/config.toml /root/.streamlit/config.toml

# сначала ставим зависимости, чтобы слой кешировался
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
