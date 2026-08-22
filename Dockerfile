FROM python:3.12-slim

WORKDIR /opt/dagster

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PYTHONPATH=/opt/dagster

EXPOSE 3000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]