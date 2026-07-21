FROM python:3.11-slim
RUN apt-get update && apt-get install -y git curl ffmpeg python3-pip wget bash && apt-get clean && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .

RUN pip3 install wheel
RUN pip3 install --no-cache-dir -U -r requirements.txt
COPY . .
RUN chmod +x start.sh
EXPOSE 5000

CMD ["bash", "start.sh"]
