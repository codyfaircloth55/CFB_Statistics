FROM alpine:latest
RUN apk add --no-cache python3 py3-pip
WORKDIR /app
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY . .
EXPOSE 8050
CMD ["python3", "cfb_dash.py"]