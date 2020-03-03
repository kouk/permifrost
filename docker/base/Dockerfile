FROM python:3.8

RUN echo "Installing system deps" \
    && apt-get update \
    && apt-get install -y build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# permifrost core deps
COPY requirements.txt .
RUN pip install -r requirements.txt

CMD ['python']
