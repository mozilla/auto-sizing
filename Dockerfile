ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}

ARG PIP_VERSION=23.0
COPY requirements.txt requirements.txt
RUN python -m pip install --no-cache-dir --upgrade pip==${PIP_VERSION} \
    && python -m pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /app
WORKDIR /app
COPY . .

RUN python -m pip install --no-cache-dir .

ENTRYPOINT ["auto_sizing"]
