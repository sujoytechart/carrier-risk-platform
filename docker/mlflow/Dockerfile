FROM python:3.12-slim

WORKDIR /opt/carrier-risk
COPY . .
RUN pip install --no-cache-dir '.[model]' \
    && useradd --uid 10001 --create-home tracker
USER tracker
EXPOSE 5000
