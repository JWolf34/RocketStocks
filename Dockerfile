FROM python:3.13-slim

WORKDIR /RocketStocks

# Install dependencies first for better layer caching
COPY legacy/pyproject.toml ./
COPY legacy/src/ src/
RUN pip install --no-cache-dir .

# Copy remaining v1 project files
COPY legacy/ .

CMD ["python", "-m", "rocketstocks"]
