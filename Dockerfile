FROM python:3.12-slim

WORKDIR /RocketStocks

# git required to install pandas-ta from source (no numpy-1.x-compatible PyPI release)
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*

# Install dependencies first for better layer caching
COPY pyproject.toml ./
COPY src/ src/
RUN pip install --no-cache-dir .

# Copy remaining project files
COPY . .

CMD ["python", "-m", "rocketstocks"]
