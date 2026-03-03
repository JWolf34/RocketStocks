FROM python:3.13-slim

WORKDIR /RocketStocks

# Install dependencies first for better layer caching
COPY pyproject.toml ./
COPY src/ src/
RUN pip install --no-cache-dir .

# Copy remaining project files
COPY . .

CMD ["python", "-m", "rocketstocks"]
