FROM python:3.12.3

WORKDIR /RocketStocks

COPY requirements.txt pyproject.toml ./
RUN pip install -r requirements.txt && pip install -e .

COPY . .

CMD ["python3", "-m", "rocketstocks"]
