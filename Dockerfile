FROM python:3.13

WORKDIR /RocketStocks

COPY requirements.txt pyproject.toml ./
RUN pip install -r requirements.txt && pip install -e .

COPY . .

CMD ["python3", "-m", "rocketstocks"]
