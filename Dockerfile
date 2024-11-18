FROM python:3.12.2-slim-bullseye

WORKDIR /RocketStocks

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . /RocketStocks

CMD ["python3", "rocketstocks.py"]
