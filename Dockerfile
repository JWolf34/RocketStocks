FROM python:3.12.2-slim-bullseye

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

ADD . /RocketStocks
WORKDIR /RocketStocks
COPY . .

CMD ["python3", "rocketstocks.py"]
