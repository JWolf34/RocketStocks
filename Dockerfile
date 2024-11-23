#FROM python:3.12.2-slim-bullseye
FROM python

WORKDIR /RocketStocks
COPY . /RocketStocks

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
#
#ADD . /RocketStocks
#WORKDIR /RocketStocks
#COPY . /RocketStocks

CMD ["python3", "rocketstocks.py"]
