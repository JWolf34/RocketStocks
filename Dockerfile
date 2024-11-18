FROM python:3.12.2-slim-bullseye



COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN mkdir /RocketStocks
ADD . /RocketStocks
WORKDIR /RocketStocks

CMD ["python3", "rocketstocks.py"]
