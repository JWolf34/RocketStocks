FROM python:3.12.0b4-bookworm

ADD . /usr/src/RocketStocks
WORKDIR /usr/src/RocketStocks

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . . 

CMD ["python3", "rocketstocks.py"]
