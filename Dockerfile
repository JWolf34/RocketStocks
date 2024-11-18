FROM python:3.12.2-slim-bullseye



COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

WORKDIR /RocketStocks
ADD . .

CMD ["python3", "rocketstocks.py"]
