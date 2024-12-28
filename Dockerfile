FROM python:3.12.3

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

WORKDIR /RocketStocks
COPY . .

CMD ["python3", "rocketstocks.py"]
