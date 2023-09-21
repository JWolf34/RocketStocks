FROM python:3.9-slim-bullseye

ADD . /RocketStocks
WORKDIR /RocketStocks

RUN pip install pip==23.2.1
RUN pip install APScheduler
RUN pip install discord
RUN pip install discord.py
RUN pip install pandas
RUN pip install pandas-datareader
RUN pip install scipy
RUN pip install yahoo-fin
RUN pip install yfinance
RUN pip install matplotlib
#COPY requirements.txt requirements.txt
#RUN pip install -r requirements.txt

COPY . . 

CMD ["python3", "rocketstocks.py"]
