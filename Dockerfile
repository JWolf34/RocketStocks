FROM python:3.12.0b4-bookworm

ADD . /usr/src/RocketStocks
WORKDIR /usr/src/RocketStocks

RUN pip install APScheduler
#RUN pip install discord
#RUN pip install discord.py
#RUN pip install pandas
#RUN pip install pandas-datareader
#RUN pip install scipy
#RUN pip install yahoo-fin
#RUN pip install yfinance
#COPY requirements.txt requirements.txt
#RUN pip install -r requirements.txt

COPY . . 

CMD ["python3", "rocketstocks.py"]
