FROM python:3.12.0b4-bookworm

ADD . /RocketStocks
WORKDIR /RocketStocks

RUN pip install pip==23.2.1
RUN pip install APScheduler
RUN pip install aiohttp==3.8.2
RUN pip install yarl==1.8.1
RUN Pip install frozenlist==1.3.1
RUN pip install discord
RUN pip install discord.py
#RUN pip install pandas
#RUN pip install pandas-datareader
#RUN pip install scipy
#RUN pip install yahoo-fin
#RUN pip install yfinance
#COPY requirements.txt requirements.txt
#RUN pip install -r requirements.txt

COPY . . 

CMD ["python3", "rocketstocks.py"]
