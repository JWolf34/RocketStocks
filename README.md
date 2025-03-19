# RocketStocks :rocket:
RocketStocks is primarily a Discord bot that routinely posts alerts and reports on stocks that generate buy and sell signals based on a variety of factors such as stock price and volume movement and sentiment analysis from Reddit. Discord users can generate reports for a quick overview of a stock or access a stock's price data to perform analysis. The bot is equipped to deliver routine alerts for stocks that meet certain criteria in real-time to help made educated decisions in the market.

## Installation
It is recommended to deploy this app using Docker via the `docker-compose.yml` file in this repositsory as a template. However, an update needs to be made to the compose file before it is ready for use.

### Environment Variables

It is recommeneded to store the following environment varaibles in a `.env.` file that can be referenced by both the RocketStocks and PostgreSQL service so the bot can properly function. 
