import sys
sys.path.append('discord')
import bot as Discord
import stockdata as sd
from multiprocessing import Process
import sched

def rocketStocks():
    sched.scheduler()



if (__name__ == '__main__'):

    rocketStocks = Process(target=rocketStocks())
    rocketStocks.start()

    discord = Process(target = Discord.run_bot())
    discord.start()

    rocketStocks.join()
    discord.join()