import sys
sys.path.append('discord')
import bot as Discord
import stockdata as sd
import subprocess
import scheduler

def rocketStocks():
    scheduler.scheduler()



if (__name__ == '__main__'):

    subprocess.run(["python", "discord/bot.py"])

    subprocess.run(["python", "scheduler.py"])

    '''
    rocketStocks = Process(target=rocketStocks())
    rocketStocks.start()

    discord = Process(target = Discord.run_bot())
    discord.start()

    rocketStocks.join()
    discord.join()
    '''