import sys
sys.path.append('discord')
import bot as Discord
import stockdata as sd
import threading
import scheduler

def rocketStocks():

    

    bot_thread = threading.Thread(target=Discord.run_bot)
    scheduler_thread = threading.Thread(target=scheduler.scheduler)

    bot_thread.start()
    scheduler_thread.start()
    
    bot_thread.join()

    



if (__name__ == '__main__'):
    rocketStocks()
    #subprocess.run(["python", "scheduler.py"])

    #subprocess.run(["python", "discord/bot.py"])

    

    