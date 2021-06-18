from nsetools import Nse
import threading
import sys
from queue import Queue
from datetime import date, datetime
import os.path
import csv

concurrent = 2000
timeIntervalInSeconds = 5

nse = Nse()
q = None

def getQuoteInfo(quote):
    quote_info = nse.get_quote(quote)
    print(quote_info['companyName'])
    return quote_info

def logQuoteInfo(quote_info):
    quote_file_location = f"./files/{quote_info['symbol']}.csv"
    headers = quote_info.keys()
    row=[quote_info]
    if os.path.exists(quote_file_location):
        with open(quote_file_location, 'a', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writerows(row)
    else:
        with open(quote_file_location, 'w', encoding='UTF8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(row)

def getAllStockCodes():
    # all_stock_codes = nse.get_stock_codes()
    all_stock_codes = {"RELIANCE": "RELIANCE", "INFY": "INFY", "ITC": "ITC", "MARUTI": "MARUTI"}
    if "SYMBOL" in all_stock_codes:
        del all_stock_codes["SYMBOL"]
    return all_stock_codes

def getStockInfoConcurrently():
    stock_code = q.get()
    quote_info = getQuoteInfo(stock_code)
    logQuoteInfo(quote_info)
    q.task_done()

def getStockInfoAndLog():
    threading.Timer(timeIntervalInSeconds, getQuoteRecursively).start()
    print("************************************************** Getting Quote at: " + str(datetime.now()) + " **************************************************")
    for i in range(concurrent):
        t = threading.Thread(target=getStockInfoConcurrently)
        t.daemon = True
        t.start()
    try:
        nifty_stock_codes = getAllStockCodes()
        for stock in nifty_stock_codes:
            q.put(stock)
        q.join()
    except KeyboardInterrupt:
        sys.exit(1)

def getQuoteRecursively():
    now = datetime.utcnow()
    if (now.hour > 4 and now.hour < 11) or (now.hour == 4 and now.minute > 44):
        getStockInfoAndLog()        
    else:
        print("Not a trading time")

q = Queue(concurrent * 2)
getQuoteRecursively()
