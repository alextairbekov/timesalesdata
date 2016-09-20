import os.path
from os import listdir
from os.path import isfile, join
from time import gmtime, strftime

# read from the parameters the location of data directory
def collect(stock, ex, date0, date1, time, max):
    #This collects the relevant data in raw format.
    #Input: the stock, the start date, the end date, the end time, and the number of ticks to collect
    #Output: raw rows of data
    import httplib
    import json
    time = "21:00:00"
    conn = httplib.HTTPConnection("quotes.wsj.com")
    url = '/tradehistory/ticks?symbol=stock/us/' + ex + '/' + stock + '&starttimeutc=' + date0 + '&endtimeutc=' + \
          date1 +'%20' + time + '&maxTicks=' + max
    #print ('quotes.wsj.com' + url)
    conn.request("GET", url)
    r1 = conn.getresponse()
    #print r1.status, r1.reason
    data2 = r1.read()
    json_data = json.loads(data2)
    ticks = json_data['ticks']
    return ticks

def write_data(stock1, ex, latest_data_time, n):
    #This puts rows of data into a stack, then merges it with existing data.
    #Input: the stock, and the time of the latest row of data
    #Output: saves to csv file, and returns the new time of the latest row of data

    import csv

    time = strftime("%H:%M:%S", gmtime())
    while True:
        try:
            ticks = collect(stock1, ex, date, date, time, str(n))
        except:
            print ("Error caught, trying again...")
            continue
        else:
            break
    stack = []
    for item in ticks:
        stack.append([item['d'], item['l'], item['v'], item['e'], item['tc']])
        temp_time0 = item['d']
    if ticks == []:
        print ("No data could be collected at this time.")
        quit()
    temp_time1 = ticks[0]['d']
    if latest_data_time == 0:
        csv1 = csv.writer(open(data_dir + STOCK_DIR + date + '/' + exchange + '/' + stock1 + '.csv', 'wb'))
    else:
        csv1 = csv.writer(open(data_dir + STOCK_DIR + date + '/' + exchange + '/' + stock1 + '.csv', 'ab'))
    #earlier time < later time
    if latest_data_time < temp_time0:
        if latest_data_time != "":
            print ("Consider choosing more ticks, a gap may exist.")
        while stack != []:
            csv1.writerow(stack.pop())
    else:
        temp = stack.pop()
        while temp[0] <= latest_data_time and stack != []:
            temp = stack.pop()
        if stack != []: csv1.writerow(temp)
        while stack != []:
            csv1.writerow(stack.pop())
    return temp_time1


data_dir = "/home/alex/oss/data/"
EXCH_DIR = "exch"
STOCK_DIR = "stocks/"
date = strftime("%Y-%m-%d")
#date = "2014-03-10"
err = open(data_dir + STOCK_DIR + 'err.txt', 'a')
latest_data = [""] * 500
p = 0
its = 0
amount_to_request = 999999
exchange = ""

if not os.path.exists(data_dir + STOCK_DIR + date):
    os.makedirs(data_dir + STOCK_DIR + date)

only_files = [f for f in listdir(data_dir + EXCH_DIR) if isfile(join(data_dir + EXCH_DIR + "/", f))]

while True:
    for item in only_files:
        exchange = item.rstrip('.txt')
        if not os.path.exists(data_dir + STOCK_DIR + date + '/' + exchange):
            os.makedirs(data_dir + STOCK_DIR + date + '/' + exchange)
        file1 = open(data_dir + EXCH_DIR + '/' + exchange + '.txt', 'r')
        for stock in file1:
            #if p >= 109:
            latest_data[p] = write_data(stock.rstrip('\n'), exchange, latest_data[p], amount_to_request)
            p += 1
            print(p)
    print ("Done.")
    quit()
    its += 1
    p = 0
    amount_to_request = 1000
