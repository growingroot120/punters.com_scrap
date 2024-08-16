import gzip, time, datetime, os, logging, argparse
import csv, re
from lxml import etree

try: #python3
    from urllib.request import urlopen as urlopen_internal, Request
    from urllib.parse import urlparse, urljoin
    from urllib.error import HTTPError
    from io import StringIO, BytesIO
    from http.cookiejar import CookieJar
    from urllib.request import build_opener, HTTPCookieProcessor, install_opener
except ImportError: #python2
    from urllib2 import urlopen as urlopen_internal, Request, HTTPError
    from urlparse import urlparse, urljoin
    from codecs import open
    from StringIO import StringIO
    from StringIO import StringIO as BytesIO
    from cookielib import CookieJar
    from urllib2 import build_opener, HTTPCookieProcessor, install_opener

cacheDir = 'cache'

loglevel=logging.INFO
#loglevel=logging.DEBUG
logging.basicConfig(level=loglevel,format='%(asctime)s %(levelname)s:%(message)s')
               
csvHeaders = ['meetingName', 'Date', 'Race',
              'Num', 'Horse Name', 'Age', 'Gender', 'Handicap Rating', 'Career Runs', 'Career Wins', 'Career Strike Rate', 'Career ROI', 'Career Placings', 'Career Place Strike Rate', 'Dry Track Runs', 'Dry Track Wins', 'Dry Track Strike Rate', 'Dry Track ROI', 'Wet Track Runs', 'Wet Track Wins', 'Wet Track Strike Rate', 'Wet Track ROI', 'Average Prize Money', 'Career Prize Money', 'Best Fixed Odds', 'BetEasy Odds', 'Weight', 'Weight Carried', 'Barrier', 'Prize Money', 'This Track Runs', 'This Track Wins', 'This Track Strike Rate', 'This Track ROI', 'This Track Places', 'This Track Place Strike Rate', 'This Distance Runs', 'This Distance Wins', 'This Distance Strike Rate', 'This Distance ROI', 'This Distance Places', 'This Distance Place Strike Rate', 'This Track Distance Runs', 'This Track Distance Wins', 'This Track Distance Strike Rate', 'This Track Distance ROI', 'This Track Distance Places', 'This Track Distance Place Strike Rate', 'This Condition Runs', 'This Condition Wins', 'This Condition Strike Rate', 'This Condition ROI', 'This Condition Places', 'This Condition Place Strike Rate', 'Jockey', 'Apprentice', 'Jockey Weight Claim', 'Jockey Last 100 Horse Earnings', 'Jockey Last 100 Avg Horse Earnings', 'Jockey Last 100 Starts', 'Jockey Last 100 Wins', 'Jockey Last 100 Strike Rate', 'Jockey Last 100 ROI', 'Jockey Last 100 Places', 'Jockey Last 100 Place Strike Rate', 'Jockey 12 Month Horse Earnings', 'Jockey 12 Month Avg Horse Earnings', 'Jockey 12 Months Starts', 'Jockey 12 Months Wins', 'Jockey 12 Months Strike Rate', 'Jockey 12 Months ROI', 'Jockey 12 Months Places', 'Jockey 12 Months Place Strike Rate', 'Jockey This Season Horse Earnings', 'Jockey This Season Avg Horse Earnings', 'Jockey This Season Starts', 'Jockey This Season Wins', 'Jockey This Season Strike Rate', 'Jockey This Season ROI', 'Jockey This Season Places', 'Jockey This Season Place Strike Rate', 'Jockey Last Season Horse Earnings', 'Jockey Last Season Avg Horse Earnings', 'Jockey Last Season Starts', 'Jockey Last Season Wins', 'Jockey Last Season Strike Rate', 'Jockey Last Season ROI', 'Jockey Last Season Places', 'Jockey Last Season Place Strike Rate', 'Trainer', 'Trainer Last 100 Horse Earnings', 'Trainer Last 100 Avg Horse Earnings', 'Trainer Last 100 Starts', 'Trainer Last 100 Wins', 'Trainer Last 100 Strike Rate', 'Trainer Last 100 ROI', 'Trainer Last 100 Places', 'Trainer Last 100 Place Strike Rate', 'Trainer 12 Month Horse Earnings', 'Trainer 12 Month Avg Horse Earnings', 'Trainer 12 Months Starts', 'Trainer 12 Months Wins', 'Trainer 12 Months Strike Rate', 'Trainer 12 Months ROI', 'Trainer 12 Months Places', 'Trainer 12 Months Place Strike Rate', 'Trainer This Season Horse Earnings', 'Trainer This Season Avg Horse Earnings', 'Trainer This Season Starts', 'Trainer This Season Wins', 'Trainer This Season Strike Rate', 'Trainer This Season ROI', 'Trainer This Season Places', 'Trainer This Season Place Strike Rate', 'Trainer Last Season Horse Earnings', 'Trainer Last Season Avg Horse Earnings', 'Trainer Last Season Starts', 'Trainer Last Season Wins', 'Trainer Last Season Strike Rate', 'Trainer Last Season ROI', 'Trainer Last Season Places', 'Trainer Last Season Place Strike Rate', 'Last Start Finish Position', 'Last Start Margin', 'Last Start Distance', 'Last Start Distance Change', 'Last Start Prize Money', 'Form Guide Url', 'Horse Profile Url', 'Jockey Profile Url', 'Trainer Profile Url', 'Finish Result (Updates after race)',
              'rail', 'Distance', 'Gear Changes', 'Race Class Details']

#headers for requests
stdheaders = {
    'Accept-Encoding':'gzip',
    'Accept-Language':'en-US',
    'Cache-Control':'no-cache',
    'Connection':'keep-alive',
    'Pragma':'no-cache',
    'User-Agent':'Mozilla/5.0 (Windows;U;Windows NT 6.1; en-US) Gecko/20100101 Firefox/35.0'
}

cj = CookieJar()
opener = build_opener(HTTPCookieProcessor(cj))
install_opener(opener)

#function to open url with several attempts
def urlopen(url,cached,max_retry_count=10):

    cacheFileName = os.path.join(cacheDir,urlparse(url).path.strip('/'))

    html = ''
    if os.path.exists(cacheFileName):
        logging.debug('Use cached file %s for url %s', cacheFileName, url)
        html = open(cacheFileName,encoding='UTF-8').read()

    if not html:
        retrycount = 0
        html = ''
    
        lastException = None
        while retrycount<max_retry_count:
            try:
                logging.info(url)
                request = Request(url,headers=stdheaders)
                response = urlopen_internal(request,timeout=60)
                if response.info().get('Content-Encoding') == 'gzip':
                    buf = BytesIO(response.read())
                    f = gzip.GzipFile(fileobj=buf)
                    html = f.read().decode('UTF-8')
                else:
                    html = response.read().decode('UTF-8')
                break
            except HTTPError as E:
                lastException = E
                if E.code in (404,410):
                    break
                logging.warning('Error requesting url %s' % url)
                retrycount += 1
                time.sleep(5)
            except:
                lastException = None
                logging.warning('Error requesting url %s' % url)
                retrycount += 1
                time.sleep(5)
        if (html or (isinstance(lastException, HTTPError) and lastException.code==404)) and cached:
            if not os.path.isdir(os.path.dirname(cacheFileName)):
                os.makedirs(os.path.dirname(cacheFileName))
            open(cacheFileName,'w',encoding='UTF-8').write(html)            
    return html


def processDate(date):
    url = 'https://www.punters.com.au/racing-results/%s/' % date.strftime('%Y-%m-%d')
    html = urlopen(url, cached=datetime.datetime.today() - date >= datetime.timedelta(days=1))
    sel = etree.HTML(html)
    for a in sel.xpath('//ul[contains(@class,"jump-to__results-list")][1]/li/a'):
        meetingName = ''.join(a.xpath('./text()')).strip()
        if not re.match('^.+\(.+\)$',meetingName):
            meeting_url = urljoin(url, a.xpath('./@href')[0])
            # Replace "sportsbet-pakenham" with "pakenham" in meeting_url
            if "sportsbet-pakenham" in meeting_url:
                meeting_url = meeting_url.replace("sportsbet-pakenham", "pakenham")
            if "mingenew-yandanooka" in meeting_url:
                meeting_url = meeting_url.replace("mingenew-yandanooka", "mingenew")
            for item in processMeeting(date, meeting_url):
                item['meetingName'] = meetingName
                yield item
            
def processMeeting(date, url):
    try:
        html = urlopen(url, cached=datetime.datetime.today() - date >= datetime.timedelta(days=1))
        sel = etree.HTML(html)
        for a in sel.xpath('//span[@class="results-table__capital results-table__form-guide"]/a[text()="Form Guide"]'):
            for item in processRace(date, urljoin(url, a.xpath('./@href')[0])):
                yield item
    except Exception as e:
        error_message = f'Fail to process meeting for url {url}, skipping\nError: {str(e)}'
        logging.error(error_message)
        error_log_filename = f'error_log_{datetime.datetime.now().strftime("%Y%m%d")}.txt'
        with open(error_log_filename, 'a', encoding='UTF-8') as error_file:
            error_file.write(error_message + '\n')

def processRace(date,url):
    html = urlopen(url,cached=datetime.datetime.today()-date>=datetime.timedelta(days=1))
    sel = etree.HTML(html)
    link = urljoin(url,sel.xpath('//a[@data-analytics-label="spreadsheet"]/@href')[0])
    csvData = urlopen(link,cached=datetime.datetime.today()-date>=datetime.timedelta(days=1))
    reader = csv.reader(StringIO(csvData))
    headers = next(reader)
    gear_changes_text = ''.join(sel.xpath('//div[@class="form-guide-overview__gear-changes"]/text() | //div[@class="form-guide-overview__gear-changes"]/a/text()')).strip()
    # Extract numbers and values using regular expressions
    gear_changes = re.findall(r'(\d+)\.\s+([^\.]+(?:\.[^\d]+)*)', gear_changes_text)
    # logging.info(gear_changes)
    # for part in gear_changes_text.split('. '):
    #     if part:
    #         number, value = part.split(' ', 1)
    #         gear_changes.append((number.strip(), value.strip()))
    #         logging.info(number, value)
            
    for row in reader:
        item = dict(zip(headers,row))
        item['Date'] = datetime.datetime.fromtimestamp(int(''.join(sel.xpath('//div[@class="form-header__time"]/abbr/@data-utime')))).strftime('%Y-%m-%d')
        item['Race'] = ''.join(sel.xpath('//ul[@class="race-nav__wrapper"]/li[contains(@class,"eventActive")]/a/text()')).strip()
        item['rail'] = ''.join(sel.xpath('//span[@class="event-details__track-details" and contains(text(),"RAIL")]/span/text()')).strip()
        # Extract and clean the distance value
        distance = ''.join(sel.xpath('//div[@class="form-header__race-dist"]/span/text()')).strip()
        item['Distance'] = distance.replace('m', '').strip()
        
        # Ensure 'Num' is treated as an integer to match the number from gear_changes
        num_key = 'Num'
        if num_key in item:
            for number, value in gear_changes:
                if item[num_key] == number:
                    item['Gear Changes'] = value.strip()
        item['Race Class Details'] = ''.join(sel.xpath('//div[@class="event-details__handicap"]/text()')).strip()
        yield item
    

def processDates(startDate,endDate):
    date = startDate
    outputFileName = 'data_%s_%s.csv' % (startDate.strftime('%Y%m%d'),endDate.strftime('%Y%m%d'))
    k = 0
    while os.path.exists(outputFileName):
        k += 1
        outputFileName = 'data_%s_%s_v%s.csv' % (startDate.strftime('%Y%m%d'),endDate.strftime('%Y%m%d'),k)
    with open(outputFileName,'w',encoding='UTF-8', newline='') as f:
        writer=csv.writer(f)
        writer.writerow(csvHeaders)
        while date<=endDate:
            logging.info('Processing date %s' % date.strftime('%Y-%m-%d'))
            for result in processDate(date):
                writer.writerow([result.get(header,'') for header in csvHeaders])
                
            date += datetime.timedelta(days=1)
        
        logging.info('Saving output file %s...',outputFileName)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-from', action='store', dest='startDate',help='Start date for period (YYYYMMDD)')
    parser.add_argument('-to', action='store', dest='endDate',help='End date for period (YYYYMMDD)')
    parser.add_argument('-days', type=int, default=1, action='store', dest='days',help='Period from DAYS days in past')
    args = parser.parse_args()
    
    startDate = args.startDate
    endDate = args.endDate
    
    if not startDate:
        startDate = (datetime.datetime.today()-datetime.timedelta(days=args.days)).strftime('%Y%m%d')
    if not endDate:
        endDate = (datetime.datetime.today()).strftime('%Y%m%d')

    startDate = datetime.datetime.strptime(startDate,'%Y%m%d')
    endDate = datetime.datetime.strptime(endDate,'%Y%m%d')
    
    processDates(startDate,endDate)
    logging.info('Done.')