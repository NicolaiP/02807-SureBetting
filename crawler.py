from pyspark import SparkContext, SparkConf, StorageLevel
import urllib3
import re
from bet_database import SqlMaker
import time
# from surebets_calculator import surebet


def loadPage(url):
        # Fetching all content from url
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        url_pool = urllib3.PoolManager()
        page_response = url_pool.request('GET', url)

        # Using the html parser to store it
        # page_content = BeautifulSoup(page_response.data, "html.parser")

        try:
            return page_response.data.split(b'class="TheList Collapsable NextMatchesList "', 1)[1].split(b'class="MatchNextInfo"')[:-1]
        except Exception as e:
            print(e)


def temploadPage(url):
        # Fetching all content from url
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        url_pool = urllib3.PoolManager()
        page_response = url_pool.request('GET', url)

        # Using the html parser to store it
        # page_content = BeautifulSoup(page_response.data, "html.parser")
        temp = []
        r = page_response.data.split(b'class="TheList Collapsable NextMatchesList "', 1)[1].split(b'class="MatchNextInfo"')[:-1]
        for i in r:
            temp.append(len(i))
        return temp


def getMatchInfo(match):
    lines = match.split(b'\n')

    matchid = ""
    names = []
    bet1 = ""
    bet2 = ""
    bet3 = ""
    bookie1 = ""
    bookie2 = ""
    bookie3 = ""

    for id, line in enumerate(lines):
        if b'\t\t\t<span itemprop="summary" class="MDxEventName">' in line:
            names = line.decode('UTF-8')[49:-8].split(' - ')
        elif b'" data-matchId=' in line:
            matchid = re.search('data-vocabulary.org/Event" data-matchId="(.*)">',
                                line.decode('UTF-8')).group(1)
        elif b'"Outcome Outcome1"' in line:
            bookie1 = re.search('<span class="BM OTBookie">(.*)</span></span>',
                                lines[id+1].decode('UTF-8')).group(1)
            bet1 = re.search('<span class="Odds">(.*)</span><span class="OutcomeName">',
                             lines[id+1].decode('UTF-8')).group(1)
        elif b'"Outcome Outcome1"' in line:
            bookie2 = re.search('<span class="BM OTBookie">(.*)</span></span>',
                                lines[id+1].decode('UTF-8')).group(1)
            bet2 = re.search('<span class="Odds">(.*)</span><span class="OutcomeName">',
                             lines[id+1].decode('UTF-8')).group(1)
        elif b'"Outcome Outcome3"' in line:
            bookie3 = re.search('<span class="BM OTBookie">(.*)</span></span>',
                                lines[id+1].decode('UTF-8')).group(1)
            bet3 = re.search('<span class="Odds">(.*)</span><span class="OutcomeName">',
                             lines[id+1].decode('UTF-8')).group(1)

    return {'matchid': matchid, 'names': names, 'odds': [bet1, bet2, bet3], 'bookies': [bookie1, bookie2, bookie3]}


if __name__ == "__main__":

    #################
    # Crawler setup #
    #################

    conf = SparkConf().setAppName("crawler").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    database = SqlMaker("SureBetDataBase.sqlite")
    database.create_tables()

    # url_pool = urllib3.PoolManager()

    url_volley = 'https://www.betbrain.dk/live-center/volleyball/'
    url_tennis = 'https://www.betbrain.dk/live-center/tennis/'
    url_football = 'https://www.betbrain.dk/live-center/football/'
    url_basket = 'https://www.betbrain.dk/live-center/basketball/'
    url_ice = 'https://www.betbrain.dk/live-center/ice-hockey/'
    url_badminton = 'https://www.betbrain.dk/live-center/badminton/'
    url_handball = 'https://www.betbrain.dk/live-center/handball/'
    url_table_tennis = 'https://www.betbrain.dk/live-center/table-tennis/'
    url_cricket = 'https://www.betbrain.dk/live-center/cricket/'

    # urlsRDD = sc.parallelize([url_football])
    urlsRDD = sc.parallelize([url_football, url_tennis, url_volley, url_basket, url_ice, url_badminton,
                              url_handball, url_table_tennis, url_cricket])
    urlsRDD.persist(StorageLevel.MEMORY_ONLY)

    ################
    # Crawler loop #
    ################
    a = True
    while a:
        # a = False
        serverStatus = database.getserverStatus()[0]
        if not serverStatus[0]:
            print("Terminating crawler")
            break
        if serverStatus[1]:
            # try:
                print("Attemts scrape")

                # responseRDD = urlsRDD.map(temploadPage)
                # responses = responseRDD.collect()
                # print(responses)

                responseRDD = urlsRDD.map(loadPage).filter(lambda r: isinstance(r, (list, )))
                responses = responseRDD.collect()
                # print(responses)

                responseRDD = sc.parallelize([match for sublist in responses for match in sublist])
                # print(responseRDD.collect()[-3][:5000])

                # responseRDD = sc.parallelize(loadPage(url))

                matchesRDD = responseRDD.filter(lambda response: len(response) < 6000)

                matchinfo = matchesRDD.map(getMatchInfo)

                for info in matchinfo.collect():
                    if info['odds'][1] != "":
                        oddsId3 = info['matchid']+"-3-"+info['names'][1]
                        database.addAllOdds(oddsId3, info['names'][1], float(info['odds'][2].replace(',', '.')), info['bookies'][2])
                        database.addAllBookies(info['bookies'][2])
                        oddsId2 = info['matchid']+"-2-Draw"
                        database.addAllOdds(oddsId2, "Draw", float(info['odds'][1].replace(',', '.')), info['bookies'][1])
                        database.addAllBookies(info['bookies'][1])
                        oddsId1 = info['matchid']+"-1-"+info['names'][0]
                        database.addAllOdds(oddsId1, info['names'][0], float(info['odds'][0].replace(',', '.')), info['bookies'][0])
                        database.addAllBookies(info['bookies'][0])
                    if info['odds'][0] != "" and info['odds'][2] != "":
                        oddsId2 = info['matchid']+"-2-"+info['names'][1]
                        database.addAllOdds(oddsId2, info['names'][1], float(info['odds'][2].replace(',', '.')), info['bookies'][2])
                        database.addAllBookies(info['bookies'][2])
                        oddsId1 = info['matchid']+"-1-"+info['names'][0]
                        database.addAllOdds(oddsId1, info['names'][0], float(info['odds'][0].replace(',', '.')), info['bookies'][0])
                        database.addAllBookies(info['bookies'][0])
                        oddsId3 = "NULL"
                    else:
                        oddsId2 = "NULL"
                        oddsId3 = "NULL"
                        oddsId1 = "NULL"
                    database.addAllBets(int(info['matchid']), oddsId1, oddsId2, oddsId3)
                database.updateserverStatus(True, False)
            # except Exception as e:  # noqa
            #     print(e)
        else:
            time.sleep(5)
