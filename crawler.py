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

        # Strips the HTML and returns only part consisting match data
        try:
            return page_response.data.split(b'class="TheList Collapsable NextMatchesList "', 1)[1].split(b'class="MatchNextInfo"')[:-1]
        except Exception as e:
            print(e)


def getMatchInfo(match):
    # Splits at newline
    lines = match.split(b'\n')

    # Creates output variables
    matchid = ""
    names = []
    bet1 = ""
    bet2 = ""
    bet3 = ""
    bookie1 = ""
    bookie2 = ""
    bookie3 = ""

    # Runs a loop parsing each line for relevant data
    for id, line in enumerate(lines):
        if b'\t\t\t<span itemprop="summary" class="MDxEventName">' in line:
            # Gets the names of each player/team
            names = line.decode('UTF-8')[49:-8].split(' - ')
        elif b'" data-matchId=' in line:
            # Gets the unique match ID
            matchid = re.search('data-vocabulary.org/Event" data-matchId="(.*)">',
                                line.decode('UTF-8')).group(1)
        elif b'"Outcome Outcome1"' in line:
            # Gets the bookie name for outcome 1
            bookie1 = re.search('<span class="BM OTBookie">(.*)</span></span>',
                                lines[id+1].decode('UTF-8')).group(1)
            # Gets the odds value for outcome 1
            bet1 = re.search('<span class="Odds">(.*)</span><span class="OutcomeName">',
                             lines[id+1].decode('UTF-8')).group(1)
        elif b'"Outcome Outcome1"' in line:
            # Gets the bookie name for outcome 2
            bookie2 = re.search('<span class="BM OTBookie">(.*)</span></span>',
                                lines[id+1].decode('UTF-8')).group(1)
            # Gets the odds value for outcome 2
            bet2 = re.search('<span class="Odds">(.*)</span><span class="OutcomeName">',
                             lines[id+1].decode('UTF-8')).group(1)
        elif b'"Outcome Outcome3"' in line:
            # Gets the bookie name for outcome 3
            bookie3 = re.search('<span class="BM OTBookie">(.*)</span></span>',
                                lines[id+1].decode('UTF-8')).group(1)
            # Gets the odds value for outcome 3
            bet3 = re.search('<span class="Odds">(.*)</span><span class="OutcomeName">',
                             lines[id+1].decode('UTF-8')).group(1)

    # Returns the parsed data
    return {'matchid': matchid, 'names': names, 'odds': [bet1, bet2, bet3], 'bookies': [bookie1, bookie2, bookie3]}


if __name__ == "__main__":

    #################
    # Crawler setup #
    #################

    # Sets up Spark context
    conf = SparkConf().setAppName("crawler").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    # loads database
    database = SqlMaker("SureBetDataBase.sqlite")
    database.create_tables()

    # Defines urls
    url_volley = 'https://www.betbrain.dk/live-center/volleyball/'
    url_tennis = 'https://www.betbrain.dk/live-center/tennis/'
    url_football = 'https://www.betbrain.dk/live-center/football/'
    url_basket = 'https://www.betbrain.dk/live-center/basketball/'
    url_ice = 'https://www.betbrain.dk/live-center/ice-hockey/'
    url_badminton = 'https://www.betbrain.dk/live-center/badminton/'
    url_handball = 'https://www.betbrain.dk/live-center/handball/'
    url_table_tennis = 'https://www.betbrain.dk/live-center/table-tennis/'
    url_cricket = 'https://www.betbrain.dk/live-center/cricket/'

    # Puts the urls into the spark context
    urlsRDD = sc.parallelize([url_football, url_tennis, url_volley, url_basket, url_ice, url_badminton,
                              url_handball, url_table_tennis, url_cricket])
    urlsRDD.persist(StorageLevel.MEMORY_ONLY)

    ################
    # Crawler loop #
    ################

    while True:
        # Reads the status [0] = is app running, [1] = has x seconds passed (crawlerInterval)
        serverStatus = database.getserverStatus()[0]
        if not serverStatus[0]:
            # Terminates crawler if app has stopped
            print("Terminating crawler")
            break
        if serverStatus[1]:
            try:
                print("Attemts scrape")

                # Gets HTML for each url
                responseRDD = urlsRDD.map(loadPage).filter(lambda r: isinstance(r, (list, )))
                
                # Collects all matches in one list - [[...], [...], ...] -> [...]
                responses = responseRDD.collect()
                matchesRDD = sc.parallelize([match for sublist in responses for match in sublist])

                # Parses HTML
                matchinfo = matchesRDD.map(getMatchInfo)

                # Stores parsed data in in databases
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
            except Exception as e:
                print(e)
        else:
            time.sleep(5)
