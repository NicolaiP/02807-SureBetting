import sys
import os
import time
from numpy.random import choice
from bet_database import SqlMaker  # noqa: E402
from surebets_calculator import surebet  # noqa: E402
import threading


# Exit dialog if the user hits Ctrl + c
def exitDialog():
    print("Are you sure you don't want to make more money? \nDo you really want to quit the program? Y/N")
    x = input()
    if x.upper() == "Y":
        print("ending")
        database = SqlMaker("SureBetDataBase.sqlite")
        database.updateserverStatus(False, False)
        time.sleep(5)
        database.delete_AllBets()
        database.resetAllBookies()
        database.delete_AllOdds()
        database.delete_AllBuffers()
        database.delete_AllWins()
        sys.exit()
    else:
        pass


def getData(database, endedMatches, money=100):
    # Get all match ids, Odds and player names
    MatchIds, Odds_dict, players = database.getMatches()
    # Get all match ids for matches in buffer
    betMatches = database.getMatchesInBuffer()
    # Filter away all matches that are in buffer or has been placed bets on earlier
    MatchIds = list(set(MatchIds).difference(set(betMatches+endedMatches)))
    print("Investigating " + str(len(MatchIds)) + " new odds\n")

    # Filtering is done inside of surebet function
    for match_id, B1, B2, B3, betsize_odds1, betsize_odds2, betsize_odds3, player1, player2, player3, revenue in surebet(MatchIds, Odds_dict, players, money):
        print('new surebet found! ')
        # remove the money that have been placed on the bet from the booking account
        database.updateBookies(B1, -betsize_odds1)
        database.updateBookies(B2, -betsize_odds2)

        # Calculate probability of each outcome
        prior1 = betsize_odds1/money
        prior2 = betsize_odds2/money

        # Check if there are two or three possible outcomes
        if B3 is not 'NULL':
            # Calculate probability of the outcome
            database.updateBookies(B3, -betsize_odds3)
            # remove the money that have been placed on the bet from the booking account
            prior3 = betsize_odds3/money
            # make sure that the sum of all probabilities is 1
            dif = (1 - prior1 - prior2 - prior3) / 3
            prior1 += dif
            prior2 += dif
            prior3 += dif

        else:
            # make sure that the sum of all probabilities is 1
            dif = (1 - prior1 - prior2) / 2
            prior1 += dif
            prior2 += dif
            prior3 = 'NULL'

        # Add data to AllWins
        database.addWin(match_id, B1, B2, B3, betsize_odds1, betsize_odds2, betsize_odds3, player1, player2, player3, revenue, time.time())
        # Add data to AllBuffers
        database.addAllBuffers(match_id, B1, B2, B3, revenue, prior1, prior2, prior3, time.time())


def updateBalances(database):
    # Get all data from AllBuffers
    betMatches = database.getBuffer()
    _endedMatches = []
    # Iterate through each row
    for match in betMatches:
        if True:  # matchEnded():
            # Get name of bookmakers
            B1 = match[1]
            B2 = match[2]
            # Test if there are two or three possible outcomes
            try:
                # Get name of bookmaker
                B3 = match[3]
                # Simulate winner using the probabilities of each outcome
                winningBookie = choice([B1, B2, B3], p=[match[5], match[6], match[7]])
            except ValueError:
                # Simulate winner using the probabilities of each outcome
                winningBookie = choice([B1, B2], p=[match[5], match[6]])

            # Add revenue to the winning bookmaker's account
            winnings = match[4]
            database.updateBookies(winningBookie, winnings)

            # Remove match from buffer
            _endedMatches.append(match[0])
            database.deleteBuffer(match[0])
        else:
            pass
    return _endedMatches


def startCrawler():
    bashCommand = 'run.sh crawler.py'
    os.system(bashCommand)


if __name__ == '__main__':

    #############
    # __Setup__ #
    #############

    # Initializing database
    print("\nInitializing database")
    database = SqlMaker('SureBetDataBase.sqlite')
    database.create_tables()

    # Initializing webcrawlers
    print("\nInitializing webcrawler")
    database.updateserverStatus(True, True)
    crawlerThread = threading.Thread(target=startCrawler, daemon=True)
    crawlerThread.start()

    # Sets the interval with which the program scrapes for new odds
    getDataTimer = 0
    getDataInterval = 10  # Seconds

    # Sets the time of program initialization
    initTime = time.time() + getDataInterval

    print("\nOdds parser interval is set to: " + str(getDataInterval) + " seconds")

    # Sets the interval with which the program updates the balances at each bookmaker
    updateBalancesInterval = getDataInterval*2  # Seconds
    updateBalancesTimer = updateBalancesInterval
    print("\nBookmaker balance updater interval is set to: " + str(updateBalancesInterval) + " seconds")

    endedMatches = []
    used_bookies = []
    revenue = 0
    revenue_list = [0]

    ############
    # __Loop__ #
    ############

    print("\n\n -- Starting loop --\n\n")
    while True:
        try:
            runtime = time.time() - initTime
            if runtime > getDataTimer:

                getDataTimer += getDataInterval
                print("Getting data")
                print("Script has been running for " + str(round(runtime)) + " seconds")

                # Remove dupliates
                endedMatches = list(set(endedMatches))

                print("setting status")
                database.updateserverStatus(True, True)

                # Find surebets and add them to database
                getData(database, endedMatches)
                print()
                # Find match ids that already have been placed bets on
                betMatches = database.getBuffer()
                endedMatches += [ii[0] for ii in betMatches]

            if runtime > updateBalancesTimer:
                # Run every updateBalancesInterval
                updateBalancesTimer += updateBalancesInterval
                print("Updating balances at bookmakers\n")
                # Simulate a winner and update balance in bookmaker accounts
                newlyEndedMatches = updateBalances(database)
                print("Current balance across " + str(len(database.getBookies())) + " Bookmakers is: " + str(
                    database.getTotalBalance()))
                print("Removing " + str(len(newlyEndedMatches)))
                # Adds the matches that were in the buffer to endedMatches
                endedMatches += newlyEndedMatches

        # End program on keyboardInterrupt
        except KeyboardInterrupt:
            exitDialog()
