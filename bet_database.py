from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import sqlite3


class SqlMaker:
    # Initialize the regestry of all the bets. The name of the database file used for
    # this instance is given as a parameter. This database will in some
    # cases be pre populated, and in some cases empty.
    def __init__(self, databaseName):
        self.db = sqlite3.connect(databaseName)
        self.c = self.db.cursor()

    # Clean up the database.
    def close(self):
        self.db.close()

    # Delete AllBets
    def delete_AllBets(self):
        self.c.execute("DROP TABLE IF EXISTS AllBets")
        print("AllBets deleted")

    # Delete AllOdds
    def delete_AllOdds(self):
        self.c.execute("DROP TABLE IF EXISTS AllOdds")
        print("AllOdds deleted")

    # Delete AllBookies
    def delete_AllBookies(self):
        self.c.execute("DROP TABLE IF EXISTS AllBookies")
        print("AllBookies deleted")

    # Delete AllBookies
    def delete_AllWins(self):
        self.c.execute("DROP TABLE IF EXISTS AllWins")
        print("AllWins deleted")

    # Delete AllBuffers
    def delete_AllBuffers(self):
        self.c.execute("DROP TABLE IF EXISTS AllBuffers")
        print("AllBuffers deleted")

    # This function creates the five tables AllBets, AllOdds, AllBookies, AllBuffers and AllWins.
    # AllBets contain the match Id, all of the odds ids and whether the game is active or not
    # AllOdds contain the odds Id, the player's name, the odds of the bet and the name of the bookmaker.
    # AllBookies contain the bookmaker's name and the total balance in the bookmaker account
    # AllBuffers is used to save the information of placed bets in matches that are still active. It contains, the matchId, the bookmakernames, the revenue of the bet, every outcome probability and the time the bet was placed
    # AllWins is used to save the information of all old placed bets. It contains, the matchId, the bookmakernames, the betsizes, the names of the players/teams and the revenue of the bet and the time the bet was placed
    def create_tables(self):
        self.c.execute(
            "CREATE table IF NOT EXISTS AllBets(matchId INTEGER PRIMARY KEY, oddsId1 TEXT, oddsId2 TEXT, oddsId3 TEXT, gameActive INTEGER)")
        self.c.execute(
            "CREATE table IF NOT EXISTS AllOdds(oddsId TEXT PRIMARY KEY, playerName TEXT, maxOdds REAL, bookmakerName TEXT)")
        self.c.execute(
            "CREATE table IF NOT EXISTS AllBookies(bookmakerName TEXT PRIMARY KEY, saldo REAL)")
        self.c.execute(
            "CREATE table IF NOT EXISTS AllBuffers(matchId INTEGER PRIMARY KEY, bookmakerName1 TEXT, bookmakerName2 TEXT, bookmakerName3 TEXT, revenue REAL, prior1 REAL, prior2 REAL, prior3 REAL, bettime REAL)")
        self.c.execute(
            "CREATE table IF NOT EXISTS AllWins(matchId INTEGER PRIMARY KEY, bookmakerName1 TEXT, bookmakerName2 TEXT, bookmakerName3 TEXT, betsize1 REAL, betsize2 REAL, betsize3 REAL, playerName1 TEXT, playerName2 TEXT, playerName3 TEXT, revenue REAL, bettime REAL)")
        self.c.execute(
            "CREATE table IF NOT EXISTS serverStatus(running BOOL, crawlInterval BOOL)")

    # Add a row to the AllBets table.
    def addAllBets(self, matchId, oddsId1, oddsId2, oddsId3, gameActive=1):
        if len(self.c.execute("SELECT oddsId1 FROM AllBets WHERE matchId=?", (matchId,)).fetchall()) > 0:
            self.c.execute("UPDATE AllBets SET oddsId1=?, oddsId2=?, oddsId3=?, gameActive = ? WHERE matchId=?", (oddsId1, oddsId2, oddsId3, gameActive, matchId))
        else:
            self.c.execute("INSERT INTO AllBets VALUES (?, ?, ?, ?, ?)", (matchId, oddsId1, oddsId2, oddsId3, gameActive))
        self.db.commit()

    # Add a row to the AllOdds table.
    def addAllOdds(self, oddsId, playerName, maxOdds, bookmakerName):
        if len(self.c.execute("SELECT maxOdds FROM AllOdds WHERE oddsId=?", (oddsId,)).fetchall()) > 0:
            self.c.execute("UPDATE AllOdds SET playerName=?, maxOdds=?, bookmakerName=? WHERE oddsId=?", (playerName, maxOdds, bookmakerName, oddsId))
        else:
            self.c.execute("INSERT INTO AllOdds VALUES (?, ?, ?, ?)", (oddsId, playerName, maxOdds, bookmakerName))
        self.db.commit()

    # Add a row to the AllBookies table.
    def addAllBookies(self, bookmakerName, saldo=1000):
        if len(self.c.execute("SELECT saldo FROM AllBookies WHERE bookmakerName=?", (bookmakerName,)).fetchall()) == 0:
            self.c.execute("INSERT INTO AllBookies VALUES (?, ?)", (bookmakerName, saldo))
        self.db.commit()

    # Add a row to the AllBuffers table.
    def addAllBuffers(self, matchId, bookmakerName1, bookmakerName2, bookmakerName3, revenue, prior1, prior2, prior3, bettime):
        if len(self.c.execute("SELECT revenue FROM AllBuffers WHERE matchId=?", (matchId,)).fetchall()) > 0:
            self.c.execute("UPDATE AllBuffers SET bookmakerName1=?, bookmakerName2=?, bookmakerName3=?, revenue=?, prior1=?, prior2=?, prior3=?, bettime=? WHERE matchId=?", (bookmakerName1, bookmakerName2, bookmakerName3, revenue, prior1, prior2, prior3, bettime, matchId))
        else:
            self.c.execute("INSERT INTO AllBuffers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (matchId, bookmakerName1, bookmakerName2, bookmakerName3, revenue, prior1, prior2, prior3, bettime))
        self.db.commit()

    # Add a row to the AllWins table.
    def addWin(self, matchId, bookmakerName1, bookmakerName2, bookmakerName3, betsize1, betsize2, betsize3, playerName1, playerName2, playerName3, revenue, bettime):
        if len(self.c.execute("SELECT revenue FROM AllWins WHERE matchId=?", (matchId,)).fetchall()) > 0:
            self.c.execute("UPDATE AllWins SET bookmakerName1=?, bookmakerName2=?, bookmakerName3=?, betsize1=?, betsize2=?, betsize3=?, playerName1=?, playerName2=?, playerName3=?, revenue=?, bettime=? WHERE matchId=?", (bookmakerName1, bookmakerName2, bookmakerName3, betsize1, betsize2, betsize3, playerName1, playerName2, playerName3, revenue, bettime, matchId))
        else:
            self.c.execute("INSERT INTO AllWins VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (matchId, bookmakerName1, bookmakerName2, bookmakerName3, betsize1, betsize2, betsize3, playerName1, playerName2, playerName3, revenue, bettime))
        self.db.commit()

    # Add or remove money from a bookmaker account
    def updateBookies(self, bookmakerName, transfer):
        newsaldo = self.getBalance(bookmakerName)+transfer
        self.c.execute("UPDATE AllBookies SET saldo=? WHERE bookmakerName=?", (newsaldo, bookmakerName))
        self.db.commit()

    def updateserverStatus(self, running, crawlInterval):
        if len(self.c.execute("SELECT * FROM serverStatus").fetchall()) > 0:
            self.c.execute("UPDATE serverStatus SET running=?, crawlInterval=?", (running, crawlInterval,))
        else:
            self.c.execute("INSERT INTO serverStatus VALUES (?, ?)", (running, crawlInterval,))
        self.db.commit()

    # Reset balance in bookmaker account to 1000 or another defined saldo
    def resetAllBookies(self, saldo=1000):
        self.c.execute("UPDATE AllBookies SET saldo=?", (saldo,))
        self.db.commit()

    # Delete Allbuffers
    def deleteBuffer(self, matchId):
        self.c.execute("DELETE from AllBuffers WHERE matchId==?", (matchId,))
        self.db.commit()

    # Outputs a list with all the match Ids, a dictionary with all the odds and a dictionary with all the player/team names.
    # The keys to the dictionaries are the match Ids
    def getMatches(self):
        odds = {}
        matchIds = []
        players = {}
        for match in self.c.execute("SELECT matchId, oddsId1, oddsId2, oddsId3 from AllBets").fetchall():
            # append matchId
            matchIds.append(match[0])
            # print(match)
            for oddsId in match[1:]:
                if oddsId != 'NULL':
                    odd = self.c.execute("SELECT maxOdds, bookmakerName from AllOdds WHERE oddsId=?", (oddsId,)).fetchall()
                    player = self.c.execute("SELECT playerName from AllOdds WHERE oddsId=?", (oddsId,)).fetchall()
                    try:
                        if len(odd) > 0:
                            odds[match[0]].append(odd[0])
                            players[match[0]].append(player[0][0])
                    except:  # noqa: E722
                        if len(odd) > 0:
                            odds[match[0]] = [odd[0]]
                            players[match[0]] = [player[0][0]]
                else:
                    odd = 'NULL'
                    player = 'NULL'
                    try:
                        if len(odd) > 0:
                            odds[match[0]].append(odd)
                            players[match[0]].append(player)
                    except:  # noqa: E722
                        if len(odd) > 0:
                            odds[match[0]] = [odd]
                            players[match[0]] = [player]

        return matchIds, odds, players

    # Takes name of bookmaker as input and returns balance
    def getBalance(self, bookmakerName):
        saldo = self.c.execute("SELECT saldo FROM AllBookies where bookmakerName=?", (bookmakerName,)).fetchall()
        return saldo[0][0]

    # Returns total balance of all the bookmaker accounts
    def getTotalBalance(self):
        return self.c.execute("SELECT SUM(saldo) from AllBookies").fetchone()[0]

    # Prints the total balance in all the bookmaker accounts
    def printTotalBalance(self):
        print(self.getTotalBalance())

    # Returns all the match ids in AllBuffers
    def getMatchesInBuffer(self):
        matches = []
        for match in self.c.execute("SELECT matchId from AllBuffers").fetchall():
            matches.append(match[0])
        return matches

    # Returns all the content in AllBuffers
    def getBuffer(self):
        return self.c.execute("SELECT * from AllBuffers").fetchall()

    # Returns all the content in AllBuffers
    def getBookies(self):
        return self.c.execute("SELECT * from AllBookies").fetchall()

    # Returns all the content in AllBuffers
    def getAllWins(self):
        return self.c.execute("SELECT * from AllWins").fetchall()

    # Returns all the content in AllBuffers
    def getserverStatus(self):
        self.create_tables()
        status = self.c.execute("SELECT * from serverStatus").fetchall()
        if len(status) != 1:
            self.c.execute("DROP TABLE IF EXISTS severStatus")
            self.c.execute("CREATE table IF NOT EXISTS serverStatus(running BOOL, crawlInterval BOOL)")
            self.c.execute("INSERT INTO serverStatus VALUES (?, ?)", (False, False))
            self.db.commit()
            status = self.c.execute("SELECT * from serverStatus").fetchall()
        return status

    # Prints AllBets
    def printAllBetsAsCsv(self):
        self.printAsCsv('AllBets')

    # Prints AllOdds
    def printAllOddsAsCsv(self):
        self.printAsCsv('AllOdds')

    # Prints AllWins
    def printAllWinsAsCsv(self):
        self.printAsCsv('AllWins')

    # Prints AllBookies
    def printAllBookiesAsCsv(self):
        self.printAsCsv('AllBookies')

    # Prints AllBuffers
    def printAllBuffersAsCsv(self):
        self.printAsCsv('AllBuffers')

    # Prints as CSV
    def printAsCsv(self, table_name):
        statement = "SELECT * from {0}".format(table_name)
        for row in self.c.execute(statement).fetchall():
            row = [str(ii) for ii in row]
            print(','.join(row))
