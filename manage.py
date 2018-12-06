# This script is build to manage the sql database
import sys
from bet_database import SqlMaker  # noqa: E402


# This function deletes all databases except AllWins and AllBookies. The saldos in AllBookies are all set to 1000
def deleteTables():
    database = SqlMaker("SureBetDataBase.sqlite")
    database.delete_AllBets()
    database.resetAllBookies()
    database.delete_AllOdds()
    database.delete_AllBuffers()


argList = ["createTables", "deleteTables", "deleteAllWins", "printOdds", "printBookies",
           "printBets", "printWins", "printBuffers", "resetAllBookies", "balance", "serverStatus"]

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print()
        print("manage.py needs one argument")
        print("Possible arguments are:")
        print()
        for arg in argList:
            print(arg)
    else:
        argIn = sys.argv[1]
        if argIn not in argList:
            print()
            print("The argument was invalid")
            print("Possible arguments are:")
            print()
            for arg in argList:
                print(arg)
            sys.exit()
        # Call deleteTables()
        elif argIn == "createTables":
            database = SqlMaker("SureBetDataBase.sqlite")
            database.create_tables()
        elif argIn == "deleteTables":
            deleteTables()
        # Delete AllWins
        elif argIn == "deleteAllWins":
            SqlMaker("SureBetDataBase.sqlite").delete_AllWins()
        # Prints AllBookies
        elif argIn == "printBookies":
            database = SqlMaker("SureBetDataBase.sqlite")
            database.printAllBookiesAsCsv()
        # Print AllOdds
        elif argIn == "printOdds":
            database = SqlMaker("SureBetDataBase.sqlite")
            database.printAllOddsAsCsv()
        # Print AllBuffers
        elif argIn == "printBuffers":
            database = SqlMaker("SureBetDataBase.sqlite")
            database.printAllBuffersAsCsv()
        # Print AllBets
        elif argIn == "printBets":
            database = SqlMaker("SureBetDataBase.sqlite")
            database.printAllBetsAsCsv()
        # Print AllWins
        elif argIn == "printWins":
            database = SqlMaker("SureBetDataBase.sqlite")
            database.printAllWinsAsCsv()
        # Print AllBookies
        elif argIn == "resetAllBookies":
            database = SqlMaker("SureBetDataBase.sqlite")
            database.resetAllBookies()
        # Print the total money in all of the bookie accounts
        elif argIn == "balance":
            database = SqlMaker("SureBetDataBase.sqlite")
            database.printTotalBalance()
        elif argIn == "serverStatus":
            database = SqlMaker("SureBetDataBase.sqlite")
            print(database.getserverStatus())

