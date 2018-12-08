import os
import csv
import matplotlib.pyplot as plt


try:
    os.system("py BestTimes.py <alldata.csv> Times.csv")
except PermissionError:
    print("")
try:
    os.system("py BestLeague.py <alldata.csv> Leagues.csv")
except PermissionError:
    print("")
try:
    os.system("py BestTeams.py <alldata.csv> Teams.csv")
except PermissionError:
    print("")

times = []
number_of_surbets = []
with open('Times.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter='\t')
    for row in readCSV:
        if len(row)>1:
            times.append(row[0])
            number_of_surbets.append(float(row[1]))
fig = plt.figure()
# plot
plt.plot(times, number_of_surbets)
plt.grid(True)
# beautify the x-labels
plt.gcf().autofmt_xdate()
plt.ylabel('Number of surebets')
plt.xlabel('Time')
plt.show()

leagues={}
with open('Leagues.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter='\t')
    for row in readCSV:
        if len(row)>1:
            leagues[row[0]] = float(row[1])
            
number_of_surbets = sorted(leagues.values(), reverse = True)
leagues = sorted(leagues.keys(), key=leagues.__getitem__, reverse=True)
num_leagues = range(len(number_of_surbets))
# plot
plt.bar(num_leagues, number_of_surbets)
plt.xticks(num_leagues, leagues, rotation=20)  
   # add some space between bars and axes 
plt.xlim([min(num_leagues) - 0.3, max(num_leagues) + 1]) # x axis
plt.ylim([0, max(number_of_surbets) + 3])                   # y axis starts at 0
   # let's add a grid on y-axis
plt.grid(True, axis='y')
plt.ylabel('Number of surebets')
plt.xlabel('League')
plt.show()   
plt.savefig('plots/league_surebets.png')

teams={}
with open('Teams.csv') as csvfile:
    readCSV = csv.reader(csvfile, delimiter='\t')
    for row in readCSV:
        if len(row)>1:
            teams[row[0]] = float(row[1])

print(len(teams))     
number_of_surbets = sorted(teams.values(), reverse=True)[0:30]
teams = sorted(teams.keys(), key=teams.__getitem__, reverse=True)[0:30]
num_teams = range(30)
# plot
plt.barh(num_teams, number_of_surbets)
plt.yticks(num_teams, teams, rotation=20)  
   # add some space between bars and axes 
plt.ylim([min(num_teams) - 0.3, max(num_teams) + 1]) # x axis
plt.xlim([0, max(number_of_surbets) + 3])                   # y axis starts at 0
   # let's add a grid on y-axis
plt.grid(True, axis='x')
plt.xlabel('Number of surebets')
plt.ylabel('Teams')
plt.title('Top 30 teams')
plt.show()   
plt.savefig('plots/team_surebets.png')

