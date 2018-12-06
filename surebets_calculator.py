
def surebet(MatchIds, Odds_dict, players, money=100):
    for match_id in MatchIds:
        # if NULL is not in any odds, it is a three outcome bet
        if 'NULL' not in Odds_dict[match_id]:
            # defines bookmaker names and odds
            B1 = Odds_dict[match_id][0][1]
            O1 = Odds_dict[match_id][0][0]

            B2 = Odds_dict[match_id][1][1]
            O2 = Odds_dict[match_id][1][0]

            B3 = Odds_dict[match_id][2][1]
            O3 = Odds_dict[match_id][2][0]
            # Calculates payout
            payout = O1*O2*O3/(O1*O2+O1*O3+O2*O3)
            # If payout is bigger than 1 it is a surebet, if it's bigger than 2 it's most likely a miscalculation or a scam
            if 2 > payout > 1:
                # Define the player/team names
                player1 = players[match_id][0]
                player2 = players[match_id][1]
                player3 = players[match_id][2]
                # What to bet on each odds for optimal revenue
                betsize_odds1 = money * payout/O1
                betsize_odds2 = money * payout/O2
                betsize_odds3 = money * payout/O3
                # Revenue
                revenue = money*payout

                yield match_id, B1, B2, B3, betsize_odds1, betsize_odds2, betsize_odds3, player1, player2, player3, revenue
        # if there are no NULL in the two first odds, it is a two possible bet
        elif 'NULL' not in Odds_dict[match_id][0:2]:
            # defines bookmaker names and odds
            B1 = Odds_dict[match_id][0][1]
            O1 = Odds_dict[match_id][0][0]

            B2 = Odds_dict[match_id][1][1]
            O2 = Odds_dict[match_id][1][0]
            # Calculates payout
            payout = O1*O2/(O1+O2)
            # If payout is bigger than 1 it is a surebet, if it's bigger than 2 it's most likely a miscalculation or a scam
            if 2 > payout > 1:
                # Define the player/team names
                player1 = players[match_id][0]
                player2 = players[match_id][1]
                player3 = "NULL"
                # What to bet on each odds for optimal revenue
                betsize_odds1 = money * payout/O1
                betsize_odds2 = money - betsize_odds1
                # Revenue
                revenue = money*payout

                yield match_id, B1, B2, 'NULL', betsize_odds1, betsize_odds2, 'NULL', player1, player2, player3, revenue
