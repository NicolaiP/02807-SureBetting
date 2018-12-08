from mrjob.job import MRJob
import numpy as np


class MRBestTeams(MRJob):

    def mapper(self, _, line):
        datarow = line.replace(' ','').replace('N/A','').split(',')
        O1 = datarow[13]
        O2 = datarow[14]
        O3 = datarow[15]
        try:
            O1 = np.float(O1)
            O2 = np.float(O2)
            O3 = np.float(O3)
            home = datarow[5]
            away = datarow[6]
        except ValueError:
            print("")
        else:
            payout = O1*O2*O3/(O1*O2+O1*O3+O2*O3)
            yield (home, (payout>1))
            yield (away, (payout>1))

    def reducer(self, key, values):
        yield (key, sum(values))


if __name__ == '__main__':
    MRBestTeams.run()

