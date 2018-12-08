from mrjob.job import MRJob
import numpy as np


class MRBestLeague(MRJob):

    def mapper(self, _, line):
        datarow = line.replace(' ','').replace('N/A','').split(',')
        O1 = datarow[13]
        O2 = datarow[14]
        O3 = datarow[15]
        try:
            O1 = np.float(O1)
            O2 = np.float(O2)
            O3 = np.float(O3)
            league = datarow[0]
        except ValueError:
            print("")
        else:
            payout = O1*O2*O3/(O1*O2+O1*O3+O2*O3)

            yield (league, (payout>1))

    def reducer(self, key, values):
        yield (key, sum(values))


if __name__ == '__main__':
    MRBestLeague.run()

