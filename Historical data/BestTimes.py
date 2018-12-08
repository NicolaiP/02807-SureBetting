from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np
from mr3px.csvprotocol import CsvProtocol


class MRBestTime(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                reducer=self.reducer_round30),
            MRStep(reducer=self.reducer)
            ]
    def mapper(self, _, line):
        datarow = line.replace(' ','').replace('N/A','').split(',')
        O1 = datarow[13]
        O2 = datarow[14]
        O3 = datarow[15]
        try:
            O1 = np.float(O1)
            O2 = np.float(O2)
            O3 = np.float(O3)
            time = datarow[4]
        except ValueError:
            print("")
        else:
            payout = O1*O2*O3/(O1*O2+O1*O3+O2*O3)
            yield (time, (payout>1))

    # This rounds the time to the closest 30 minutes
    def reducer_round30(self, key, values):
        hours=int(key.split(":")[0])
        minutes=int(key.split(":")[1])
        if abs(minutes - 30) < 15:
            minutes = 30
        elif minutes // 30 == 1:
            hours += 1
            minutes = 0
            if hours == 24:
                hours = 0
        else: 
            minutes = 0
        newtime = format(hours, "02")+":"+format(minutes, "02")
        yield (newtime, sum(values))

    def reducer(self, key, values):
        yield (key, sum(values))


if __name__ == '__main__':
    MRBestTime.run()

