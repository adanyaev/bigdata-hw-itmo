# %%file is an Ipython magic function that saves the code cell as a file

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTask1(MRJob):


    def mapper(self, _, line):
        # yield each word in the line
        name = line.split('" "')[1]
        yield (name.strip(), 1)

    def combiner(self, name, counts):
        # optimization: sum the words we've seen so far
        yield (name, sum(counts))

    def reducer_count_replicas(self, name, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield None, (sum(counts), name)

    def reducer_sort_num_replicas(self, _, num_replicas_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        sorted_pairs = sorted(num_replicas_pairs, key=lambda x: x[0], reverse=True)[:20]
        yield None, sorted_pairs
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer_count_replicas),
            MRStep(reducer=self.reducer_sort_num_replicas)
        ]

if __name__ == '__main__':
    MRTask1.run()
