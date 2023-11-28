# %%file is an Ipython magic function that saves the code cell as a file

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTask2(MRJob):


    def mapper(self, _, line):
        spl = line.split('" "')
        name = spl[1].strip('"\n ')
        if name != 'dialogue':
            phrase = spl[2].strip('"\n ')  
            yield (name, phrase)

    def combiner(self, name, phrases):
        p = max(phrases, key=lambda x: len(x))
        yield (name, p)

    def reducer_max_len(self, name, phrases):
        p = max(phrases, key=lambda x: len(x))
        yield None, (name, p)

    def reducer_sort_phrases(self, _, phrases_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        sorted_pairs = sorted(phrases_pairs, key=lambda x: len(x[1]), reverse=True)
        yield None, sorted_pairs
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer_max_len),
            MRStep(reducer=self.reducer_sort_phrases)
        ]

if __name__ == '__main__':
    MRTask2.run()
