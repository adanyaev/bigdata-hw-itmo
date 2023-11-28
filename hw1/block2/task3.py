# %%file is an Ipython magic function that saves the code cell as a file

from mrjob.job import MRJob
from mrjob.step import MRStep

class MRTask3(MRJob):

    def mapper_init(self):
        import nltk
        nltk.download('punkt', quiet=True)
        nltk.download('stopwords', quiet=True)
        
    def mapper(self, _, line):
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        from nltk.util import bigrams
        spl = line.split('" "')
        name = spl[1].strip('"\n ')
        if name != 'dialogue':
            phrase = spl[2].strip('"\n ').lower()
            tokens = word_tokenize(phrase)
            tokens = [word for word in tokens if not word in stopwords.words('english') and word.isalnum()]
            tokens = list(bigrams(tokens))
            counter = {}
            for token in tokens:
                if token not in counter:
                    counter[token] = 1
                else:
                    counter[token] += 1
            for token in counter:
                yield (token, counter[token])

    def combiner(self, token, counts):
        
        yield (token, sum(counts))

    def reducer_token_counts(self, token, counts):
        
        yield None, (token, sum(counts))

    def reducer_sort_counts(self, _, token_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        sorted_pairs = sorted(token_pairs, key=lambda x: x[1], reverse=True)[:20]
        yield None, sorted_pairs
    
    def steps(self):
        return [
            MRStep(mapper_init=self.mapper_init,
                    mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer_token_counts),
            MRStep(reducer=self.reducer_sort_counts)
        ]

if __name__ == '__main__':
    MRTask3.run()
