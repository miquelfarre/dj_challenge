from pyspark import SparkContext
import sys

def strip_punctuation(s):
    punctuations = '''!()-[]{};:"\,<>./?@#$%^&*_~'''
    return ''.join(c for c in s if c not in punctuations)

if __name__ == "__main__":    
    sc = SparkContext("local", "Dictionary")
    lines = sc.textFile(sys.argv[1])
    words = lines.flatMap(lambda line: line.lower().encode("ascii", "ignore").split(" ")) \
    		 .map(lambda word: strip_punctuation(word).strip()) \
             .distinct() \
             .zipWithIndex() \
             .saveAsTextFile(sys.argv[2])