This folder includes the dj challenge implemented in Spark 2.3

This job includes 2 functionalities :

- Dictionary: Generates dictionary files from current dataset.
- Inverted Index: Generates inverted index from dataset and dictionary.

Prerequisites:
- dataset
- sbt

INSTRUCTIONS:

In project folder:

Open SBT console. Compile. Then we need to run the 2 functionalities.

- First we need to generate the dictionary.
	run ${your_directory}/data-engineering-coding-challenge/dataset ${your_directory}/dj_challenge_spark/dictionary
Example: 
	run /Users/miquel/dj_challenge/data-engineering-coding-challenge/dataset /Users/miquel/dj_challenge/dj_challenge_spark/dictionary

    - select: [1] sparkchallenge.BuildDictionary

- After generating dictionary, we can generate inverted index.
    run ${your_directory}/data-engineering-coding-challenge/dataset ${your_directory}/dj_challenge_spark/dictionary ${your_directory}/dj_challenge/dj_challenge_spark/inverted_index_result
Example: 
    run /Users/miquel/dj_challenge/data-engineering-coding-challenge/dataset /Users/miquel/dj_challenge/dj_challenge_spark/dictionary /Users/miquel/dj_challenge/dj_challenge_spark/inverted_index_result

    - select: [2] sparkchallenge.BuildInvertedIndex