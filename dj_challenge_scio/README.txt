This folder includes the dj challenge implemented in SCIO (Spotify).

Works on the top of Apache Beam, there 

This job includes 2 functionalities :

- Dictionary: Generates dictionary files from current dataset.
- Inverted Index: Generates inverted index from dataset and dictionary.

Prerequisites:
- dataset
- sbt

INSTRUCTIONS:

In project folder:

Open SBT console. Compile. Then we need to run the 2 functionalities.
We will execute this job with SparkRunner. (--runner=SparkRunner).

DICTIONARY.

- First we need to generate the dictionary.
    run --input=${your_directory}/data-engineering-coding-challenge/dataset/* --output=${your_directory}/dj_challenge_scio/dictionary --runner=SparkRunner

Example: 
    run --input=/Users/miquel/dj_challenge/data-engineering-coding-challenge/dataset/* --output=/Users/miquel/dj_challenge/dj_challenge_scio/dictionary --runner=SparkRunner

select: [1] scio.BuildDictionary

INVERTED INDEX.

- After generating dictionary, we can generate the inverted index.
    run --input=${your_directory}/data-engineering-coding-challenge/dataset/* --dictionary=${your_directory}/dj_challenge_scio/dictionary/* --output=${your_directory}/dj_challenge_scio/inverted_index_result --runner=SparkRunner
Example: 
    run --input=/Users/miquel/dj_challenge/data-engineering-coding-challenge/dataset/* --dictionary=/Users/miquel/dj_challenge/dj_challenge_scio/dictionary/* --output=/Users/miquel/dj_challenge/dj_challenge_scio/inverted_index_result --runner=SparkRunner

select: [2] scio.BuildInvertedIndex