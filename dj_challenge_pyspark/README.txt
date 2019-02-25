This folder includes the dj challenge implemented in Pyspark.

This job includes 2 functionalities :

- Dictionary: Generates dictionary files from current dataset.
- Inverted Index: Generates inverted index from dataset and dictionary.

Prerequisites:
- dataset
- Python 2.7
- Pyspark (pip install PySpark)

INSTRUCTIONS:

In project folder:

run (with 2 parameters). (dataset folder path args)(dictionary folder path args)

Example:
python dictionary.py 'file:///${your_directory}/dataset' 'file:///${your_directory}/dictionary'

Example:
    python dictionary.py 'file:////Users/miquel/dj_challenge/dj_challenge_pyspark/dataset' 'file:////Users/miquel/dj_challenge/dj_challenge_pyspark/dictionary'