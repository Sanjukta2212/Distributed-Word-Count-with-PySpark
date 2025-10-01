import nltk
nltk.download('stopwords')

import string
import time
from typing import List, Union
from nltk.corpus import stopwords

from pyspark import SparkContext
from pyspark.rdd import RDD

sc = SparkContext("local", "Word_Count_App")


def save_result_in_text_file(output_file_path: str, output_rdd: RDD) -> None:
	output_rdd.coalesce(1).saveAsTextFile(output_file_path)


def main() -> None:
	input_file_path: str = "/Users/sanjuktabaruah/University_at_Buffalo/2nd_Sem2/DIC/bonus_proj/Metamorphosis.txt"
	file_content = sc.textFile(input_file_path)

	all_words = file_content.flatMap(lambda line: line.split(" "))

	word_frequency_initialization = all_words.map(lambda word: (word, 1))
	
	word_frequency = word_frequency_initialization.reduceByKey(lambda x, y: x + y)

	save_result_in_text_file(
		output_file_path="/Users/sanjuktabaruah/University_at_Buffalo/2nd_Sem2/DIC/bonus_proj/basic_word_count_result.txt",
		output_rdd=word_frequency
	)

	time.sleep(180)
	sc.stop()


if __name__ == "__main__":
	main()
