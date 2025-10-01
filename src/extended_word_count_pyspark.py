import nltk
nltk.download('stopwords')

import string
import time
from typing import List, Union
from nltk.corpus import stopwords

from pyspark import SparkContext
from pyspark.rdd import RDD

sc = SparkContext("local", "Word_Count_App")


def read_text_files(input_files: Union[str, List[str]]) -> RDD:
	all_sentences: RDD = sc.emptyRDD()

	if isinstance(input_files, list):

		for input_file in input_files:
			sentences = sc.textFile(input_file)
			all_sentences = all_sentences.union(sentences)

	else:
		sentences = sc.textFile(input_files)
		return sentences

	return all_sentences


def process_line(line):
	if line.strip():
		line = line.translate(str.maketrans("", "", string.punctuation))

	return [word.lower() for word in line.split(" ")]


def get_word_count(file_contents: RDD) -> RDD:
	all_words = file_contents.flatMap(lambda content: process_line(content))

	stop_words = set(stopwords.words("english"))
	filtered_text = all_words.filter(lambda word: word not in stop_words)

	word_counts = filtered_text.map(lambda word: (word, 1))
	word_count_frequency = word_counts.reduceByKey(lambda x, y: x + y)
	sorted_word_count_frequency = word_count_frequency.sortBy(lambda x: x[1], ascending=False)

	filtered_result = sorted_word_count_frequency.filter(lambda x: x[0] != '')

	return filtered_result


def save_result_in_text_file(output_file_path: str, output_rdd: RDD) -> None:
	output_rdd.coalesce(1).saveAsTextFile(output_file_path)


def main() -> None:
	# input_file_paths: List[str] = [
	# 	"/Users/jaskirat/University_at_Buffalo/courses/data_intensive_computing/homeworks/homework3/src/dataset/common_sense.txt",
	# 	"/Users/jaskirat/University_at_Buffalo/courses/data_intensive_computing/homeworks/homework3/src/dataset/the_time_machine.txt"
	# ]

	input_file_path: str = "/Users/jaskirat/University_at_Buffalo/courses/data_intensive_computing/homeworks/homework3/src/dataset/the_time_machine.txt"

	file_contents = read_text_files(input_files=input_file_path)
	sorted_word_count_frequency: RDD = get_word_count(file_contents=file_contents)

	save_result_in_text_file(
		output_file_path="/Users/jaskirat/University_at_Buffalo/courses/data_intensive_computing/homeworks/homework3/basic_word_count_result.txt",
		output_rdd=sorted_word_count_frequency
	)

	time.sleep(240)
	sc.stop()


if __name__ == "__main__":
	main()

