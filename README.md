# Distributed Word Count with PySpark

This project demonstrates **basic and extended word count programs** built with **Apache Spark (PySpark)**.  
The goal is to practice **distributed computation concepts** (RDD transformations, actions, jobs, stages, DAGs) and text analytics using large text files from [Project Gutenberg](https://www.gutenberg.org/ebooks/).

---

## 🚀 Features

### Basic Word Count
- Reads a single text file.
- Counts number of occurrences of each word.
- Outputs key-value pairs (word, count).

### Extended Word Count
- Case-insensitive (lowercased words).
- Ignores punctuation.
- Removes stopwords.
- Sorts output by count (descending).
- Runs on **two text files** combined.

