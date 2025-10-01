# Spark Word Count Assignment

## 📘 Basic Word Count

In the first phase of this assignment, we wrote a basic word count program using **Apache Spark** which reads in a single text file, 
counts the number of occurrences of each word in the text file, and outputs the result back to a text file as a list of key-value pairs, 
where the key is the word and the value is the number of times the word occurred in the text file.

### Dataset Source
- [Project Gutenberg](https://www.gutenberg.org/ebooks/)

---

## 🔧 Extended Word Count

In addition to basic word counting, we extended our code to do the following:

- Make counting **case-insensitive** (`lower()` in Python)  
- Ignore **punctuation** (`translate()` in Python)  
- Ignore **stopwords** (`filter()` in Spark)  
- Sort output by count in **descending order** (`sortBy()` in Spark)  

To accomplish these tasks, we used reference code from:  
- [RDD Programming Guide - Spark Documentation](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

---

## 📊 Analysis

### 1. Basic Word Count on Single Text File

**Question:** What are the 25 most common words?  

**Answer:** (no stopwords/punctuation removal, raw counts only)

```
[
('the', 2226)
('of', 1264)
('and', 1179)
('I', 1175)
('a', 839)
('', 802)
('to', 737)
('in', 564)
('was', 537)
('my', 424)
('that', 404)
('had', 346)
('it', 276)
('with', 253)
('as', 241)
('at', 216)
('The', 210)
('for', 200)
('this', 161)
('were', 154)
('or', 144)
('me', 144)
('you', 140)
('on', 135)
('from', 131)
]
```

#### Execution Stages
- **1 job and 2 stages**  
  - Stage 1: Reading the text file, performing `reduceByKey`.  
  - Stage 2: Executing `partitionBy`, `mapPartitions`, `map`, `coalesce`, `saveAsTextFile`.  

**Reason:**  
Spark creates stages based on transformations and actions. Wide dependencies (e.g., `reduceByKey`) trigger shuffles and stage boundaries.  
Lazy evaluation means execution starts only when an action is called.

---

### 2. Extended Word Count on Two Text Files

**Question:** What are the 25 most common words?  

**Answer:** (case-insensitive, no punctuation, stopwords removed, sorted descending)

```
[
('time', 266)
('one', 168)
('project', 167)
('us', 140)
('upon', 131)
('may', 127)
('little', 126)
('would', 126)
('could', 116)
('gutenberg™', 112)
('came', 112)
('man', 110)
('work', 109)
('said', 100)
('first', 99)
('saw', 89)
('like', 88)
('machine', 88)
('hath', 87)
('people', 86)
('must', 84)
('thing', 78)
('even', 75)
('without', 75)
('seemed', 73)
]
```

#### Execution Stages
- **3 jobs and 6 stages**  
  - Stage 0: Read 2 text files, perform `union`, `reduceByKey`.  
  - Stages 1–4: Execute `partitionBy`, `mapPartitions`, `sortBy`.  
  - Stage 5: Perform final `partitionBy`, `mapPartitions`, `map`, `coalesce`, `saveAsTextFile`.  

**Reason:**  
Larger data requires additional shuffling and repartitioning. Wide dependencies (from `reduceByKey`, `sortBy`) break execution into multiple stages.

---

## 📈 DAG Lineage Graph Task

**Task:** Draw the lineage graph DAG for the RDD `ranks` on line 12 when iteration variable `i = 2`.  
- Include nodes for all intermediate RDDs, even unnamed.  

**Answer:** (Graphical representation to be added as screenshot/diagram.)

---

