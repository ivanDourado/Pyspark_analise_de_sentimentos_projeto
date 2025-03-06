# Pyspark Sentiment Analysis Project

## Project Description

This project is part of a university assignment, using **PySpark** to perform **sentiment analysis** and execute various computations based on a dataset of movie reviews.

## Objectives

- **Load and preprocess a dataset**: Work with `imdb-reviews-pt-br.csv`, which contains movie reviews in both Portuguese and English.
- **Perform sentiment analysis**: Identify reviews classified as positive or negative.
- **Compute requested metrics**:
  - **Sum of IDs** for negative reviews.
  - **Word count difference** between Portuguese and English texts.

## Dataset

The dataset consists of the following columns:

- `id`: Unique identifier for each review.
- `text_en`: Movie review in English.
- `text_pt`: Movie review in Portuguese.
- `sentiment`: Sentiment classification (`pos` for positive, `neg` for negative).

## Project Structure

```bash
├── pyspark_sentiment_analysis.ipynb  # Jupyter Notebook with all analysis
├── imdb-reviews-pt-br.csv            # Dataset file (must be added manually)
└── README.md                         # Documentation of the project
```

## Prerequisites

To run this project, ensure you have the following dependencies installed:

- **Google Colab** (or a local Jupyter Notebook)
- **Python 3.7+**
- **Apache Spark (PySpark)**

Install necessary dependencies using:

```bash
pip install pyspark
```

If running in **Google Colab**, execute the following command to install Java:

```bash
!apt-get install openjdk-8-jdk-headless -qq > /dev/null
```

## How to Run the Project

### 1. Set Up PySpark Environment

```python
import os
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.4.2-bin-hadoop3"

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Sentiment Analysis") \
    .getOrCreate()
```

### 2. Load the Dataset

```python
df = spark.read.csv("/content/imdb-reviews-pt-br.csv", header=True, quote="\"", escape="\"", encoding="UTF-8")
df.show(5)
```

### 3. Convert `id` Column to Integer

```python
from pyspark.sql.functions import col, regexp_replace

df = df.withColumn("id", regexp_replace("id", "", "").cast("int"))
df.printSchema()
```

### 4. Compute Sum of `id` Values for Negative Reviews

```python
df_neg = df.filter(df.sentiment == "neg")
total_neg = df_neg.groupBy("sentiment").agg(sum("id").alias("Total_Sum_Neg"))
total_neg.show()
```

### 5. Compute Word Count Difference (Portuguese vs. English)

```python
from pyspark.sql.functions import split, explode, desc

# Tokenize words
en_words = df.select("id", explode(split("text_en", "[ ]+")).alias("word"))
pt_words = df.select("id", explode(split("text_pt", "[ ]+")).alias("word"))

# Count words
count_en = en_words.groupBy().count().collect()[0][0]
count_pt = pt_words.groupBy().count().collect()[0][0]

print(f"Portuguese text has {count_pt} words, English text has {count_en} words.")
print(f"Difference: {count_pt - count_en} words.")
```

## Expected Outputs

- **Sum of negative review IDs**: `459568555`
- **Word count difference**: `54,997 more words in Portuguese text than in English text.`

## Contributing

If you’d like to contribute:

1. Fork the repository.
2. Clone the project.
3. Create a new branch (`git checkout -b feature-branch`).
4. Make your changes and commit (`git commit -m 'Add new feature'`).
5. Push to the branch (`git push origin feature-branch`).
6. Open a Pull Request.

## License

This project is for educational purposes and follows an open-source model. Feel free to use, modify, and distribute.

---

This documentation follows the best practices for GitHub README files, providing clarity for users and potential contributors. 🚀
