# ngram_count_hadoop
Code to get n-gram count of text files with Hadoop MapReduce

Compiled using NetBeans IDE and JDK-1.8.

## Usage
`hadoop jar target/NgramCount-1.0-SNAPSHOT.jar br.com.ggvd.ngramcount.NgramCount <ngram> <minCount> <inputDirectory> <outputDirectory>`

Args: 
- ngram: N gram value (e.g., for bigrams, N=2)
- minCount: minimum count a ngram must have to be included in the output
- inputDirectory: local directory where the input text files are stored
- outputDirectory: local directory where the output will be saved

Considerations:
- The code implementation considers that the files are stored and saved in a `local directory`, not in HDFS.
- The results are stored in the files `output/part-r-*` for each reducer result (`-r-` stands for reducer), and if the job is sucessful a `_SUCCESS` file is created
