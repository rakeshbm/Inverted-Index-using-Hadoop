Created an Inverted Index of words occurring in a set of web pages on Google Cloud App Engine.<br />The Hadoop's Mapper and Reducer classes were extended to build a map-reduction algorithm in Java.

The input data can be found here: https://ebiquity.umbc.edu/resource/html/id/351
It is a subset of 74 files from a total of 408 files (text extracted from HTML tags) derived from the
Stanford WebBase project.

Repository File Descriptions:

InvertedIndex.java - A Java implementation of Map-Reduce algorithm for unigrams. <br />

index.txt - Inverted Index output for selected unigrams. <br />

log.txt - Logger output of the cluster job running InvertedIndex.java. <br />

InvertedIndexBigrams.java - A Java implementation of Map-Reduce algorithm for bigrams. <br />

index_bigrams.txt - Inverted Index output for selected bigrams. <br />
