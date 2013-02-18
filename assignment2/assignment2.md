Chris Imbriano
828G : Spring 2013
Assignment 2 Questions

Question 0:

Pairs.

Uses two MapReduce Jobs.

Job 1 first calculates the appearance total for each term and produces a file with the format:

[term] [count]

where term is a token and count is the number of times that token appeared in a document.

Then, using the same input as Job 1, Job 2's mapper emits PairsOfStrings for each pair of co-occurring terms. Job 2's reducer uses the setup method to build an in memory map of terms and their count total using the data set produced by the first job. The reducer now has the pair, the pair's total count, the total counts for each of the terms in the pair, and the total number of documents and can calculate the PMI. The output file has the format:

(term1, term2) [PMI(term1, term2)]


Key-vlaue pairs flow
Job 1
MAP IN:  Long Writable, Text
MAP OUT: Text, IntWritable

REDUCE IN:  Text, Iterable<IntWritable>
REDUCE OUT: Text, IntWritable

Job 2
MAP IN:  Long Writable, Text
MAP OUT: PairOfStrings, IntWritable

REDUCE IN: 	PairOfStrings, Iterable<IntWritable>
REDUCE OUT: PairOfStrings, DoubleWritable


Stripes.

The stripes solution is very similar. The first MR job produces the term totals data set in the same way as the Pairs solution. 

The difference is in the handling of the co-occurrence pairs.  Whereas in the Pairs solution, mappers emit PairsOfStrings as the key representing a co-occurrence pair, the Stripes mappers emit a term and a corresponding map of "second term" to totals.  For example, consider the following input:

"A B C A B D"

A mapper would emit a key for each of the unique terms: A B C D.  For each of these terms,

A -> {A -> 1, B -> 1, C -> 1, D -> 1}

which says that A co-occurred with each of A, B, C, and D in this context.

The reducers in this case perform an element-wise sum of all the maps for a particular key.  This results in the reducer having the first term in a pair as the key and each subsequent second term as part of the final summed map.  The final output has the same format as the Pairs solution.


Key-vlaue pairs flow
Job 1
MAP IN:  Long Writable, Text
MAP OUT: Text, IntWritable

REDUCE IN:  Text, Iterable<IntWritable>
REDUCE OUT: Text, IntWritable

Job 2
MAP IN:  Long Writable, Text
MAP OUT: Text, HMapSIW

REDUCE IN: 	Text, Iterable<HMapSIW>
REDUCE OUT: PairOfStrings, DoubleWritable



Question 1. What is the running time of the complete pairs implementation (in your VM)? What is the running time of the complete stripes implementation (in your VM)?



Question 2. Now disable all combiners. What is the running time of the complete pairs implementation now? What is the running time of the complete stripes implementation?

Question 3. How many distinct PMI pairs did you extract?

Question 4. What's the pair (x, y) with the highest PMI? Write a sentence or two to explain what it is and why it has such a high PMI.

Question 5. What are the three words that have the highest PMI with "cloud" and "love"? And what are the PMI values?

Note that you can compute the answer to questions 3—6 however you wish: a helper Java program, a Python script, command-line manipulation, etc.