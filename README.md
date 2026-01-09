
AWS-EMR-Collocation-Extractor
=================================================

Created by:
Niv Yaakobov - 322578998

-------------------------------------------------
1. Project Overview
-------------------------------------------------
This project implements a system for automatic Collocation Extraction from the 
Google N-Grams dataset (English and Hebrew)[cite: 263, 279]. A collocation is a 
sequence of words that co-occur more often than expected by chance[cite: 265]. 
The system utilizes Hadoop MapReduce on Amazon Elastic MapReduce (EMR) to 
process massive datasets and calculate the Log-Likelihood Ratio (LLR) for 
word pairs across different decades[cite: 267, 303].

-------------------------------------------------
2. System Architecture & MapReduce Steps
-------------------------------------------------
The solution consists of 4 distinct MapReduce steps to ensure scalability:

Step 1: Calculate N - Calculates the total number of unigrams for each decade
to establish the corpus size[cite: 275].

Step 2: Join Bigrams with C1 - Joins the 2-gram dataset with 1-gram counts to
attach the frequency of the first word (C1)[cite: 275]. 
* Optimization: Stop words are filtered at this stage to drastically reduce 
  data size and prevent disk overflow[cite: 298].

Step 3: Join with C2 & LLR Calculation - Joins with 1-gram counts for the 
second word (C2) and calculates the final LLR score using the log-likelihood 
formula[cite: 272, 275].

Step 4: Sort & Filter - Uses a Composite Key and Secondary Sort to rank the 
top 100 collocations per decade by their LLR score in descending order of 
strength[cite: 278].

-------------------------------------------------
3. How to Run
-------------------------------------------------
1. Build the JAR: 
   mvn clean package

2. Upload to S3: 
   Upload the resulting JAR file to your S3 bucket[cite: 316, 367].

3. Run via EmrRunner:
   mvn org.codehaus.mojo:exec-maven-plugin:3.1.0:java \
   '-Dexec.mainClass=com.assignment2.EmrRunner'

* Note: Ensure AWS credentials are configured in ~/.aws/credentials[cite: 312].

-------------------------------------------------
4. Challenges & Technical Solutions
-------------------------------------------------
* Disk Space Management: To handle "No space left on device" errors on AWS 
  EMR, we implemented filtering of unigrams and bigrams before the shuffle 
  phase using a stop-words list[cite: 298].
  
* Sorting Logic: Strong collocations result in large negative LLR values. We 
  implemented a custom WritableComparable key to ensure mathematically 
  smaller (stronger) values appear first.

* S3 Access: Resolved "Wrong FS" errors by dynamically loading the 
  FileSystem in the setup() method using path.getFileSystem(conf) to 
  correctly interface with S3[cite: 367].

-------------------------------------------------
5. Statistics: Impact of Local Aggregation
-------------------------------------------------
Comparison of network traffic between Step 1 (with Combiner) and Step 2 
(without Aggregation) [cite: 291]:

* With Aggregation (Step 1): ~31 KB Shuffle Bytes.
* Without Aggregation (Step 2): ~6.37 GB Shuffle Bytes.
* Reduction Ratio: ~99.99% for Step 1.

-------------------------------------------------
6. Sample Results
-------------------------------------------------
English:
* Good: starting point, private sector, Mental Health, vice versa.
* Bad: I shall, OF THE, I think, thou hast.

Hebrew:
* Good: ארצות הברית, בית הספר, ראש הממשלה, עבודה זרה.
* Bad: דף ודף, האלו נתונים, אנו רואים, אינו יכול.

-------------------------------------------------
7. Output Links
-------------------------------------------------
Final Results (S3): 
https://dist-sys-romniv-assign2.s3.us-east-1.amazonaws.com/output/run_1765968615063/step4_final/part-r-00002
