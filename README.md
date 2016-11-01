# MapReduce on AWS

## Introduction

The penetration of the Internet and the increased usage of mobile devices have dramatically increased the data available in this world. The questions is, how are we going to process such a magnificent amount of data? Yes, we do have super computers in some places, but not everyone has access to them, especially normal developers. Is there a way for an individual to perform big data analysis efficiently?

Thanks to all the open-source communities, we do. One of the most popular model is MapReduce. We will talk about it in this tutorial. You'll learn how to process a dataset of dozens of gigabytes with the help of cloud services. It means you can do it on a normal machine, anywhere, anytime.   

#### Tutorial Content

- [MapReduce](#MapReduce)
- [Understanding MapReduce: Wordcount](#Understanding-MapReduce:-Wordcount)
- [Meet mrjob](#Meet-mrjob)
- [Let's Talk About Shakespeare](#Let's-Talk-About-Shakespeare)
- [Elastic MapReduce](#Elastic-MapReduce)
- [Streaming Hadoop MapReduce](#Streaming-Hadoop-MapReduce)
- [Build Your Own Hadoop Cluster](#Build-Your-Own-Hadoop-Cluster)
- [Summary](#Summary)
- [References](#References)

## MapReduce

The MapReduce programming model is designed to process big dataset using a large number of machines in-parallel. It could be illustrated as a "server farm". The main advantage of MapReduce is to process data on a flexible scale over many machines. Imagine you are processing a dataset of 100TB, it may take days to complete this task on a single machine, even it's a very powerful one, but perhaps only a few hours on a cluster of twenty regular machines, as they are working simultaneously and collaboratively on one dataset. 

#### Hadoop MapReduce

[Hadoop MapReduce](http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz) is an open-source implementation of Google's MapReduce. Though Hadoop MapReduce is written in Java, its implementation can be realzied through other languages, such as Python and C. Hadoop MapReduce uses the Hadoop Distributed File System (HDFS) as the underlying file system. 

#### How it works
(skip this part if you want, it's easier to read the following examples)

Hadoop MapReduce has mainly two phases: Map and Reduce. The "map phrase" includes split, map, and partition tasks and the "reduce phase" includes shuffle, merge & sort, and reduce tasks. There should be at least one map task and zero or more reduce tasks in a MapReduce job. The job first creates HDFS partitions by spliting the dataset into chunks of a certain size and then distribute the chunks to all the nodes in the cluster. MapReduce jobs can process HDFS blocks in parallel at distributed machines and save the output to one directory.

A standard MapReduce would follow these six steps:  

1. **Map-Spliting**: The dataset is firstly splited into HDFS blocks chunks. If a file is smaller than the size limit, it remains intact, otherwise it will be split. The default size of each chunk is 100MB, but this can be determined by user configuraion. For example, if we have two files for the MapReduce job, one at 87MB and the other at 123MB, we will have three HDFS chunks: 87MB, 100MB, 23MB. This process will be completed automatically by Hadoop MapReduce and is not controled by our code; 

2. **Map-Map**: This task is where our code for Map phase gets executed. For each line of the data, our map() function is invoked to process the line according to the desired patterns. The output of our map() function would be key-value pairs. For example, if we analyze an article, we may break lines into words, and generate a key-value pair for each word as ['word':1]. This is a process where we "extend" our data with redundancy;

3. **Map-Partition**: The output of Map function is exported directly to a in-memory buffer using "print" in Python (System.out.println() in Java). Each time when the buffer is almost full, 80% by default, it created a sorted partition. The output of this task are partitions with various features;

4. **Reduce-Shuffle**: The data partitions from Map phase will be shuffled. Partitions share certain charactistics will be recognized as one group.This is a process controlled by MapReduce's Resource Manager;

5. **Reduce-Merge&Sort**: Those partitions that have been categorized as one group will together form a single, large partition, which will be used to feed the Reduce task. This is a process controlled by MapReduce's Resource Manager;

6. **Reduce-Reduce**: A partiion, a group of data, will be processed by our reduce() function in this task. Ususally, it should summarize the output, leading to a significant reduce in the data size, as this task removes redundancy;

(Step 3, 4 and 5 are often recogized as one "Shuffle" stage in many cases, as they are closed related and are not controlled by user functions)    

## Understanding MapReduce: Wordcount

Wordcount is the simplest implementation of MapReduce task. Let's get our hands dirty now. Given the sentence below:

![ppap](https://cloud.githubusercontent.com/assets/14355257/19906093/ca4cf962-a04f-11e6-9d9e-0e4053259dbe.png)
<center>__"I have a pan, I have an applie, ah, apple-pan."__</center>

We firstly clean up the text line using this clean function. We will implement similar cleaning process inside our mapper function later. 

```
text = "I have a pan, I have an applie, ah, apple-pan."

import nltk
import string

def clean(text, lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()):
    result = []
    processed = text.lower()
    processed = processed.replace("'s", "")
    processed = processed.replace("'", "")
    remove_punct = string.maketrans(string.punctuation," "*len(string.punctuation))
    processed = processed.translate(remove_punct)
    for element in nltk.word_tokenize(processed):
        try:
            result.append(str(lemmatizer.lemmatize(element)))
        except:
            continue
    return ' '.join(result)
```
We shall have：
```
print clean(text)
```

#### Execution Steps Explained:

##### Split
The Map phase will firstly split the data into HDFS chunk and distribute them to several different machines, let's say three machines:  

Assume each chunk can have maximum four words (corresponding to 100MB):  
HDFS 1: i have a pan  
HDFS 2: i have an apple  
HDFS 3: ah apple pan  

Machine 1: HDFS1(i have a pan)  
Machine 2: HDFS2(i have an apple)  
Machine 3: HDFS3(ah apple pan)  

##### Map  
Now we execute a map() function for each chunk. After the Map function, we shall have many uncategorized partitions in each machine:  

Machine 1: {"i":1}  
Machine 1: {"have":1}  
Machine 1: {"a":1}  
Machine 1: {"pan":1}  
  
Machine 2: {"i":1}  
Machine 2: {"have":1}  
Machine 2: {"an":1}  
Machine 2: {"apple":1}   
  
Machine 3: {"ah":1}   
Machine 3: {"apple":1}    
Machine 3: {"pan":1}

##### Shuffle    
We categorize and sort similar partitions and make them new partitions, which we used to feed our Reducer.     
  
New Partition 1: {"i":1}  
New Partition 1: {"i":1}  
New Partition 1: {"have":1}  
New Partition 1: {"have":1}  
  
New Partition 2: {"a":1}  
New Partition 2: {"an":1}  
New Partition 2: {"ah":1}  
  
New Partition 3: {"pan":1}    
New Partition 3: {"pan":1}  
New Partition 3: {"apple":1}     
New Partition 3: {"apple":1}     

##### Reduce   
Finally we invode reduce() function on each newly-formed partition and get the word counts for each word.  
Reducer 1: {"i":2}   
Reducer 1: {"have":2}  
  
Reducer 2: {"a":1}  
Reducer 2: {"an":1}  
Reducer 2: {"ah":1}  
  
Reducer 3: {"pan":2}   
Reducer 3: {"apple":2}  

The final results of word counts will be put to target HDFS directory at the user's disposal.

## Meet mrjob

[mrjob](https://pythonhosted.org/mrjob/index.html) is a ad-hoc library for python programs to operate on Hadoop MapReduce. We will use mrjob library to write our program for Hadoop MapReduce. Although we can also use no library with simple sys.in, mrjob is more integrated and allows us to run and debug locally (also allows us to disply the result in Jupyter). Each mrjob has to have at least one mapper, one combiner(shuffle), and one reducer, included in one or multiple "steps". 

Below is a simple example using mrjob to implement the "apple-pan" example we described above.  

```
%%file wordcount.py
from mrjob.job import MRJob

class MRWordFreqCount(MRJob):

    # Assign one count to each word
    def mapper(self, _, line):
        for word in line.split():
            yield word, 1
    
    # Sum up the frequency of each word
    def combiner(self, word, counts):
        yield word, sum(counts)
    
    # Generate the results
    def reducer(self, word, counts):
        yield word, sum(counts)
```
P.S: You may have concern regarding the space this cleaning process takes, but recall that Hadoop MapReduce split large files into HDFS pieces at 100MB, so it is very unlikely to suffer from stake overflow in reality. In case that happens, simply decrease the size of default HDFS chunks, say, to 50MB. 

```
# This cell displays the results of mrjob in Jupyter. This is not working in Hadoop
import wordcount
reload(wordcount)

# With in example.txt, we have:
# "i have a pan i have an applie ah apple pan"
mr_job = wordcount.MRWordFreqCount(args=['example.txt'])
with mr_job.make_runner() as runner:
    runner.run()
    for line in runner.stream_output():
        key, value = mr_job.parse_output_line(line)
        print key, value
```

## Let's Talk About Shakespeare

In this part, we will count the number of words in "shakespeare.txt", which we used in a previous project, and fine out the top 100 words used. Though it is not a huge file(4MB), it is plain text and contains very rich contents. The file can thus generate a very representative output of a Hadoop MapReduce job. If you are looking for a larger dataset, you could tend to [Twitter](https://dev.twitter.com/docs), [Wikipedia](https://www.mediawiki.org/wiki/API:Main_page), or [this guide](http://kevinchai.net/datasets)

```
# This is the mrjob program we will use on Hadoop Mapreduce
from mrjob.job import MRJob
from mrjob.step import MRStep
import string
import nltk

class MRTopWords(MRJob):
    
    # Store the key-value pair
    topdict = {}
        
    # Clean up the text and assign one count to each word
    def mapper_get_words(self, _, line):
        # Clean up text before further processing
        result = []
        processed = line.lower()
        processed = processed.replace("'s", "")
        processed = processed.replace("'", "")
        remove_punct = string.maketrans(string.punctuation," "*len(string.punctuation))
        processed = processed.translate(remove_punct)
        lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()
        for element in nltk.word_tokenize(processed):
            try:
                result.append(str(lemmatizer.lemmatize(element)))
            except:
                continue
        result = ' '.join(result)
        
        for word in result.split():
            yield word, 1
    
    # Sum up the frequency of each word
    def combiner_count_frequency(self, word, counts):
        yield word, sum(counts)
    
    # Populate the dictionary with word occurances
    def reducer_populate_frequency(self, word, counts):
        frequency = sum(counts)
        
        # Eliminate words with only one occurance
        if frequency > 1:
            self.topdict[word] = frequency
        dummy = self.topdict
        yield None, dummy
    
    # Second reducer used to print out 
    def reducer_find_top_word(self, _, dummy):
        count = 0
        sorted_list = sorted(self.topdict, key=self.topdict.get, reverse=True)
        for key in sorted_list:
            yield key, self.topdict[key]
            count += 1
            if count > 99:
                break
    
    # Steps allow us to flexibly construct the MapReduce processes
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                    combiner=self.combiner_count_frequency,
                    reducer=self.reducer_populate_frequency),
            MRStep(reducer=self.reducer_find_top_word)
        ]

# These lines are required if we run the program on EMR, but not locally.
if __name__ == '__main__':
    MRTopWords.run()

# We may need to changed these two lines to if the above lines does not yield the desired results:
if __name__ == '__main__':
    job = MRTopWords(args=['-r', 'emr'])
    with job.make_runner() as runner:
        runner.run()
        for line in runner.stream_output():
            key, value = job.parse_output_line(line)
            print key, value
```

We cannot run and display the result here, as shakespeare is a large file and thus there are too many arguments for input, leading to exception: "[Errno 22] Invalid argument". In addition, it would also be a challenge for our memory.

## Elastic MapReduce
Amazon Web Services (AWS) is one of the most popular cloud computing services available for all parties. AWS offers various services that support all kinds of missions developers may have. In our scope, we will use the Elastic MapReduce (EMR) service from AWS.

#### Get Our AWS Ready  
  
1. Log into our AWS account, go to "Security Credentials", create a new credential, and download it(automatically);  
2. Create a configuation file named "mrjob.conf", copy the following contents inside:  
runners:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;emr:  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;aws_access_key_id: AKIAISOAS3434(Your credentials)  
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;aws_secret_access_key: AKIAISOAS3434(Your credentials)  
3. Launch an EMR cluster. Click "Create cluster" on the upper left corner and go to advanced options.  
  
    Uncheck all other software that we will not use.  
![emr1](https://cloud.githubusercontent.com/assets/14355257/19906075/be2e1fe4-a04f-11e6-8c53-1a4726cc60ff.png)
  
    Pick three m1.large instace with spot price. The size of the instances is large enough for our small file and the spot price is way much cheaper than on-demand price (at least ten times!).    
![emr2](https://cloud.githubusercontent.com/assets/14355257/19906074/be2e219c-a04f-11e6-97a0-bded40e06f18.png) 

    Change the security group to an all-open one, so we will not encounter any weird exception due to permission denial.  
![emr3](https://cloud.githubusercontent.com/assets/14355257/19906076/be31c5fe-a04f-11e6-85bf-2c528e3c7ba7.png)

    Keep clicking next and create the cluster. It may take a while for the spot instances to be launched.   
    
4. Once the cluster is up and running, SSH into the master node(very important) and put our .py file, .txt file, and also this .conf file in one dictionary. You can also upload them to S3 storage, but we're not here for cloud computing, so let save them locally.  
5. Download the packages we need:  
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`sudo pip install mrjob`
6. Run the following command to start our MapReduce quest:  
    &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;`python mapr.py -r emr shakespeare.txt --conf-path mrjob.conf`
7. Sit back and wait for its completion.  
8. Once it's finished, it generates an output file in a temperory dictionary in our S3 storage. We could go and collect our output file(usually a .txt file) in this newly-generated folder.  
9. Don't forget to close your EMR cluster. Yes I lost $5 for forgetting this. It could be worse    
  
#### Below is our output:  
```
# "the"   27052
# "and"   25083
# "i"     20142
# "to"    18993
# "of"    15867
# "a"     14196
# "you"   13347
# "my"    11875
# "that"  10847
# "in"    10568
# "is"    8851
# "not"   8235
# "it"    7512
# "me"    7489
# "with"  7403
# "for"   7372
# "be"    6670
# "his"   6518
# "your"  6507
# "he"    6454
# "this"  6448
# "but"   6055
# "have"  5746
# "as"    5535
# "thou"  5194
# "him"   5070
# "will"  4838
# "so"    4823
# "what"  4712
# "her"   3838
# "thy"   3727
# "all"   3709
# "no"    3676
# "do"    3640
# "by"    3606
# "shall" 3472
# "if"    3437
# "are"   3324
# "we"    3275
# "thee"  3024
# "our"   3016
# "on"    2948
# "good"  2724
# "now"   2722
# "lord"  2651
# "o"     2559
# "from"  2537
# "well"  2499
# "sir"   2452
# "come"  2451
# "at"    2448
# "they"  2391
# "she"   2384
# "enter" 2338
# "or"    2332
# "here"  2290
# "let"   2280
# "would" 2248
# "more"  2227
# "which" 2192
# "was"   2164
# "there" 2147
# "how"   2118
# "then"  2101
# "am"    2100
# "love"  1996
# "their" 1991
# "ill"   1983
# "man"   1940
# "them"  1935
# "when"  1914
# "hath"  1845
# "than"  1803
# "one"   1760
# "like"  1751
# "an"    1738
# "go"    1693
# "upon"  1671
# "king"  1650
# "know"  1635
# "us"    1632
# "say"   1624
# "may"   1600
# "make"  1586
# "did"   1571
# "were"  1538
# "yet"   1526
# "should"        1510
# "must"  1465
# "why"   1455
# "had"   1392
# "out"   1385
# "tis"   1384
# "see"   1378
# "such"  1351
# "where" 1314
# "give"  1297
# "who"   1288
# "these" 1282
# "some"  1281
```

## Streaming Hadoop MapReduce

If you have low demand on local tests or do not want to use mrjob library and set up the EMR environment, I get a good news for you. There is a way to generate output file without even logging in to your node: Streaming. We simply tell the cluster where the mapper, reducer, input file, output file are and the cluster will finish all tasks automatically. Using streaming, we have to upload all the required to S3 stroage and download the output from it as well. Noteice that there is no combiner, so we have to make some changes to our programs, because reducer will now take the output of mapper as input. To create a streaming MapReduce program, we need to launch an EMR cluster with these configurations changed:  

![streaming1](https://cloud.githubusercontent.com/assets/14355257/19906085/c25ce03c-a04f-11e6-8396-59634f1e5931.png)
![streaming2](https://cloud.githubusercontent.com/assets/14355257/19906086/c261bc4c-a04f-11e6-9526-9a233f386bb8.png) 
  
  
Here are sample mapper and reducer we can use for the same purpose:  
mapper.py  
```
import sys
import string
import nltk

for line in sys.stdin:
    processed = line.lower()
    processed = processed.replace("'s", "")
    processed = processed.replace("'", "")
    remove_punct = string.maketrans(string.punctuation," "*len(string.punctuation))
    processed = processed.translate(remove_punct)
    lemmatizer=nltk.stem.wordnet.WordNetLemmatizer()
    for element in nltk.word_tokenize(processed):
        try:
            result.append(str(lemmatizer.lemmatize(element)))
        except:
            continue
        
    for word in result:
        print '%s\t%s' % (word, 1) # print formatted results, not yield
```
reducer.py
```
import sys

counter = collections.Counter()

for line in sys.stdin:
    key, value = line.strip().split("\t")
    counter[key] += int(value)

result = counter.most_common(100)
for each in result:
    print '%s\t%s' % (each[0], each[1])
```
## Build Your Own Hadoop Cluster

To avoid the high costs of AWS EMR, we could also run Hadoop MapReduce tasks locally on linux machines. I have successfully set up the Hadoop environment, but got challenged when I tried to run my program on a cluster of virtual machines. However, this is a valid way to execute a Hadoop MapReduce program and should be concerned when you have limited budget.  

If you have a PC, you can download [VirtualBox](https://www.virtualbox.org/wiki/Downloads) with an [Ubuntu 14.04](http://releases.ubuntu.com/14.04/) image to launch linux machines. These two links provide very comprehensive tutorials about how to set up Hadoop environment in your local machine.

1. [Running Hadoop on Ubuntu Linux (Single-Node Cluster)](http://www.michael-noll.com/tutorials/running-hadoop-on-ubuntu-linux-single-node-cluster/)  
2. [Setting up a Apache Hadoop 2.7 single node on Ubuntu 14.04](http://thepowerofdata.io/setting-up-a-apache-hadoop-2-7-single-node-on-ubuntu-14-04/)  

## Summary

Now, you know at least three ways to perform MapReduce tasks: with regular EMR, with streaming EMR, and with local machines. Though we only talked about word count here, there are other tasks you can perform using MapReduce and their idea is the same: map and then reduce. One example is to find mutual friends, based on which you can make friend recommendation. This is a common model used by many organizations such as Facebook and Linkedin.  
  
In a word, there is a lot more to explore about MapReduce, a mature and practical model for big data analysis.  
![recommendation](https://cloud.githubusercontent.com/assets/14355257/19906092/c91b8036-a04f-11e6-8bd6-5bffe4e8a5f1.jpg)
<center>__Using MapReduce to Find Common Friend__</center>

## References   
[Apache Hadoop](http://www.apache.org/dyn/closer.cgi/hadoop/common/hadoop-2.7.0/hadoop-2.7.0.tar.gz)  
[mrjob v0.5.6 documentation](https://pythonhosted.org/mrjob/index.html)  
[Anatomy of a MapReduce Job](http://ercoppa.github.io/HadoopInternals/AnatomyMapReduceJob.html)     
[mrjob and S3](https://www.classes.cs.uchicago.edu/archive/2013/spring/12300-1/labs/lab5/)  
[Elastic Map Reduce with Amazon S3, AWS, EMR, Python, MrJob and Ubuntu 14.04](http://meshfields.de/elastic-map-reduce/)  
