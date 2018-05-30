# MapReduce Movie Count
### Overview
This program uses MapReduce to count the number of movies each user in the [Movielens ml-100k](https://grouplens.org/datasets/movielens/100k/) dataset has rated. The keys are a user ID, and the values are the number of movies the user has rated.

The `u.data` dataset has columns of the following form: 

user id | item id | rating | timestamp

### Specifics

This job was created using a pseudo-distributed Hadoop cluster on a RedHat Enterprise Linux 7.2 virtual machine.

This job uses `Hadoop 2.7.4`. You can find your version of Hadoop using the following in Terminal: `hadoop version`

The Java compiler version is `javac 1.8.0_144`. You can find your Java compiler version using: `javac -version`

### Running the Job

* Open Terminal and change the current directory to the directory holding `MovieCountDriver.java`, `MovieCountMapper.java`, `MovieCountReducer.java`, and ml-100k's `u.data` dataset.

* Create directory in HDFS: `hadoop fs -mkdir movie_count`

* Put the data set in movie_count dir in HDFS: `hadoop fs -put u.data movie_count`

* Compile with: ``javac -classpath `$HADOOP_HOME/bin/hadoop classpath` *.java``

  * Where `$HADOOP_HOME` is `/usr/share/hadoop`. 
  * Use `export HADOOP_HOME=/usr/share/hadoop` to create the `HADOOP_HOME` variable.
  * `hadoop classpath` enables all imports in code to be resolved.

* Package the compiled files into a Java archive file using `jar` command in JDK: `jar cvf MovieCount.jar *.class`

* Submit the application using the Hadoop `jar` command:
`$HADOOP_HOME/bin/hadoop jar movie-count.jar MovieCountDriver -D mapreduce.job.reduces=2 movie_count movie_count_results`

* Running the above command again will result in a failure because HDFS is an immutable file system. Sice the output directory for a MapReduce job can't be overwritten, it can't exist when the job is launched.

* Notice a `movie_count_results` directory has been created in HDFS:
`hadoop fs -ls`

* You can view the contents using: `hadoop fs -ls movie_count_results/`

* To get the directory onto your hard drive, use:
`hadoop fs -get movie_count_results`