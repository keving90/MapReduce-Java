/*
 * MovieCount MapReduce program by Kevin Geiszler
 *
 * This program uses MapReduce to count the number of movies each user in the Movielens
 * ml-100k dataset has rated. This is the Driver file. There is also a Mapper and
 * Reducer file. The results are spread throughout two text files. The first column
 * displays the user's ID, and the second column shows how many movies they have rated.
 */

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/* 
 * The MovieCountDriver class specifies various facets of the job, such as input/output
 * paths, key-value types, and more.
 * 
 * The Tool interface only contains run(). It is a method used to interpret command-line
 * arguments.
 *
 * The Configured class is used to Configure a job. Configured implements the Configurable
 * interface. You can use setConf() and getConf() to set a job configutation or get a job
 * configuration respectively. getConf() return stype Configuration.
 */

public class MovieCountDriver extends Configured implements Tool {
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.printf("Usage: %s [generic options] <inputdir> <outputdir>\n", getClass().getSimpleName());
      return -1;
    }

    // Create a new job based on the configuration
    Job job = new Job(getConf());

    job.setJarByClass(MovieCountDriver.class);
    job.setJobName("Movie Count");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new  Path(args[1]));

    job.setMapperClass(MovieCountMapper.class);
    job.setReducerClass(MovieCountReducer.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new MovieCountDriver(), args);
    System.exit(exitCode);
  }
}

