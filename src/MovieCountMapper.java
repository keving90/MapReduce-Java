/*
 * MovieCount MapReduce program by Kevin Geiszler
 *
 * This program uses MapReduce to count the number of movies each user in the Movielens
 * ml-100k dataset has rated. This is the Mapper file. There is also a Driver and
 * Reducer file. The results are spread throughout two text files. The first column
 * displays the user's ID, and the second column shows how many movies they have rated.
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*
 * The Mapper class is used to map input key-value pairs to a set of intermediate
 * key-value pairs, which are used in the reduce phase. The MovieCountMapper class 
 * is a child class of Mapper.
 *
 * The generic form of Mapper is: Mapper<InputKey, InputValue, OutputKey, OutputValue>
 *
 * Here, we are overriding the map() method. It is called once for each key/value
 * pair in the input split.
 */

public class MovieCountMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

  // We are counting the number of movies each user has rated, therefore the ouput
  // key-value pair will be a unique userID (moveieObject) paired with one or more ones
  // (one)for every movie they have rated.
  private final static IntWritable one = new IntWritable(1);
  private IntWritable movieObject = new IntWritable();
  
  @Override
  public void map(Object key, Text value, Context context)
                              throws IOException, InterruptedException {
    // Convert the record to String
    String record = value.toString();

    // Split the line into a String array. The data is tab-delimited.
    String[] splitRecord = record.split("\t");

    // We want each unique key to be the userID, which is the first element of the 
    // String array.
    String userID = splitRecord[0];

    // Set the userID as the output key's text.
    movieObject.set(Integer.parseInt(userID));

    // Write the key-value pair.
    context.write(movieObject, one);
  }
}

