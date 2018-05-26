/*
 * MovieCount MapReduce program by Kevin Geiszler
 *
 * This program uses MapReduce to count the number of movies each user in the Movielens
 * ml-100k dataset has rated. This is the Reducer file. There is also a Driver and
 * Mapper file. The results are spread throughout two text files. The first column
 * displays the user's ID, and the second column shows how many movies they have rated.
 */

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * The Reduce class reduces a set of intermediate values sharing a key to a smaller 
 * set of values. In this case, a userID may have one or more values (each of a value
 * one). The reducer will sum all of these ones (simulating a count), and use the
 * result as the final output value for the key. The MovieCountReducer class is a child
 * class of Reducer.
 *
 * The generic form of Reducer is: Reducer<InputKey, InputValue, OutputKey, OutputValue>
 *
 * Here, we are overriding the reduce() method. It sums (counts) the values corresponding
 * to the key. Each value is one, so the sum is basically a count. The resulting key-value
 * pair is the final output.
 */

public class MovieCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
  private IntWritable movieCountWritable = new IntWritable();

  @Override
  public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                                            throws IOException, InterruptedException {
    int movieCount = 0;

    // Get each value for the corresponding key and add it to the total (movieCount).
    for (IntWritable value : values) {
      movieCount += value.get();
    }

    // Set the total value.
    movieCountWritable.set(movieCount);

    // Write the final key-value pair.
    context.write(key, movieCountWritable);
  }
}
