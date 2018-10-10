package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Converts each line in the file to a string array. The file is formatted so that the first item is a movie id followed by a comma, followed by the rating the user gave for that movie
     * We get each value and write it with the movie ID as the key and the rating as the value.
     */
	String[] line = value.toString().split(",");

	if (line.length > 0 && line[0].length() > 0 && line[2].length() > 0) {
		  String movieID = line[0];
		  int rating = (int) Double.parseDouble(line[2]);
		  context.write(new Text(movieID), new IntWritable(rating));
	}
  }
}