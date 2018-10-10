package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  /*
	   * Converts each line in the document into a string and the splits it into an array, using whitespace as delimeter.
	   */
	  String[] line = value.toString().split("\\s+");
	  
	  /*
	   * Making sure that the length of the line array was split properly and that the first element is not empty.
	   * The first element of the array should be the IP address, based on the standard structure of the weblog data.
	   * Write this IP address as the key in format Text and give it a value of 1 in order to sum the count later on.
	   */
	  if (line.length > 0 && line[0].length() > 0) {
		  String address = line[0];
		  context.write(new Text(address), new IntWritable(1));
	  }
  }
}