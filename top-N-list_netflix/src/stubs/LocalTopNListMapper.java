package stubs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class LocalTopNListMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
  
  /*
   * N is the parameter that was passed into to denote the number of top items that we want
   * topNitems is the map that contains the top N items with the sum of the Ratings as the key and the movie ID as the value
   */
  int N;
  SortedMap<Integer, String> topNitems = new TreeMap<Integer, String>();
  
  public void setup(Context context) {
	  Configuration conf = context.getConfiguration(); //Get the configuration object that was passed through to the driver
	  N = conf.getInt("N", 3); //Get the int N parameter from the configuration that was passed through to the driver
  }
  
  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line in the output file into a string. The output file should be formatted with the movieID followed by a space and followed by the rating sum on each line
     * If the line and the first and second elements in it as an array are not empty, then proceed to get the movieID and ratingSum
     * Concatenate each into a String with a , to separate each value
     * Add it into the topNitems map with the ratingSum as the key and the new String as the value
     * If the size of the map is greater than our N parameter, remove the key-value pair with the smallest rating sum key
     */
	String[] line = value.toString().split("\\W+");

	if (line.length > 0 && line[0].length() > 0 && line[1].length() > 0) {
	  String movieID = line[0];
	  int ratingSum = (int) Double.parseDouble(line[1]);
	  
	  String pair = ratingSum + "," + movieID;
	  topNitems.put(ratingSum, pair);
	  
	  if (topNitems.size() > N) {
		  topNitems.remove(topNitems.firstKey());
	  }
	}
  }
  
  /*
   * Runs last. Writes all of the topNitems values (the ratingSum,movieid Strings) with a null key.
   */
  public void cleanup(Context context) throws IOException, InterruptedException {
	  for (String pair: topNitems.values()) {
		  context.write(NullWritable.get(), new Text(pair));
	  }
  }
}