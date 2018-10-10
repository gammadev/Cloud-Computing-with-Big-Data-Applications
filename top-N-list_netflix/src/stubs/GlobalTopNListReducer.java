package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import java.util.Collections;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
  
public class GlobalTopNListReducer extends Reducer<NullWritable, Text, IntWritable, Text> {
  
  /*
   * N is a global variable that will be set to the N parameter passed through.
   * topNitems is a sorted map that has key-value pairs corresponding to the sum of the ratings and the movie id sorted by the key value in descending order (the sum of the ratings)
   * movieTitles is a sorted map that has the movie id as the key and the title of the movie as the value
   */
  int N;
  SortedMap<Integer, String> topNitems = new TreeMap<Integer, String>(Collections.reverseOrder());
  SortedMap<Integer, String> movieTitles = new TreeMap<Integer, String>();
  
  public void setup(Context context) {
	  Configuration conf = context.getConfiguration(); //Get the configuration object that was passed through to the driver
	  N = conf.getInt("N", 3); //Get the int N parameter which denotes the number of top items from the configuration that was passed through to the driver
	  try {
		  
		  /*
		   * Movie_titles file is opened and then read using a FileReader and the output is passed through
		   * a buffered reader. Then, it is read line by line to get the id and title of each movie.
		   * When the line is converted into an array, the first element is the movie id, the second is the year, and the third is the movie title.
		   * We get the movie id and the title and add those as a key-value pair respectively in the movieTitles map
		   */
		  File movieTitlesFile = new File("movie_titles.txt");
		  
		  FileReader fr = new FileReader(movieTitlesFile);
		  BufferedReader br = new BufferedReader(fr);
		  
		  String line;
		  while((line = br.readLine()) != null) {
			  String[] lineArray = line.split(",");
			  int movieID = Integer.parseInt(lineArray[0]);
			  String title = lineArray[2];
			  
			  movieTitles.put(movieID, title);
          }
	  } catch(IOException e) {
		  e.printStackTrace();
	  }
  }
  
  @Override
	public void reduce(NullWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		/*
		 * Parse through the ratingSum,movie_id values in the iterable and format each to be an int and String respectively.
		 * Add a new key-value pair into the topNitems map with the ratingSum as the key and the original String pair value as the value in order to sort by ratingSum.
		 * If the new size of the map is greater than the N parameter we passed in, then remove the firstKey (which is the smallest key) 
		 */
		
		for (Text value : values) {
			String pair = value.toString();
			String[] pairArray = pair.split(",");
			
			int ratingSum = Integer.parseInt(pairArray[0]);
			String movieID = pairArray[1];
			topNitems.put(ratingSum, pair);
			
			if (topNitems.size() > N) {
				topNitems.remove(topNitems.firstKey());
			}
		}
		
		/*
		 * Iterate over the values in the topNitems map from largest key to smallest and format the ratingSum and MovieID into ints.
		 * The movieID from the topNitems is then matched to the corresponding key in the movieTitles map to obtain the title of the movie that corresponds to it.
		 * Write with the ratingSum as the key and the title of the movie as the value. These items will be listed in descending order from the movie with the 
		 * largest rating sum to the one with the smallest 
		 */
		
		for (String pair: topNitems.values()) {
			String[] item = pair.split(",");
			int ratingSum = Integer.parseInt(item[0]);
			int movieID = Integer.parseInt(item[1]);
			
			String title = movieTitles.get(movieID);
			context.write(new IntWritable(ratingSum), new Text(title));
		}
	}
}