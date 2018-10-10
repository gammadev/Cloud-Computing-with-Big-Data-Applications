package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class IndexReducer extends Reducer<Text, Text, Text, Text> {
  
  /*
   * The reducer receives a key-value pair with the key as a specific word and the value as a list of location (filename@linenumber) strings
   * denoting each place in the data that the word was found. In the reduce function, we iterate over this list of location values and concatenate
   * them all into one string where each location is separated by a ",". We write the key-value pair as the specific word and the value as the list of
   * all the locations of the word within the data.
   */
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  String valueList = "";
	  for (Text value: values) {
		  String location = value.toString();
		  valueList += location + ",";
	  }
	  
	  context.write(key, new Text(valueList));
    
  }
}