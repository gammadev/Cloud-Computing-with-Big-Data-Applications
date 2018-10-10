package stubs;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {
	
  /*
   * Each mapper reads in the data which is in the form of Text key-value pairs where the key is the line number,
   * which is always included even if there is no other text on the line, and the value is the text of the line.
   * In the map function, get the text of the line and convert to a string. If it is not null, then split it by words
   * to eliminate other characters. Next, get the name of the file that is being read through an input split and getting the Path
   * and name of the file. Convert each word in the line to lower case and create a location string by concatenating the filename, and @ symbol,
   * and the line number. Finally, write each word as a key and its location string (filename and linenumber) as the value. 
   */
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
	  
	  String line = value.toString();
	  
	  if (line!= null) {
		  for (String word: line.split("\\W+")) {
			  if (word.length() > 0) {
				  String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
				  String w = word.toLowerCase();
				  String val = fileName + "@" + key;
				  context.write(new Text(w), new Text(val));
			  }
		  }
	  }
    
  }
}