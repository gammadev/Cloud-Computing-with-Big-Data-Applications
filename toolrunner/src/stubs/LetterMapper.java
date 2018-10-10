package stubs;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  boolean caseSensitive;
  
  public void setup(Context context) { //
	  Configuration conf = context.getConfiguration(); //Get the configuration object that was passed through to the driver
	  caseSensitive = conf.getBoolean("caseSensitive", true); //Get the boolean parameter from the configuration that was passed through to the driver
  }
  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String line = value.toString();
	  
	  if (!caseSensitive) { //If the caseSensitive parameter was set to false, then change the case of each line to lowercase.
		  line = line.toLowerCase();
	  }
	  
	  for (String word: line.split("\\W+")) {
		  if (word.length() > 0) {
			  context.write(new Text(word.substring(0,1)), new IntWritable(word.length()));
		  }
	  }

  }
}
