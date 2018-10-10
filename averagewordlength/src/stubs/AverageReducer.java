package stubs;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  
	  double letterCount = 0;
	  double wordCount = 0;
	  double averageLength = 0;
	  
	  for (IntWritable value: values) {
		  letterCount += value.get();
		  wordCount += 1;
	  }
	  
	  averageLength = letterCount/wordCount;
	  
	  context.write(new Text(key), new DoubleWritable(averageLength));
  }
}