package stubs;

import java.io.IOException;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  /*
	   * Split the line into an array by words. Each word will be an array element.
	   */
	  String[] line = value.toString().split("\\W+");
	  String first_word = "";
	  String second_word = "";
	  
	  /*
	   * For each element in the array, up to the last one (since the last word will never have a pair where it is the first word), get the
	   * first element and one element ahead. If these words are not empty, then make them lowercase and add concatenate them with a "," as a separator to show that they are a pair
	   * Then, write them to the program as a Text key with a value of 1
	   */
	  for (int i = 0; i < line.length - 1; i++) {
		  first_word = line[i];
		  second_word = line[i + 1];
		  
		  if (first_word.length() > 0 && second_word.length() > 0) {
			  first_word = first_word.toLowerCase();
			  second_word = second_word.toLowerCase();
			  String word_pair = first_word + "," + second_word;
			  
			  context.write(new Text(word_pair), new IntWritable(1));
		  }
	  }
  }
}
