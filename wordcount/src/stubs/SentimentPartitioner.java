package stubs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();


  @Override
  public void setConf(Configuration configuration) {
	  this.configuration = configuration; //Instantiates the configuration to the driver configuration
	  try {
		  
		  /*
		   * Each word file is opened and then read using a FileReader and the output is passed through
		   * a buffered reader. Then, each file is read line by line since each word exists on a new line.
		   * If that line does not start with a semicolon (It is a comment if it does), then add that line to the
		   * respective set.
		   */
		  File positiveWords = new File("positive-words.txt"); //Open the positive-words.txt file
		  File negativeWords = new File("negative-words.txt"); //Open the negative-words.txt file
		  
		  FileReader pr = new FileReader(positiveWords); //Read the positive-words.txt file using a file reader
		  BufferedReader pbr = new BufferedReader(pr); //Open the file reader with a buffered reader
		  
		  String line;
		  while((line = pbr.readLine()) != null) { //While there are lines to read from the positive-words.txt file, add each line
              if (line.substring(0,1) != ";") {    //from the positive-words.txt file (each word is on a new line) if it is a word into the positive set
            	 positive.add(line);
              }
          }
		  
		  FileReader nr = new FileReader(negativeWords); //Same as above
		  BufferedReader nbr = new BufferedReader(nr); 
		  
		  while((line = nbr.readLine()) != null) {
			  if (line.substring(0, 1) != ";") {
				  negative.add(line);
			  }
		  }
		  
	  } catch(IOException e) {
		  e.printStackTrace();
	  }
	  
  }

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You need to implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
	  
	  /*
	   * Check to see if the current key is contained in the positive or negative set. If it is in the positive set,
	   * then return a 0 to use reducer 0 for that key-value pair. If it is in not in the positive set but it is in the negative set, return 1 and user reducer 1.
	   * If it is not in either, then return 2 and use reducer 2.
	   */
	  if (positive.contains(key.toString())) { //
		  return 0;
	  } 
	  else if (negative.contains(key.toString())) {
		  return 1;
	  }
	  else {
		  return 2;
	  }
  }
}
