package stubs;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;

public class ProcessLogs extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
	  int exitCode = ToolRunner.run(new Configuration(), new ProcessLogs(), args);
	  System.exit(exitCode);
  }
  
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.out.printf("Usage: ProcessLogs <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(ProcessLogs.class);
    job.setJobName("Process Logs");
    
    /*
     * Set the first and second arguments on the command line as input/output directories respectively.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    /*
     * Set LogFileMapper as the mapper we will use to run this MapReduce job.
     * Set SumReducer from the WordCount MapReduce job to act as our reducer for this current MapReduce job. 
     */
    job.setMapperClass(LogFileMapper.class);
    job.setReducerClass(SumReducer.class);
    
    /*
     * Make the MapOutput Keys of type Text. Each key is the IP address found on each line.
     * Make the MapOutput Values of type IntWritable. Each value is the integer 1, denoting that we found 1 IP address on the line.
     */
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    /*
     * Make the Output Keys of type Text. Each key is a unique IP address found within weblog.
     * Make the Output Value of type IntWritabale. Each value is the number of times the unique IP key was found in the data.
     */
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
}
