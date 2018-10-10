package stubs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopNList extends Configured implements Tool { //Implement ToolRunner with a parameter N with a default value of 3
  public static void main(String[] args) throws Exception {
	  Configuration conf = new Configuration();
	  conf.setInt("N", 3);
	  
	  int exitCode = ToolRunner.run(conf, new TopNList(), args);
	  System.exit(exitCode);
  }

  public int run(String[] args) throws Exception {

    /*
     * The expected command-line arguments are the paths containing
     * input and output data. Terminate the job if the number of
     * command-line arguments is not exactly 2. If the N parameter is not set, then it will run with the value 3.
     */
    if (args.length != 2) {
      System.out.printf(
          "Usage: TopNList <input dir> <output dir>\n");
      System.exit(-1);
    }

    /*
     * Instantiate a Job object for your job's configuration.  
     */
    Job job = new Job(getConf());
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running
     * mapper and reducer tasks.
     */
    job.setJarByClass(TopNList.class);
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("Top N List");

    /*
     * Specify the paths to the input and output data based on the
     * command-line arguments.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Specify the mapper, partitioner, and reducer classes.
     */
    job.setMapperClass(LocalTopNListMapper.class);
    job.setReducerClass(GlobalTopNListReducer.class);
      

    /*
     * Specify that the MapOutputKeyClass will be a NullWritable to make sure that it doesnt use the job OutputKeyClass
     * Specify the job's output key and value classes.
     */
    job.setMapOutputKeyClass(NullWritable.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
}
