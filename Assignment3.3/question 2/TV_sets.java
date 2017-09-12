import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TV_sets {

	public static void main(String[] args) throws Exception {
  if (args.length != 2 ){ 
	  System.err.println("usage : TV_sets <input path> <output path>") ;
	  System.exit(-1);
 }
  Configuration conf =new Configuration(); 
  @SuppressWarnings("deprecation")
Job job = new Job(conf, " my TV-sets");
  job.setJarByClass(TV_sets.class);
  FileInputFormat.setInputPaths(job, args[0]);
  Path outputPath = new Path(args[1]);
  FileOutputFormat.setOutputPath(job, outputPath);
  outputPath.getFileSystem(conf).delete(outputPath, true);
  job.setMapperClass(TV_setsMapper.class);
  job.setReducerClass(TV_setsReducer.class);
  job.setInputFormatClass(TextInputFormat.class);
  job.setOutputFormatClass(TextOutputFormat.class);
  job.setMapOutputKeyClass(Text.class);
  job.setMapOutputValueClass(IntWritable.class);
  job.setOutputValueClass(IntWritable.class);
  job.setOutputKeyClass(Text.class);
  //job.setNumReduceTasks(1);
  //execute the job
  System.exit(job.waitForCompletion(true) ? 0 :1);
  
  
  
  
  
	}

}
