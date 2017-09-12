import org.apache.hadoop.io.*;
import java.io.IOException;
import org.apache.hadoop.mapreduce.Mapper;

public class TV_setsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {




public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
 
  
	String tokens[] = value.toString().split("\\|");
	
	if(tokens[0].equals("NA") || tokens[1].equals("NA")){
		// Do Nothing
	
	}
	else if(tokens[0].equals("Onida")){
		
		
	   
		context.write(new Text(tokens[3]), new IntWritable(1));
       		
	}
	
		 
	}

 

}
  