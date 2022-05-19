package PackageDemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class LogProcess {
	public static void main(String [] args) throws Exception
	{
		Configuration c=new Configuration();
		String[] files=new GenericOptionsParser(c,args).getRemainingArgs();
		Path input=new Path(files[0]);
		Path output=new Path(files[1]);
		Job j=new Job(c,"wordcount");
		j.setJarByClass(LogProcess.class);
		j.setMapperClass(MapForLogProcess.class);
		j.setReducerClass(ReduceForLogProcess.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(j, input);
		FileOutputFormat.setOutputPath(j, output);
		System.exit(j.waitForCompletion(true)?0:1);
	}
	
	public static class MapForLogProcess extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
		{
			String line = value.toString();
			String[] words = line.split(",");
			String user_ip = words[2];
			
			Text outputKey = new Text(user_ip);
			IntWritable outputValue = new IntWritable(1);
			con.write(outputKey, outputValue);	
				
		}
	}
	
	public static class ReduceForLogProcess extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		
		public void reduce(Text user_ip, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException
		{
			int sum=0;
				for(IntWritable value: values){
					sum+= value.get();
				}
				
				con.write(user_ip, new IntWritable(sum));
			
		}
	}
}
