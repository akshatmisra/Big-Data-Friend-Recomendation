//Please answer this question by using dataset from Q1.
//Given any two Users as input, output the list of the user id of their mutual friends.
//Output format:
//UserA, UserB list userid of their mutual Friends.

// To run the program => hadoop jar Program2.jar Program2 /socNetData/soc-LiveJournal1Adj.txt /axm149230_program2 0 12
// To check the out put => hdfs dfs -cat /axm149230_program2/*

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class Program2 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			Configuration conf = context.getConfiguration();
			Long usr1 = Long.parseLong(conf.get("usr1").trim());
			Long usr2 = Long.parseLong(conf.get("usr2").trim());
			
			
			String line[] = value.toString().split("\t");

			Long userID = Long.parseLong(line[0].trim());
			if (line.length == 2 && (userID== usr1 || userID== usr2)) {
				context.write(new Text(usr1+","+usr2), new Text(line[1].trim()));
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> 
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			List<Long> arr = new ArrayList<Long>();
			String mutualFrnds = "";
			for (Text value : values) {

				String [] vals = (value.toString().split(","));
				for(int i=0 ; i< vals.length ; i++){
					Long id = Long.parseLong(vals[i].trim());
					if(arr.contains(id)){
						if(mutualFrnds.equals(""))
							mutualFrnds += id;
						else
							mutualFrnds += ","+id;
					}
					else
						arr.add(id);
				}

			}
			context.write(key, new Text(mutualFrnds));
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		conf.set("usr1", args[2]);
		conf.set("usr2", args[3]);
		String[] Args = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (Args.length != 4) 
		{
			System.err.println("Usage: Program2 <soc.txt> <out> <UsearA ID> <UserB ID>");
			System.exit(2);
		}

		Job job = new Job(conf, "MutualFriends");
		job.setJarByClass(Program2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileSystem outFs = new Path(args[1]).getFileSystem(conf);
		outFs.delete(new Path(args[1]), true);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		

		job.waitForCompletion(true);
	}
}
