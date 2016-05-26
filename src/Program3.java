//Please use in-memory join to answer this question.
//Given any two Users as input, output the list of the names and the zipcode of their mutual friends.
//Note: use the userdata.txt to get the extra user information.
//Output format:
//UserA id, UserB id, list of [names:zipcodes] of their mutual Friends.
//Sample Output:
//1234 4312 [John:75075, Jane : 75252, Ted:45045]

// To Execute hadoop dfs jar Program.jar Program3 /socNetData/networkdata/soc-LiveJournal1Adj.txt /axm149230_program3 <userA> <userB> /socNetData/networkdata/userdata.txt
//To check output: hdfs dfs -cat /axm149230_program3/*

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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Program3 {

	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();
			String input1 = conf.get("input1").trim();
			String input2 = conf.get("input2").trim();
			FileSystem fSystem = FileSystem.get(conf);
			Path path = new Path(conf.get("file"));	

			String line[] = value.toString().split("\t");
			String id = line[0].trim();

			if (line.length == 2){
				if(id.equals(input1) || id.equals(input2)) {
					String output = "";
					for(String friendId: line[1].split(",")){
						BufferedReader br=new BufferedReader(new InputStreamReader(fSystem.open(path)));
						String line1;
						while ((line1 = br.readLine()) != null){
							String userInfo[] = line1.split(",");
							if(userInfo[0].trim().equals(friendId.trim())){ 
								output += output.equals("") ? userInfo[1]+":"+userInfo[6] : ","+userInfo[1]+":"+userInfo[6];
							}
						}
						br.close();
					}
					context.write(new Text(input1+","+input2), new Text(output));
				}
			}
		}
	}

	public static String hashFunc(Long userId){
		String userDetails = "";

		return userDetails;
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {

			List<String> totalList = new ArrayList<String>();
			String finalList = "";
			for(Text value: values){
				List<String> list = Arrays.asList(values.toString().split(","));
				totalList.addAll(list);
			}
			Set<String> set = new HashSet<String>();
			for(String s : totalList){
				if(!set.add(s)){
					finalList = finalList.equals("") ? finalList+s : finalList+","+s;
				}
				else 
					set.add(s);

			}
			context.write(key, new Text("["+finalList+"]"));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("input1", args[2]);
		conf.set("input2", args[3]);
		conf.set("file", args[4]);

		Job job = new Job(conf, "Program3");
		job.setJarByClass(Program3.class);
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
