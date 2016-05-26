//Using reduce-side join and job chaining:
//Step 1: Calculate the average age of the direct friends of each user.
//Step 2: Sort the users by the calculated average age from step 1 in descending order.
//Step 3. Output the top 20 users from step 2 with their address and the calculated average age.
//Sample output.
//
//User A, 1000 Anderson blvd, Dallas, TX, average age of direct friends.

// To run the Program => 
//hadoop jar Program4.jar Program4 /socNetData/userdata.txt /socNetData/soc-LiveJournal1Adj.txt /axm149230_program4 /axm149230_program4_final

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Program4 
{
	public static class Map1 extends Mapper<LongWritable, Text, Text, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] usrlist = value.toString().split("\t");
			if(usrlist.length == 2)
			{
				String[] frndlist = usrlist[1].split(",");
				for(String frnd : frndlist)
				{
					context.write(new Text(usrlist[0]), new Text(frnd));//emit userid and its friend
				}    
			}
		}
	}

	public static class Reduce1 extends Reducer<Text,Text,Text,Text> 
	{
		private HashMap<String, String> userInfo = new HashMap<String, String>();

		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);

			Configuration con = context.getConfiguration();
			String usrpath = con.get("userdata");
			Path part=new Path(usrpath);

			FileSystem f = FileSystem.get(con);
			FileStatus[] fs = f.listStatus(part);
			for(FileStatus status: fs)
			{
				Path p = status.getPath();

				BufferedReader br = new BufferedReader(new InputStreamReader(f.open(p)));
				String line;
				line=br.readLine();
				while (line != null)
				{
					String[] arr= line.split(",");
					int yearofBirth = Integer.parseInt((arr[9].split("/"))[2]); // Last entry gives the year of birth
					int age = 2016 - yearofBirth;
					StringBuilder userdata = new StringBuilder();
					userdata.append(arr[3]+","+arr[4]+","+arr[5]+","+Integer.toString(age));
					userInfo.put(arr[0], userdata.toString());

					line=br.readLine();
				}
			}
		}

		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException 
		{
			int avgAge = 0;
			int numberofusers = 0;
			String[] adressData = userInfo.get(key.toString()).split(",");
			String fullAddress = adressData[0]+","+adressData[1]+","+adressData[2];
			for(Text val:values)
			{
				avgAge += Integer.parseInt((userInfo.get(val.toString()).split(","))[3]);//get the age from user Info
				numberofusers++;
			}
			int average = avgAge / numberofusers;
			context.write(key, new Text(fullAddress+","+Integer.toString(average)));
		}
	}

	public static class Map2 extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
		{
			String[] userInfo = value.toString().split("\t");
			IntWritable age = new IntWritable(0);
			if(userInfo.length == 2)
			{
				String[] data = userInfo[1].split(",");
				if(data.length == 4) //
				{
					age = new IntWritable(Integer.parseInt(data[3]));
					context.write(age, new Text(userInfo[0]+","+data[0]+","+data[1]+","+data[2]));
				}                
			}
		}
	}

	public static class Reduce2 extends Reducer<IntWritable,Text,Text,Text>
	{
		private ArrayList<String> finalResult = new ArrayList<String>();

		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			for (Text val:values)
			{
				String[] value = val.toString().split(",");
				String str;
				
				if(value.length ==1)
				{
					str = value[0]+"#"+key.toString();
					finalResult.add(str);
				}
				else if (value.length ==2)
				{
					str = value[0]+"#"+value[1]+","+","+key.toString();
					finalResult.add(str);
				}
				else if(value.length ==3)
				{
					str = value[0]+"#"+value[1]+","+value[2]+","+key.toString();
					finalResult.add(str);
				}
				else
				{
					str = value[0]+"#"+value[1]+","+value[2]+","+value[3]+","+key.toString();
					finalResult.add(str);
				}
				
			}
		}

		protected void cleanup(Reducer<IntWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException 
		{
			super.cleanup(context);

			for(int i=finalResult.size()-1;i>(finalResult.size()-1-20);i--) // to get the top 20 from the list
			{
				String[] res = finalResult.get(i).split("#");
				context.write(new Text(res[0]), new Text(res[1])); // emit only the top 20 
			}
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration con = new Configuration();
		String[] Args = new GenericOptionsParser(con, args).getRemainingArgs();
		// getting all the args from command prompt
		if (Args.length != 4) 
		{
			System.err.println("Usage: Program4 <userdata.txt> <soc.txt> <Intermediate out> <Final out>");
			System.exit(2);
		}

		con.set("userdata", Args[0]);// get the data from Userdata.txt

		//Job 1
		Job job = new Job(con, "Program4"); 

		job.setJarByClass(Program4.class); 
		job.setMapperClass(Map1.class);
		job.setReducerClass(Reduce1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(Args[1]));

		FileOutputFormat.setOutputPath(job, new Path(Args[2])); // Intermediate output path

		job.waitForCompletion(true);

		//Job 2
		Job job2 = new Job(con, "Program4"); 
		job2.setJarByClass(Program4.class); 
		job2.setMapperClass(Map2.class);
		job2.setReducerClass(Reduce2.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(Args[2])); // Intermediate output will be input for this job
		FileOutputFormat.setOutputPath(job2, new Path(Args[3]));// final out put
		System.exit(job2.waitForCompletion(true) ? 0 : 1);


	}
}