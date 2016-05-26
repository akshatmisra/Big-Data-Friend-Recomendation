//to run => hadoop jar Program.jar Program1 /socNetData/soc-LiveJournal1Adj.txt /axm149230_program1
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

public class Program1 {
	
    public static class Map extends Mapper<LongWritable, Text, LongWritable, Text> 
    {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String line[] = value.toString().split("\t");
            Long userID = Long.parseLong(line[0]);
            List<Long> friendList = new ArrayList<Long>();

            if (line.length == 2) 
            {
            	String [] list = line[1].split(",");
            	
            	for(String s : list)
            	{
            		Long toUser = Long.parseLong(s);
            		friendList.add(toUser);
            		context.write(new LongWritable(userID), new Text(toUser +","+ -1L));// emit each user friend pair
            	}

            	// emit all pairs of mutual friends
                for (int i = 0; i < friendList.size(); i++) 
                {
                    for (int j = i + 1; j < friendList.size(); j++) 
                    {
                        context.write(new LongWritable(friendList.get(i)), new Text((friendList.get(j))+","+ userID)); 
                        context.write(new LongWritable(friendList.get(j)), new Text((friendList.get(i))+","+ userID));
                    }
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, Text, LongWritable, Text> 
    {   
        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            // key is the recommended friend, and value is the list of mutual friends
        	final HashMap<Long, List<Long>> mutualFriends = new HashMap<Long, List<Long>>();

            for (Text val : values) 
            {
            	String [] str = val.toString().split(",");
            	Long toUser = Long.parseLong(str[0]);
            	final Long mutualFriend = Long.parseLong(str[1]);
                
                Boolean isAlreadyFriend = (mutualFriend == -1);
                
                if (mutualFriends.containsKey(toUser)) 
                {
                    if (isAlreadyFriend) 
                    {
                        mutualFriends.put(toUser, null);
                    } 
                    else if (mutualFriends.get(toUser) != null) 
                    {
                        mutualFriends.get(toUser).add(mutualFriend);
                    }
                } 
                else 
                {
                    if (!isAlreadyFriend) 
                    {
                    	ArrayList<Long> mutualFriendList = new ArrayList<Long>();
                    	mutualFriendList.add(mutualFriend);
                        mutualFriends.put(toUser, mutualFriendList);
                        
                    } 
                    else 
                    {
                        mutualFriends.put(toUser, null);
                    }
                }
            }

            java.util.SortedMap<Long, List<Long>> sortedMutualFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>() 
            {
                
                public int compare(Long key1, Long key2) 
                {
                    Integer size1 = mutualFriends.get(key1).size();
                    Integer size2 = mutualFriends.get(key2).size();
                    if (size1 > size2) 
                    {
                        return -1;
                    } 
                    else if (size1.equals(size2) && key1 < key2) 
                    {
                        return -1;
                    } 
                    else 
                    {
                        return 1;
                    }
                }
            });

            for (java.util.Map.Entry<Long, List<Long>> entry : mutualFriends.entrySet()) 
            {
                if (entry.getValue() != null) 
                {
                    sortedMutualFriends.put(entry.getKey(), entry.getValue());
                }
            }

            Integer i = 0;
            String output = "";
            
            for (java.util.Map.Entry<Long, List<Long>> entry : sortedMutualFriends.entrySet()) 
            {
                if (i == 0) 
                {
                    output = entry.getKey().toString() + " (" + entry.getValue().size() + ": " + entry.getValue() + ")";
                } 
                else 
                {
                    output += "," + entry.getKey().toString() + " (" + entry.getValue().size() + ": " + entry.getValue() + ")";
                }
                ++i;
            }
            context.write(key, new Text(output));
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        
        String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();
		
		// get all args
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: Progarm1 <soc.txt> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, "Program1");
        job.setJarByClass(Program1.class);
        job.setOutputKeyClass(LongWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem outFs = new Path(args[1]).getFileSystem(conf);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
    
}
