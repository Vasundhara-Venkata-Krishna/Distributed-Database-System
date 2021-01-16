import java.util.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.*;

import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Context;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.*;

public class equijoin
{
	public static class EquijoinMapper extends  Mapper <Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context con) throws IOException, InterruptedException {
	
			String input = value.toString();
			String metadata[] = input.split(", ");
			String key_val = metadata[1].trim();
			int metadata_size = metadata.length;
			StringJoiner rows = new StringJoiner("");
			for(int i = 0; i < metadata_size; i++)
			{
				rows.add(metadata[i]);
				rows.add(",");
			}
			con.write(new Text(key_val), value);
		}
	}


	public static class EquijoinReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
		{
			Text finalView = new Text();
			String first_Index = "";
			String second_Index = "";
			int i=0;
			ArrayList <String> f_i = new ArrayList<>();
			ArrayList <String> s_i = new ArrayList<>();
			for(Text value : values) {
				String metadata1[] = value.toString().split(",");
				if(i==0) {
					first_Index = metadata1[0];
					f_i.add(value.toString());
					i=1;
					continue;
				}
				if(!first_Index.equals(metadata1[0])) {
					second_Index = metadata1[0];
					s_i.add(value.toString());
				}
				else {
					f_i.add(value.toString());
				}
				i++;
			}
			String view = "";
			if(f_i.size()!=0 && s_i.size()!=0) {
				for(int x =0 ; x < f_i.size(); x++)
				{
					String temp_1 = f_i.get(x);  
					for(int y =0 ; y < s_i.size(); y++)
					{
						String temp_2 = s_i.get(y);
						view = temp_1 + ", " + temp_2;
						finalView.set(view);
						Text empty1 = new Text();
						String emp = "";
						empty1.set(emp);		  
						con.write(empty1, finalView); 
    
					}  
				}
			}
		}
	}

	public static void main(String[] args) throws Exception
	{
	    Configuration conf = new Configuration();
	    Job job_conf = Job.getInstance(conf, "equijoin");
	    job_conf.setJarByClass(equijoin.class);
	    job_conf.setMapOutputKeyClass(Text.class);
	    job_conf.setMapOutputValueClass(Text.class);
	    job_conf.setMapperClass(EquijoinMapper.class);
	    job_conf.setReducerClass(EquijoinReducer.class);
	    job_conf.setOutputKeyClass(Text.class);
	    job_conf.setOutputValueClass(Text.class);
	    FileInputFormat.setInputPaths(job_conf, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job_conf, new Path(args[2]));
	    System.exit(job_conf.waitForCompletion(true) ? 0 : 1);
	}
}