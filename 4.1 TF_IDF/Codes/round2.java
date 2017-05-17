package TF_IDF;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class round2 extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new round2(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "round2");

		job.setJarByClass(round2.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Combine.class);
		job.setNumReduceTasks(1);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.newInstance(getConf());
		if (fs.exists(new Path(args[1]))) {
			fs.delete(new Path(args[1]), true);
		}

		job.waitForCompletion(true);

		return 0;
	}

	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] val = value.toString().split(";");
			String ID = val[0].split("/")[1];
			String word = val[0].split("/")[0];
			String count = val[1];
			String word_count = word + "/" + count;
			
			context.write(new Text(ID), new Text(word_count));	
		}
	}

	
	public static HashMap<String,String> Words_per_Doc = new HashMap<String, String>();
	
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		
		@Override
	    public void reduce(Text key, Iterable<Text> value, Context context)
	            throws IOException, InterruptedException {
	    	
			int sum = 0;
			for (Text val : value){
				String[] word_count = val.toString().split("/");
				sum += Integer.valueOf(word_count[1]);
				
				context.write(key,val);
			}
			String ID = key.toString();
			Words_per_Doc.put(ID,String.valueOf(sum));
		}
	}
	
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			
			for (Text val : value){
				
				String word = val.toString().split("/")[0];
				String count = val.toString().split("/")[1];
				String ID = key.toString();
				String words_per_doc = Words_per_Doc.get(ID);
				String word_doc = word + "/" + ID;
				String wordCount = count + "/" + words_per_doc;
				
				context.write(new Text(word_doc), new Text(wordCount));
			}
		}
	}
	
}