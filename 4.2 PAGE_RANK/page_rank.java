package PAGE_RANK;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.FileSystem;


public class page_rank extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		int res = ToolRunner.run(new Configuration(),
				new page_rank(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Job job = new Job(getConf(), "page_rank");

		job.setJarByClass(page_rank.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
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

	
	public static HashMap<String,Integer> links_out = new HashMap<String,Integer>();
	public static HashMap<String,Double> page_rank = new HashMap<String,Double>();
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String[] val = value.toString().split(";");
			
			String from = val[0];
			String to = val[1];
			
			// number of links which go out from a page
			if(links_out.containsKey(from)) {
				links_out.put(from, links_out.get(from)+1);
			}
			else {
				links_out.put(from,1);	
			}
			
			// page rank of a page A, initialize at 1 - 0.85 = 0.15
			page_rank.put(to,0.15);
			
			// page A has pages T1, T2, ..., Tn which point to it
			context.write(new Text(to), new Text(from));	 
		}
	}
	
	

	public static HashMap<String,Double> new_page_rank = new HashMap<String,Double>();
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterable<Text> value, Context context) 
				throws IOException, InterruptedException {
			
			double sum = 0.0;
			for (Text val : value){
				String from = val.toString();
				if(page_rank.containsKey(from)) {
					sum = (double) sum + ( page_rank.get(from) / links_out.get(from) );
				}
			}
			
			String page = key.toString();
			new_page_rank.put(page, (1-0.85) + 0.85*sum );
			String page_rank_value = String.format("%.12f",new_page_rank.get(page));
			
			context.write(key,new Text(page_rank_value));
			
		}
	}
	
}