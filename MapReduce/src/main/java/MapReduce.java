import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author Jon Cobi
 * Map Reduce Project for CS417
 */
public class MapReduce {
	
	public static class WordMapper extends Mapper<Object, Text, Text, IntWritable>{
		/**
		 * Receives a line of text and parses it
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String values[] = value.toString().split("[\t]");
			Text imageID = new Text();
			int score = Integer.parseInt(values[2]) + Integer.parseInt(values[3]) + Integer.parseInt(values[4]); //only need upvotes, downvotes, and comments
			imageID.set(values[1]);
			context.write(imageID, new IntWritable(score)); //map imageID to score
		}
	}
	
	public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		/**
		 * Combine the scores for each image
		 */
		@Override
		public void reduce(Text imageID, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int score = 0;
			for(IntWritable val : values) {
				score += val.get();
			}
			context.write(imageID, new IntWritable(score));
			System.out.println(imageID.toString() + ": " + score);
		}
	}
	
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		System.setProperty("hadoop.home.dir", "D:\\Downloads\\winutils-master\\winutils-master\\hadoop-2.8.3");
		
		Configuration conf = new Configuration();
		
		if(args.length != 2) {
			System.err.println("Usage: MapReduce <input_file> <output_directory>");
			System.exit(2);
		}
		
		Job job = Job.getInstance(conf, "Map Reduce");
		job.setJarByClass(MapReduce.class);
		job.setMapperClass(WordMapper.class);
		job.setCombinerClass(WordReducer.class);
		job.setReducerClass(WordReducer.class);
//		job.setNumReduceTasks(0); //changeable 0 works
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		try {
			boolean status = job.waitForCompletion(true);
			if(status) {
				System.out.println("Success");
				System.exit(0);
			}else {
				System.out.println("Failure");
				System.exit(1);
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
