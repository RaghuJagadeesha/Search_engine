package org.length;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class ComputeLength {
	
	public static class ComputeMap extends Mapper<Text, BytesWritable, IntWritable, LongWritable> {
		@Override
		protected void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String st1 = XmlParser.parse(value, key);
			StringTokenizer stk = new StringTokenizer(st1," \t\n!,\"");
			long len_d = stk.countTokens();
			context.write(new IntWritable(1),new LongWritable(len_d));
		}
	}
	public static class ComputeReduce extends Reducer<IntWritable, LongWritable, Text, Text> {
		@Override
		public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			int nfiles= 0;
			long total = 0;
			for (LongWritable val : values) {
				total += Long.parseLong(val.toString());
				nfiles++;
			}
			int average = (int)total/nfiles;
			context.write(new Text(String.valueOf(nfiles)), new Text(String.valueOf(average)));
		} 
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf);
		job.setJarByClass(ComputeLength.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(ComputeLength.ComputeMap.class);
		job.setReducerClass(ComputeLength.ComputeReduce.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setInputFormatClass(ZipFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}