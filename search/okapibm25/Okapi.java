package org.Probabilistic;

import java.io.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Okapi extends Configured implements Tool {

	public static class BM25Map extends Mapper<LongWritable, Text, Text, Text> {

		private final static Text term = new Text();
		private final static Text data = new Text();
		private int N;
		private double k1;
		private double b;
		private long Lave;

		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			// hardcoded or set it in the jobrunner class and retrieve via this
			// key

			k1 = Double.parseDouble(conf.get("Value of k1"));
			b = Double.parseDouble(conf.get("Value of b"));
			// System.out.println(k1);
			// System.out.println(b);
			String location = conf.get("job.Lave.path");

			if (location != null) {
				BufferedReader br = null;
				try {
					FileSystem fs = FileSystem.get(conf);
					Path path = new Path(location);
					if (fs.exists(path)) {
						FSDataInputStream fis = fs.open(path);
						br = new BufferedReader(new InputStreamReader(fis));
						String line = null;
						int i = 0;
						while ((line = br.readLine()) != null
								&& line.trim().length() > 0) {
							N = Integer.parseInt(line.trim().split("\\t")[0]);
							Lave = Long.parseLong(line.trim().split("\\t")[1]);
							i++;
						}
					}

				} catch (IOException e) {
					// handle
				}
			}
		}

		public static double tfweight(int tf) {
			return (1 + Math.log10(tf));
		}

		public static double idfweight(int df, int N) {
			return (Math.log10(N / df));
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper.Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String param = conf.get("Query");

			String document = value.toString();
			// System.out.println(document);
			String[] pos = document.split("\\t");
			String[] term_df = pos[0].split(":");
			// System.out.println(param);
			String[] queryterms = param.split(" ");
			for (int i = 0; i < queryterms.length; i++) {
				if (term_df[0].toLowerCase().contains(queryterms[i].toLowerCase())) {

					String[] doclist = pos[1].trim().split(";");
					for (int j = 0; j < doclist.length; j++) {
						String docid = doclist[j].trim().replaceAll(
								"\\<.*?\\> ?", "");

						int tf_doc = Integer.parseInt(docid.split(":")[1]
								.trim());
						int len_doc = Integer.parseInt(docid.split(":")[2]
								.trim());
						double rsv = idfweight(tf_doc, N)
								* (((k1 + 1) * tf_doc) / (k1
										* ((1 - b) + b * (len_doc / Lave)) + tf_doc));
						term.set(docid.split(":")[0].trim());
						data.set(String.valueOf(rsv));
						context.write(term, data);
					}
				}
			}
		}
	}

	public static class BM25Reduce extends Reducer<Text, Text, Text, Text> {

		private final static Text term = new Text();
		private final static Text data = new Text();
		
		ArrayList<String> scores_files = new ArrayList<String>();
		ArrayList<Double> scoreslist = new ArrayList<Double>();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double rsvd = 0;
			for (Text val : values)
				rsvd += Double.parseDouble(val.toString());
			scores_files.add(rsvd+"-"+key.toString());
			scoreslist.add(rsvd);
			//	context.write(key, new Text(String.valueOf(rsvd)));
		}
		
		protected void cleanup(Context context) throws IOException,
        InterruptedException {
			Collections.sort(scoreslist, Collections.reverseOrder());
			if (!scoreslist.isEmpty()) {
				Iterator itr = scoreslist.iterator();
				int top_list = 0;
				while (itr.hasNext() && top_list <= 50) {
					String s_val = itr.next().toString();
					for (int i = 0; i < scores_files.size(); i++) {
						String s_key = scores_files.get(i).split("-")[0];
						if (s_val.equals(s_key)) {
							String[] k_v = scores_files.get(i).toString()
									.split("-");
							context.write(new Text(k_v[1] + "newsML.xml"),new Text(k_v[0]));
							top_list++;
							scores_files.remove(i);
							i = scores_files.size();
						}
					}
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		conf.set("Query", args[2]);

		conf.set("Value of k1", args[4]);
		conf.set("Value of b", args[5]);

		conf.set("job.Lave.path", args[3]);

		Job job = new Job(conf);
		job.setJarByClass(Okapi.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(Okapi.BM25Map.class);
		job.setReducerClass(Okapi.BM25Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		String outputpath = FileOutputFormat.getOutputPath(job).toString();
		job.waitForCompletion(true);

		return 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new Okapi(), args);
	}
}