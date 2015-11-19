package org.retrieve;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Scanner;
import java.util.StringTokenizer;

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

class locations {
	String term;
	ArrayList<String> loc;

	public locations() {
		this.term = new String();
		loc = new ArrayList<String>();
	}

	public String toString() {
		Iterator it = loc.iterator();
		String sb = new String();
		while (it.hasNext()) {
			sb = sb + " " + it.next() + ",";
		}
		return sb;
	}
}

public class BooleanRetrieval extends Configured implements Tool {
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private final static Text term = new Text();
		private final static Text data = new Text();

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
			String[] queryterms = param.split("\\s");
			for (int i = 0; i < queryterms.length; i++) {
				// System.out.println("query term is "+queryterms[i]);
				if (queryterms[i].equalsIgnoreCase("AND")
						|| queryterms[i].equalsIgnoreCase("ANDNOT")
						|| queryterms[i].equalsIgnoreCase("OR")
						|| queryterms[i].equalsIgnoreCase("OR"))
					continue;
				if (queryterms[i].equalsIgnoreCase(term_df[0])) {
					term.set(pos[0]);
					// System.out.println("term_df is "+term_df[0]);
					String[] doclist = pos[1].trim().split(";");
					locations term_loc = new locations();
					term_loc.term = queryterms[i];
					for (int j = 0; j < doclist.length; j++) {
						String docid = doclist[j].trim().replaceAll(
								"\\<.*?\\> ?", "");
						term_loc.loc.add(docid);
					}
					data.set(term_loc.loc.toString());
					context.write(term, data);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String param = conf.get("Query");
			String[] queryterms = param.split("\\s");
			ArrayList<String> values_list = new ArrayList<String>();
			for (Text val : values) {
				String doclist = val.toString().replaceAll("\\[|\\]", "");
				String[] docid_tf = doclist.split(",");
				for (int i = 0; i < docid_tf.length; i++)
					values_list.add(docid_tf[i].trim().split(":")[0].trim());

			}
			context.write(key,
					new Text(values_list.toString().replaceAll("\\[|\\]", "")));
			context.getCounter("NoOfRed", "NoOfRed").increment(1);
		}
	}

	private String booleanSearch(String[] args)throws Exception {
    	
    	Configuration conf = getConf();
    	String[] splitter = args[2].split(" ");
    	BufferedReader br = null;
    	String result = "";
    	try {
			
    	boolean flag = false;
    	
		String searchterm1 = "";
		String searchterm2 = "";
		String term1 = "";
		String term2 = "";
		
		for(int i = 0 ; i< splitter.length;i++)
    	{	
			//System.out.println(splitter[i]);
    		if(splitter[i].equalsIgnoreCase("AND"))
			{
    			searchterm1 = splitter[i-1];
    			searchterm2 = splitter[i+1];
    			FileSystem fs = FileSystem.get(conf);
    			Path path = new Path(args[1] + "/part-r-00000");
    			if (fs.exists(path)) {
    				FSDataInputStream fis = fs.open(path);
    				br = new BufferedReader(new InputStreamReader(fis));
    				String linefromdoc = null;
    				if(flag==false)
					{
    					while ((linefromdoc = br.readLine()) != null && linefromdoc.trim().length() > 0) 
    					{
    						if(linefromdoc.contains(searchterm1))
    						{
    							String[] postingsfromdoc = linefromdoc.split("\\t");
    							term1 = postingsfromdoc[1];
    						}
    						else if(linefromdoc.contains(searchterm2))
    						{
    							String[] postingsfromdoc = linefromdoc.split("\\t");
    							term2 = postingsfromdoc[1];
    						}
    					}
    						flag = true;
    						result = intersect(term1,term2,splitter[i]);
    				}
    					else 
    					{
    						while ((linefromdoc = br.readLine()) != null && linefromdoc.trim().length() > 0) {
    		    				if(linefromdoc.contains(searchterm2))
    							{
    								String[] postingsfromdoc = linefromdoc.split("\\t");
    								term2 = postingsfromdoc[1];
    							}
    						}
    						result = intersect(result,term2,splitter[i]);
    					}
    				}
    		}
			else if(splitter[i].equalsIgnoreCase("ANDNOT"))
			{
				searchterm1 = splitter[i-1];
    			searchterm2 = splitter[i+1];
    			FileSystem fs = FileSystem.get(conf);
    			Path path = new Path(args[1] + "/part-r-00000");
    			if (fs.exists(path)) {
    				FSDataInputStream fis = fs.open(path);
    				br = new BufferedReader(new InputStreamReader(fis));
    				String linefromdoc = null;
    				if(flag==false)
					{
    					while ((linefromdoc = br.readLine()) != null && linefromdoc.trim().length() > 0) 
    					{
    						if(linefromdoc.contains(searchterm1))
    						{
    							String[] postingsfromdoc = linefromdoc.split("\\t");
    							term1 = postingsfromdoc[1];
    						}
    						else if(linefromdoc.contains(searchterm2))
    						{
    							String[] postingsfromdoc = linefromdoc.split("\\t");
    							term2 = postingsfromdoc[1];
    						}
    					}
    						flag = true;
    						result = intersect(term1,term2,splitter[i]);
    				}
    					else 
    					{
    						while ((linefromdoc = br.readLine()) != null && linefromdoc.trim().length() > 0) {
    		    				if(linefromdoc.contains(searchterm2))
    							{
    								String[] postingsfromdoc = linefromdoc.split("\\t");
    								term2 = postingsfromdoc[1];
    							}
    						}
    						result = intersect(result,term2,splitter[i]);
    					}
    				}
			}
    		else if(splitter[i].equalsIgnoreCase("OR"))
			{
    			searchterm1 = splitter[i-1];
    			searchterm2 = splitter[i+1];
    			FileSystem fs = FileSystem.get(conf);
    			Path path = new Path(args[1] + "/part-r-00000");
    			if (fs.exists(path)) {
    				FSDataInputStream fis = fs.open(path);
    				br = new BufferedReader(new InputStreamReader(fis));
    				String linefromdoc = null;
    				if(flag==false)
					{
    					while ((linefromdoc = br.readLine()) != null && linefromdoc.trim().length() > 0) 
    					{
    						if(linefromdoc.contains(searchterm1))
    						{
    							String[] postingsfromdoc = linefromdoc.split("\\t");
    							term1 = postingsfromdoc[1];
    						}
    						else if(linefromdoc.contains(searchterm2))
    						{
    							String[] postingsfromdoc = linefromdoc.split("\\t");
    							term2 = postingsfromdoc[1];
    						}
    					}
    						flag = true;
    						result = intersect(term1,term2,splitter[i]);
    				}
    					else 
    					{
    						while ((linefromdoc = br.readLine()) != null && linefromdoc.trim().length() > 0) {
    		    				if(linefromdoc.contains(searchterm2))
    							{
    								String[] postingsfromdoc = linefromdoc.split("\\t");
    								term2 = postingsfromdoc[1];
    							}
    						}
    						result = intersect(result,term2,splitter[i]);
    					}
    				}
				}
    		}
		}
		catch(Exception e)
		{
			
		}
    	return result;
    }

	private String intersect(String docId1, String docId2, String op) {
		String result = "";
		String[] doc1 = docId1.split(",");
		String[] doc2 = docId2.split(",");
		int i = 0, j = 0, oper = 0;

		if (op.equalsIgnoreCase("AND"))
			oper = 1;
		if (op.equalsIgnoreCase("OR"))
			oper = 2;
		if (op.equalsIgnoreCase("ANDNOT"))
			oper = 3;

		switch (oper) {

		case 1:
			while (i < doc1.length && j < doc2.length) {
				if (Integer.parseInt(doc1[i].trim()) == Integer
						.parseInt(doc2[j].trim())) {
					// System.out.println(Integer.parseInt(doc1[i]));
					if (result.isEmpty())
						result = doc1[i].trim();
					else
						result = result + ", " + doc1[i].trim();
					i++;
					j++;
				} else if (Integer.parseInt(doc1[i].trim()) < Integer
						.parseInt(doc2[j].trim())) {
					i++;
				} else {
					j++;
				}
			}
			break;
		case 2:
			while (i < doc1.length) {
				if (result.isEmpty())
					result = doc1[i].trim();
				else
					result = result + ", " + doc1[i].trim();
				i++;
			}
			while (j < doc2.length) {
				if (result.isEmpty())
					result = doc1[i];
				else {
					if (!result.contains(doc2[j]))
						result = result + ", " + doc2[j].trim();
					j++;
				}
			}
			break;
		case 3:
			while (i < doc1.length && j < doc2.length) {
				if (Integer.parseInt(doc1[i].trim()) == Integer
						.parseInt(doc2[j].trim())) {
					i++;
					j++;
					if (j >= doc2.length) {
						while (i < doc1.length) {
							if (result.isEmpty())
								result = doc1[i].trim();
							else
								result = result + ", " + doc1[i].trim();
							i++;
						}
					}
				} else if (Integer.parseInt(doc1[i].trim()) < Integer
						.parseInt(doc2[j].trim())) {
					if (result.isEmpty())
						result = doc1[i].trim();
					else
						result = result + ", " + doc1[i].trim();
					i++;
				} else {
					j++;
					if (j >= doc2.length) {
						while (i < doc1.length) {
							if (result.isEmpty())
								result = doc1[i].trim();
							else
								result = result + ", " + doc1[i].trim();
							i++;
						}
					}
				}
			}
			break;
		default:
			break;
		}
		return result;
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("Query", args[2]);
		Job job = new Job(conf);
		job.setJarByClass(BooleanRetrieval.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		job.setMapperClass(BooleanRetrieval.Map.class);
		job.setReducerClass(BooleanRetrieval.Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		String resultlist = new String();
		resultlist = booleanSearch(args);
		
		String[] found_files = resultlist.trim().split(",");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(args[1] + "/outputfiles.txt");
		FSDataOutputStream fos = fs.create(path);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
		for(int i =0 ; i < found_files.length;i++)
		{
			bw.write(found_files[i] + "newsML.xml");
			bw.newLine();
		}
		bw.close();
		return 1;
	}

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new BooleanRetrieval(), args);
	}
}
