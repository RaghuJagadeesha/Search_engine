package org.position.parse;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;//XMLInputFactory;
import javax.xml.stream.XMLStreamReader;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.xml.sax.SAXException;

import javax.xml.stream.*;

public class PositionalIndex {
	public static class PosMap extends Mapper<Text, BytesWritable, Text, Text> {

		private StringBuilder temp = null;
		private String tmp = "";

		private final static Text word = new Text();
		private final static Text loc_id = new Text();
		private HashMap stopWordList;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {

			Configuration conf = context.getConfiguration();
			// hardcoded or set it in the jobrunner class and retrieve via this
			// key
			String location = conf.get("job.stopwords.path");

			if (location != null) {
				BufferedReader br = null;
				try {
					FileSystem fs = FileSystem.get(conf);
					Path path = new Path(location);
					if (fs.exists(path)) {
						stopWordList = new HashMap<String, Object>();
						FSDataInputStream fis = fs.open(path);
						br = new BufferedReader(new InputStreamReader(fis));
						String line = null;
						int i = 0;
						while ((line = br.readLine()) != null
								&& line.trim().length() > 0) {
							stopWordList.put(i, line);
							i++;
						}
					}
				} catch (IOException e) {
					// handle
				}
			}
		}

		@Override
		protected void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String st1 = XmlParser.parse(value, key);
			String itemid = st1.split(" ")[0].trim().split("_")[1].trim();
			StringTokenizer stk = new StringTokenizer(st1," \t\n!,\"");
			int doclength = stk.countTokens();
			
			int index = 0;
			while (stk.hasMoreTokens()) {
				String wrd = stk.nextToken();
				if (!(wrd.matches(".*\\w.*")))
					continue;
				index++;
				if (stopWordList.containsValue(wrd.trim()))
					continue;
				wrd = wrd.trim();
				if (wrd.endsWith(".")) {
					wrd = wrd.substring(0, wrd.length() - 1);
					wrd = wrd.trim();
				}
				if (wrd.endsWith(":")) {
					wrd = wrd.substring(0, wrd.length() - 1);
					wrd = wrd.trim();
				}
				if (wrd.endsWith(",")) {
					wrd = wrd.substring(0, wrd.length() - 1);
					wrd = wrd.trim();
				}
				wrd = wrd.replaceAll("[\\[\\](){}]", "");
				wrd = wrd.replace("'", "");
				
				word.set(wrd.trim().toLowerCase());
				loc_id.set(itemid + "-" + doclength + ":" + index);
				context.write(word, loc_id);
			}
		}
	}

	public static class PosReduce extends Reducer<Text, Text, Text, Text> {

		class doc_detail {
			int doc_num;
			int count;
			int doclength;
			boolean first;
			ArrayList<Integer> pos_list;

			public int getDoc_num() {
				return doc_num;
			}

			public int getCount() {
				return count;
			}

			public int getDocLength() {
				return doclength;
			}
			public doc_detail() {
				this.doc_num = 0;
				this.count = 0;
				this.doclength = 0;
				this.first = true;
				pos_list = new ArrayList<Integer>();
			}

			public String toString() {
				String sb = String.valueOf(doc_num);
				sb = sb + ":" + String.valueOf(count)+":"+String.valueOf(doclength);
				Iterator it = pos_list.iterator();
				while (it.hasNext()) {
					if (first == true) {
						first = false;
						sb = sb + "<" + it.next();
						// System.out.println("details value1 "+sb);
					} else {
						sb = sb + ", " + it.next();
						// System.out.println("details value2 "+sb);
					}
				}
				sb = sb + ">; ";
				// System.out.println("details value final "+sb);
				return sb;
			}
		}
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			StringBuilder temp = new StringBuilder();
			ArrayList<String> lst = new ArrayList<String>();
			ArrayList<doc_detail> details = new ArrayList<doc_detail>();

			int index;
			int tf = 0;
			boolean found;
			// System.out.println(key+" is key");
			for (Text val : values) {
				String id_pos[] = val.toString().split(":");
				int docID = Integer.parseInt(id_pos[0].split("-")[0].trim());
				// System.out.println("map_loc is "+val.toString());
				doc_detail temp_doc = new doc_detail();
				index = 0;
				found = false;
				while (index < details.size()) {
					int doc_number = details.get(index).getDoc_num();
					if (doc_number == docID) {
						found = true;
						temp_doc = details.get(index);
						// System.out.println("test 1");
						break;
					}
					// System.out.println("test 2");
					index++;
				}
				if (found) {
					temp_doc.count++;
					temp_doc.doclength = Integer.parseInt(id_pos[0].split("-")[1].trim());
					temp_doc.pos_list.add(Integer.parseInt(id_pos[1]));
					//Collections.sort(temp_doc.pos_list);
					details.set(index, temp_doc);
					tf++;
				} else {
					temp_doc.doc_num = docID;
					temp_doc.count++;
					temp_doc.pos_list.add(Integer.parseInt(id_pos[1]));
					details.add(temp_doc);
					tf++;
				}
			}
			index = 0;
			 
			Iterator itr = details.iterator();
			while (itr.hasNext()) {
				lst.add(itr.next().toString());
			}
			details.clear();
			details.trimToSize();
			Collections.sort(lst);
			itr = lst.iterator();
			while (itr.hasNext())
				temp.append(itr.next().toString());
			String key1 = key.toString();
			key1 = key + ":" + lst.size();
			lst.clear();
			lst.trimToSize();
			context.write(new Text(key1), new Text(temp.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		conf.set("job.stopwords.path", args[2]);
		Job job = new Job(conf);
		job.setJarByClass(PositionalIndex.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(PositionalIndex.PosMap.class);
		job.setReducerClass(PositionalIndex.PosReduce.class);
		job.setInputFormatClass(ZipFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setNumReduceTasks(64);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}