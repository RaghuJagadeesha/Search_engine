package org.sample;

import javax.xml.stream.XMLStreamConstants;//XMLInputFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.stream.*;


public class XmlParser11
{
        public static class Map extends Mapper<Text, BytesWritable, Text, Text> {
        	
    		private StringBuilder temp=null;
    		private String tmp ="";

    		private final static Text word = new Text();
    		private final static Text loc_id = new Text();

    	    private HashMap stopWordList;
    		@Override
    		protected void setup(Context context)
    				throws IOException, InterruptedException {
    			
    			Configuration conf = context.getConfiguration();
    		    //hardcoded or set it in the jobrunner class and retrieve via this key
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
    		                int i=0;
    		                while ((line = br.readLine()) != null && line.trim().length() > 0) {
    		                	stopWordList.put(i, line);
    		                	i++;
    		                }
    		            }
    		        }
    		        catch (IOException e) {
    		            //handle
    		        } 
    		    }
    		}
   @Override
  protected void map(Text key, BytesWritable value,
                     Context context)
      throws
      IOException, InterruptedException {
	   
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
			loc_id.set(itemid);
			context.write(word, loc_id);
		}
   }
}
public static class Reduce
    extends Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values,
                     Context context)
      throws IOException, InterruptedException {
    
	  boolean first = true;
		StringBuilder temp = new StringBuilder();
		List<String> lst = new LinkedList<String>();
		
		for(Text val : values)
		{
			String docID = val.toString();
			if(!lst.contains(docID))
			{
				lst.add(docID);
			}
		}
		Collections.sort(lst);
		Iterator itr = lst.iterator();	
		while(itr.hasNext())
		{
			if(first==false)
				temp.append("; ");
			first = false;
			temp.append(itr.next().toString());
		}
		String key1= key.toString();
		key1=key+":"+String.valueOf(lst.size());
		lst.clear();
		context.write(new Text(key1), new Text(temp.toString()));
  }

}

        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();

                conf.set("job.stopwords.path", args[2]);
                
                Job job = new Job(conf);
                job.setJarByClass(XmlParser11.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setMapperClass(XmlParser11.Map.class);
                job.setReducerClass(XmlParser11.Reduce.class);
                job.setNumReduceTasks(64);
                job.setInputFormatClass(ZipFileInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.waitForCompletion(true);
        }
}