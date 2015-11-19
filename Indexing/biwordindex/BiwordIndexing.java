package org.indexing;

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
import org.indexing.XmlParser;

import javax.xml.stream.*;



public class BiwordIndexing
{

	public static class Map extends Mapper<Text, BytesWritable, Text, Text> {
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
   protected void map(Text key, BytesWritable value, Context context)
                    		 throws IOException, InterruptedException {
	   
	   	String st1 = XmlParser.parse(value, key).replaceAll("\t\n!,\"", " ");
		String itemid = st1.split(" ")[0].trim().split("_")[1].trim();
		String[] stk1 = st1.split(" ");
		for(int i=0; i < stk1.length-1;i++)
	  	{
			//System.out.println(stk1.length+" "+stk1[i]+" "+stk1[i+1]);
			String wrd1 = ""; 
			String wrd2 = "";
			if(!stk1[i].matches(".*\\w.*"))
				continue;
			wrd1 = stk1[i];
			while(i<(stk1.length-1))
			{
				wrd2 = stk1[i+1];
				if(wrd2.matches(".*\\w.*"))
				{
					if (!stopWordList.containsValue(wrd2.trim()))
						break;
				}
				i++;
			}
			
			wrd1 = wrd1.trim();
			if (wrd1.endsWith(".")) {
				wrd1 = wrd1.substring(0, wrd1.length() - 1);
				wrd1 = wrd1.trim();
			}
			if (wrd1.endsWith(":")) {
				wrd1 = wrd1.substring(0, wrd1.length() - 1);
				wrd1 = wrd1.trim();
			}
			if (wrd1.endsWith(",")) {
				wrd1 = wrd1.substring(0, wrd1.length() - 1);
				wrd1 = wrd1.trim();
			}
			wrd1 = wrd1.replaceAll("[\\[\\](){}]", "");
			wrd1 = wrd1.replace("'", "");
			
			wrd2 = wrd2.trim();
			if (wrd2.endsWith(".")) {
				wrd2 = wrd2.substring(0, wrd2.length() - 1);
				wrd2 = wrd2.trim();
			}
			if (wrd2.endsWith(":")) {
				wrd2 = wrd2.substring(0, wrd2.length() - 1);
				wrd2 = wrd2.trim();
			}
			if (wrd2.endsWith(",")) {
				wrd2 = wrd2.substring(0, wrd2.length() - 1);
				wrd2 = wrd2.trim();
			}
			wrd2 = wrd2.replaceAll("[\\[\\](){}]", "");
			wrd2 = wrd2.replace("'", "");
			
			word.set(wrd1.trim().toLowerCase()+" "+wrd2.trim().toLowerCase());
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
                job.setJarByClass(BiwordIndexing.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setMapperClass(BiwordIndexing.Map.class);
                job.setReducerClass(BiwordIndexing.Reduce.class);
                job.setNumReduceTasks(64);
                job.setInputFormatClass(ZipFileInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));

                job.waitForCompletion(true);
        }
}