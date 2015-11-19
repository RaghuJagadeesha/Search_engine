package rankedSearchMp;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Map;

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

class locations{
	String term;
	ArrayList<String> loc;
	
	public locations()
	{
		this.term = new String();
		loc = new ArrayList<String>();
	}
	
	public String toString(){
		Iterator it = loc.iterator();
		String sb = new String();
		while(it.hasNext())
		{
			sb = sb+" "+it.next()+",";
		}
		return sb;
	}
}

class doc_detail{
	
	int doc_id;
	int term_freq;
	
	public int getDoc_id() {
		return doc_id;
	}
	public int getTerm_freq() {
		return term_freq;
	}
	public doc_detail()
	{		
		this.doc_id = 0;
	}
}

class term_instance{
	int doc_freq;
	String term;
	int tf;
	double f_wt;
	ArrayList<doc_detail> doc_list;
	
	public String getTerm() {
		return term;
	}
	public int getDoc_freq() {
		return doc_freq;
	}
	public term_instance() {
		this.doc_freq = 0;
		this.term = new String();
		this.tf = 0;
		this.f_wt = 0.0;
		doc_list = new ArrayList<doc_detail>();
	}
}

public class RankedSearchMp extends Configured implements Tool{
	
	public static double tfweight(int tf) {
		return (1+Math.log10(tf));
	}
	
	public static double idfweight(int df, int N) {
		return (Math.log10(N/df));
	}

    public static class PostingsMap extends Mapper<LongWritable, Text, Text, Text> {
    
    	private final static Text term = new Text();
	    private final static Text data = new Text();
	    

	    @Override
	    protected void map(LongWritable key, Text value,
                 Mapper.Context context)
                		 throws
                		 IOException, InterruptedException {
	    	
	    	Configuration conf = context.getConfiguration();
		    String param = conf.get("Query");
		    
		    String document = value.toString();
		    //System.out.println(document);
		    String[] pos = document.split("\\t");
		    String[] term_df = pos[0].split(":");
		   // System.out.println(param);
		    String[] queryterms = param.split(" ");
		    for(int i = 0 ; i < queryterms.length ; i++)
		    {
		    	if(queryterms[i].equalsIgnoreCase(term_df[0]))
		    	{
		    		term.set(pos[0]);
		    	    String[] doclist = pos[1].trim().split(";");
				    locations term_loc = new locations();
				    term_loc.term = queryterms[i];
				    for(int j = 0 ; j < doclist.length ; j++)
				    {
				    	String docid = doclist[j].trim().replaceAll("\\<.*?\\> ?", "");
				    	term_loc.loc.add(docid);
				    }
				    data.set(term_loc.loc.toString());
				    context.write(term, data);
		    	}
		    }
	    }
    }
    
    public static class PostingsReduce extends Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values,
    			Context context)
    					throws IOException, InterruptedException {    		
    		Configuration conf = context.getConfiguration();
		    String param = conf.get("Query");
		    String[] queryterms = param.split("\\s");
		    ArrayList<String> values_list = new ArrayList<String>();
		    for (Text val : values)
		    {
		    	String doclist = val.toString().replaceAll("\\[|\\]", "");
		    	String[] docid_tf = doclist.split(",");
		    	for(int i =0; i<docid_tf.length;i++)
		    		values_list.add(docid_tf[i].trim());
		    	
		    }
		    context.write(key, new Text(values_list.toString().replaceAll("\\[|\\]", "")));
		    context.getCounter("NoOfRed","NoOfRed").increment(1);
        }
    }

    public static class NormMap extends Mapper<LongWritable, Text, Text, Text> {
        
    	private final static Text term = new Text();
	    private final static Text data = new Text();
	    

	    @Override
	    protected void map(LongWritable key, Text value,
                 Mapper.Context context)
                		 throws
                		 IOException, InterruptedException {
	    	
	    	Configuration conf = context.getConfiguration();
		    String param = conf.get("positions");
		    String[] positions = param.replaceAll("\\[|\\]", "").split(",");
		    
		    String document = value.toString().replaceAll("\\<.*?\\>", "");
		    String[] pos = document.split("\\t");
		    String[] doclists = pos[1].split(";");
		    
		    
		    for(int i = 0 ; i < positions.length ; i++)
		    {
		    	double t_weight = 0;
		    	//System.out.println("each docid "+positions[i]);
		    	for(int j = 0 ; j < doclists.length ; j++)
			    {
		    		if(doclists[j].trim().split(":")[0].equals(positions[i].trim()))
		    		{
		    			t_weight = tfweight(Integer.parseInt(doclists[j].trim().split(":")[1].trim()));
		    			
		    			context.write(new Text(positions[i].trim()), new Text(String.valueOf(t_weight)));
		    		}
			    }
		    }
	    }
    }
    public static class NormReduce extends Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values,
    			Context context)
    					throws IOException, InterruptedException {    		
    		double sum = 0;
    		for (Text val : values)
		    	sum += Double.parseDouble(val.toString());
		    double norm = Math.sqrt(sum);
		    context.write(key, new Text(String.valueOf(norm)));
        }
    }
    
    	
	private static void extract_details(term_instance term_1, String det) {
		String[] pos_detail = det.split(",");
		doc_detail doc_instance;
		//System.out.println(det);
		for(int i=0 ; i< term_1.doc_freq;i++)
		{
			doc_instance = new doc_detail();
			String[] temp = pos_detail[i].trim().split(":");
			doc_instance.doc_id = Integer.parseInt(temp[0].trim());
			doc_instance.term_freq = Integer.parseInt(temp[1].trim());
			term_1.doc_list.add(doc_instance);
		}
	}

	private long termsPostings(String[] args) throws Exception
	{
		Configuration conf = getConf();
		conf.set("Query", args[2]);
		Job job = new Job(conf);
        job.setJarByClass(RankedSearchMp.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(RankedSearchMp.PostingsMap.class);
        job.setReducerClass(RankedSearchMp.PostingsReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        long count = job.getCounters().findCounter("NoOfRed", "NoOfRed").getValue();
        return count;
	}
	
	private void normDoclists(String inputpath, String outputpath, String positions) throws Exception
	{
		Configuration conf = getConf();
		conf.set("positions", positions);
		Job job = new Job(conf);
        job.setJarByClass(RankedSearchMp.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setMapperClass(RankedSearchMp.NormMap.class);
        job.setReducerClass(RankedSearchMp.NormReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job, new Path(inputpath));
        FileOutputFormat.setOutputPath(job, new Path(outputpath));

        job.waitForCompletion(true);

	}	
	public int run(String[] args) throws Exception
    {
		
		String query = args[2];
             
        long count = termsPostings(args);
             
        String[] queryterms = query.split(" ");
        
        ArrayList<Integer> result_doclist = new ArrayList<Integer>();
         	
        ArrayList<term_instance> term_data = new ArrayList<term_instance>();
        ArrayList<Double> normalize_docid = new ArrayList<Double>();
        ArrayList<Double> rankinglst = new ArrayList<Double>();
        TreeMap<Double, Integer> ranked_output = new TreeMap<Double, Integer>(Collections.reverseOrder());

        int N = 806791;
         	
        File term = new File(args[1]);
        File[] l_files = term.listFiles();
        
        
        File filename = new File("part-r-00000");
        
        for(int fndx = 0 ; fndx < l_files.length;fndx++)
 		{
 			filename = l_files[fndx];
 			//System.out.println(filename.getAbsolutePath());
 			if(filename.getName().startsWith("part"))
 			{
 				Scanner termDoc = new Scanner(filename);
 					
 				while(termDoc.hasNextLine())
 				{
 					String linefromdoc = termDoc.nextLine();
 					String[] term_postings = linefromdoc.split("\\t");
 					term_instance search_term = new term_instance();
 						
 					search_term.term = term_postings[0].trim().split(":")[0];
 					search_term.doc_freq = Integer.parseInt(term_postings[0].trim().split(":")[1]);
 					int j = 0;
 					for(j = 0 ;j<term_data.size();j++)
 					{
 						if(term_data.get(j).term.equals(search_term.getTerm()))
 						{
 							search_term = term_data.get(j);
 							for(int i = 0 ; i < queryterms.length ; i++)
 							{
 								if(queryterms[i].equalsIgnoreCase(search_term.term))
 									search_term.tf = search_term.tf+1;
 							}
 							break;
 						}
 							
 					} 
 					if(j==term_data.size())
 					{
 						extract_details(search_term, term_postings[1]);
 						search_term.tf = 1;
 						search_term.f_wt = tfweight(search_term.tf) * idfweight(search_term.doc_freq, N);
 						//	System.out.println(search_term.f_wt);
 						term_data.add(search_term);
 					}
 						
 					for(int idx = 0; idx < search_term.doc_freq; idx++)
 					{
 						if(result_doclist.contains(search_term.doc_list.get(idx).doc_id))
 							continue;
 						result_doclist.add(search_term.doc_list.get(idx).doc_id);
 						normalize_docid.add(0.0);
 						rankinglst.add(0.0);
 					}
 				}
 			}
 		}
        Collections.sort(result_doclist);
	    normDoclists(args[0], args[1]+"/norm",result_doclist.toString());
	    
        
	    term = new File(args[1]+"/norm");
	    File[] r_files = term.listFiles();
	    for(int fndx = 0 ; fndx < l_files.length;fndx++)
 		{
 			filename = r_files[fndx];
 			//System.out.println(filename.getAbsolutePath());
 			if(filename.getName().startsWith("part"))
 			{
 				Scanner termDoc = new Scanner(filename);
 					
 				while(termDoc.hasNextLine())
 				{
 					String linefromdoc = termDoc.nextLine();
 					String[] doc_norms = linefromdoc.split("\\t");
 					normalize_docid.set(result_doclist.indexOf(Integer.parseInt(doc_norms[0])),Double.parseDouble(doc_norms[1]));
 				}
 			}
 		}
	    for(int i =0 ;i< term_data.size();i++) 
		{
	    	term_instance search_term = new term_instance();
			search_term = term_data.get(i);
			for(int j =0 ;j<result_doclist.size();j++)
			{ 
				double sum = 0;
				for(int k=0;k<search_term.doc_freq;k++)
				{
					if(result_doclist.get(j)==search_term.doc_list.get(k).doc_id)
					{
						sum = (tfweight(search_term.doc_list.get(k).term_freq)) * search_term.f_wt;
						//System.out.println(search_term.term+" "+sum);
						sum = sum/normalize_docid.get(j);
						rankinglst.set(j, rankinglst.get(j)+sum);
					}
				}
			}
		}
	    
	    for(int i =0 ;i< rankinglst.size();i++) 
		{
			ranked_output.put(rankinglst.get(i),result_doclist.get(i));
		}
	    
	    File outfile = new File(args[1]+"/part-00000"); 
        FileOutputStream fos = new FileOutputStream(outfile);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        
	    if(rankinglst.isEmpty())
		{
			System.out.println("Search returned no results");
		}
		else
		{
			Set set = ranked_output.entrySet();
	    	Iterator itr = set.iterator();
	    	int top_list = 0;
	    	while(itr.hasNext() && top_list<=20) {
	    		
	    		String[] k_v = itr.next().toString().split("=");
	    		//System.out.println("key is "+k_v[0]);
	    		//System.out.println("val is "+k_v[1]);
	    		//bw.write(k_v[1]+"\t"+k_v[0]);
	    		bw.write(k_v[1]);
	    		bw.newLine();
	    		top_list++;
	    	}
		}
	    bw.close();	
	    return 1;
    }
	
	public static void main(String[] args) throws Exception{
		ToolRunner.run(new Configuration(), new RankedSearchMp(), args);
	}
}
