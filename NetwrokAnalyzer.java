package org.myorg;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class NetworkAnalysis {
        
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private IntWritable lengthValue = new IntWritable();    
    private Text keyWord = new Text();
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
       	String[] hasilPecahInput = line.split("\t");
       	if(hasilPecahInput[2].length() > 1)
       	{
       		StringTokenizer stSource = new StringTokenizer(hasilPecahInput[2], ".");
       		StringTokenizer stDest = new StringTokenizer(hasilPecahInput[4], ".");
       		
       		while(stSource.hasMoreElements())
	       	{
       			String stSourceN = stSource.nextToken();
       			String stDestN = stDest.nextToken();
       			
       			//setting agar koneksi ip timbal balik dihitung sama (1.1.1.1 ke 2.2.2.2 dan 2.2.2.2 ke 1.1.1.1 dicounting pada data yang sama)
       			if(Integer.parseInt(stSourceN) > Integer.parseInt(stDestN))
       			{
       				keyWord.set("Koneksi "+hasilPecahInput[4]+ " dan "+hasilPecahInput[2]+ " Protokol "+hasilPecahInput[6]);
       				break;
       			}
       			
       			if(Integer.parseInt(stSourceN) < Integer.parseInt(stDestN))
       			{
       				keyWord.set("Koneksi "+hasilPecahInput[2]+ " dan "+hasilPecahInput[4]+ " Protokol "+hasilPecahInput[6]);
       				break;
       			}
       			
       			if(Integer.parseInt(stSourceN) == Integer.parseInt(stDestN))
       			{
       				keyWord.set("Koneksi "+hasilPecahInput[2]+ " dan "+hasilPecahInput[4]+ " Protokol "+hasilPecahInput[6]);
       				
       			}
       		}
       	}
       	else
       	{
       		keyWord.set("Koneksi protokol "+hasilPecahInput[6]);
       	}
       	
       	try{
       		lengthValue.set(Integer.parseInt(hasilPecahInput[1]));
       	}
       	catch(NumberFormatException ex)
       	{
       		lengthValue.set(0);
       	}
       	
       	
       	context.write(new Text(keyWord.toString()+",Paket"), one);
       	context.write(new Text(keyWord.toString()+",Lenght"), lengthValue);
       	
    }
    
    
 } 
        
 public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	 private org.apache.hadoop.mapreduce.lib.output.MultipleOutputs<Text, IntWritable> mos;
	 private IntWritable outputValue = new IntWritable();
	 
	 
   	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}



	public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
        String[] splitKey = key.toString().split(",");
        
        String keyOut = splitKey[0];
        
    	if(splitKey[1].equals("Paket"))
    	{
    		int sumPaket = 0;
    		for(IntWritable value : values)
    		{
    			sumPaket += value.get();
    		}
    		outputValue.set(sumPaket);
    		mos.write("PaketTotal", keyOut, outputValue);
    	}
    	else if(splitKey[1].equals("Lenght"))
    	{
    		int sumLength = 0;
    		for(IntWritable value : values)
    		{
    			sumLength += value.get();
    		}
    		outputValue.set(sumLength);
    		mos.write("PaketLenght", keyOut, outputValue);
    	}
    		
    }
	
	@Override
	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		mos.close();						
	}




 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf, "NetworkAnalysis");
    job.setJarByClass(NetworkAnalysis.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
        
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
        
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
        
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    
    org.apache.hadoop.mapreduce.lib.output.MultipleOutputs.addNamedOutput(job, "PaketTotal", TextOutputFormat.class, Text.class, IntWritable.class);
    org.apache.hadoop.mapreduce.lib.output.MultipleOutputs.addNamedOutput(job, "PaketLenght", TextOutputFormat.class, Text.class, IntWritable.class);
        
    job.waitForCompletion(true);
 }
        
}


