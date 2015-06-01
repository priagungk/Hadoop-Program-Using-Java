import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
         
 
public class outRem{
    public static class Map extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            String lines = value.toString();
            String[] words = lines.split("\\s+");
	  
             
            Configuration conf = context.getConfiguration();
            String avg1 = conf.get("avg1");
            String std1 = conf.get("std1");
             
            Double avg = Double.parseDouble(avg1);
            Double std = Double.parseDouble(std1);
            Double s = Double.parseDouble(words[1]);

            if (words[1].length() > 1)
            {
                StringTokenizer tokenizer = new StringTokenizer(words[1]);
                while(tokenizer.hasMoreElements())
                {   String token = tokenizer.nextToken();
                 
                 
                    if(Double.parseDouble(token)>(avg-std)&&Double.parseDouble(token)<(avg+std))
                    { word.set(words[1]);
                      break;
                    }
                     
                }
                 
            }   
            else{
                word.set(words[1]);}
                     
                     
            context.write(word,one);
             
        }
    }
 
    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            int total = 0;
            for (IntWritable val : values){
                total++;
            }
            context.write(key, new IntWritable(total));
        }
    }
 
 
 
 
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br1 = new BufferedReader(new InputStreamReader(fs.open(new Path(args[1]))));
        String avg = "", std = "", line;
        String[] words;
        line = br1.readLine();
        while (line!=null){
            words = line.split("\\s+");
            switch(words[0]){
            case "avg":
                avg = words[1];
                break;
            case "std":
                std=words[1];
                break;
            }
            line = br1.readLine();
        }
        conf.set("avg1", avg);
        conf.set("std1",std);
        Job job = new Job(conf, "Outlier Removal");
        job.setJarByClass(outRem.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.getConfiguration().set("avg1", avg);
        job.getConfiguration().set("std1", std);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
