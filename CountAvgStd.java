import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
 
public class avgstdCall {
 
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
 
        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            // this will work even if we receive more than 1 line
            Scanner scanner = new Scanner(value.toString());
            String line;
            String[] tokens;
            double observation;
            while (scanner.hasNext()) {
                line = scanner.nextLine();
                tokens = line.split("\\s+");
                observation = Double.parseDouble(tokens[1]);
                output.collect(new Text("values"), new DoubleWritable(observation));
            }
        }
    }
 
    public static class Combine extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
 
        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double count = 0d; 
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            double sum = 0d;
            double sumSquared = 0d;
 
            double value;
            while (values.hasNext()) {
                ++count;
                value = values.next().get();
                min = Math.min(min, value);
                max = Math.max(max, value);
                sum += value;
                sumSquared += value * value;
            }
 
            
            output.collect(new Text("count"), new DoubleWritable(count));
            output.collect(new Text("max"), new DoubleWritable(max));
            output.collect(new Text("min"), new DoubleWritable(min));
            output.collect(new Text("sum"), new DoubleWritable(sum));
            output.collect(new Text("sumSquared"), new DoubleWritable(sumSquared));
        }
    }
 
    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
 
        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            if (key.equals(new Text("count"))) {
                double count = 0d;
                double value;
                while (values.hasNext()) {
                    value = values.next().get();
                    count += value;
                }
                output.collect(new Text("count"), new DoubleWritable(count));
            }
 
            if (key.equals(new Text("max"))) {
                double max = Double.MIN_VALUE;
                double value;
                while (values.hasNext()) {
                    value = values.next().get();
                    max = Math.max(max, value);
                }
                output.collect(new Text("max"), new DoubleWritable(max));
            }
 
            if (key.equals(new Text("min"))) {
                double min = Double.MAX_VALUE;
                double value;
                while (values.hasNext()) {
                    value = values.next().get();
                    min = Math.min(min, value);
                }
                output.collect(new Text("min"), new DoubleWritable(min));
            }
 
            if (key.equals(new Text("sum"))) {
                double sum = 0d;
                double value;
                while (values.hasNext()) {
                    value = values.next().get();
                    sum += value;
                }
                output.collect(new Text("sum"), new DoubleWritable(sum));
            }
 
            if (key.equals(new Text("sumSquared"))) {
                double sumSquared = 0d;
                double value;
                while (values.hasNext()) {
                    value = values.next().get();
                    sumSquared += value;
                }
                output.collect(new Text("sumSquared"), new DoubleWritable(sumSquared));
            }
        }
    }
     
     
     
     
     
    public static boolean applySecondPass(Path in, Path out) {
        double count = 0d, max = 0d, min = 0d, sum = 0d, sumSquared = 0d;
        try (FileSystem fs = FileSystem.get(new Configuration());
                BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(in)));) {
            String line;
            String[] words;
 
            line = br.readLine();
            while (line != null) {
                words = line.split("\\s+");
 
                switch (words[0]) {
                    case "count":
                        count = Double.parseDouble(words[1]);
                        break;
                    case "max":
                        max = Double.parseDouble(words[1]);
                        break;
                    case "min":
                        min = Double.parseDouble(words[1]);
                        break;
                    case "sum":
                        sum = Double.parseDouble(words[1]);
                        break;
                    case "sumSquared":
                        sumSquared = Double.parseDouble(words[1]);
                        break;
                }
                line = br.readLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
 
        double avg = sum / count;
        double std = Math.sqrt((sumSquared - (sum * sum) / count) / (count - 1));
		
        try (FileSystem fs = FileSystem.get(new Configuration());
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(out, true)));) {
            String line;
            line = "avg\t" + String.valueOf(avg) + System.lineSeparator();
            bw.write(line);
            line = "std\t" + String.valueOf(std) + System.lineSeparator();
            bw.write(line);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
 
        return true;
    }
 



     
    public static void main(String[] args) throws Exception {

        JobConf conf = new JobConf(avgstdCall.class);
        conf.setJobName("callAll");
 
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);
 
        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Combine.class);
        conf.setReducerClass(Reduce.class);
 
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        Path out1 = new Path(args[1]);
	FileOutputFormat.setOutputPath(conf, out1);
  
        JobClient.runJob(conf); 
        Path out1Merged = new Path(args[2]);
        Configuration config = new Configuration();
        try {
            FileSystem hdfs = FileSystem.get(config);
            FileUtil.copyMerge(hdfs, out1, hdfs, out1Merged, false, config, null);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
 
       
        boolean success = applySecondPass(out1Merged, new Path(args[3]));
        System.exit(success ? 1 : 0);
         
    }
}
