import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;

import jdk.nashorn.internal.runtime.Context;

public class WordCountFirst {
    
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, Text>{
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String aux = value.toString();
            String [] values = aux.split(",");
            //System.out.println(values[0]);
            context.write(new Text(values[0]), new Text(values[1]+";"+values[2]));
        }
    }
    
    public static class UserRatingReducer
    extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            String text = "";
            for (Text val : values) {
                text = text + "," + val.toString();
            }
            context.write(key, new Text(text));
        }
    }

    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountFirst.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(UserRatingReducer.class);
        job.setReducerClass(UserRatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
