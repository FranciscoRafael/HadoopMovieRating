import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobClient;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jdk.nashorn.internal.runtime.Context;

public class WordCount {
    
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
                text = text + "|" + val.toString();
            }
            context.write(key, new Text(text));
        }
    }

    public static class CombinationMapper
    extends Mapper<Object, Text, Text, Text>{
        
        //public static final Log log = LogFactory.getLog(CombinationMapper.class);
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String aux = value.toString();
            String movies_line = aux.split("||")[1];
            String [] movies = movies_line.split("|");
            String [] movie1 = null;
            String [] movie2 = null;
            
            //Log.info("12345678" + movies_line);

            for (int i=0; i<movies.length; i++) {
                for (int j=i; j<movies.length; j++) {
                    if (movies[i].split(";").length == 2 && movies[j].split(";").length == 2) {
                        if (movies[i].compareTo(movies[j]) <= 0) {
                            movie1 = movies[i].split(";");
                            movie2 = movies[j].split(";"); 
                        } else {
                            movie2 = movies[i].split(";");
                            movie1 = movies[j].split(";");
                        }
                        context.write(new Text(movie1[0] + ";" + movie2[0]),new Text(movie1[1] + ";" + movie2[1]));
                    }
                }
            }

        }
    }

    public static class FinalReducer
    extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            double sum_xx = 0.0, sum_xy = 0.0, sum_yy = 0.0, sum_x = 0.0, sum_y = 0.0;
            int n = 0;

            String [] moviePair = key.toString().split(";");

            for (Text rating : values) {
                double r1 = Double.parseDouble(rating.toString().split(";")[0]);
                double r2 = Double.parseDouble(rating.toString().split(";")[1]);

                sum_xx += r1 * r1;
                sum_yy += r2 * r2;
                sum_xy += r1 * r2;
                sum_y += r2;
                sum_x += r1;
                n += 1;
            }

            double correlation = (n*sum_xy - sum_x*sum_y)/(Math.sqrt(n*sum_xx - (sum_x*sum_x)) * Math.sqrt(n*sum_yy - (sum_y*sum_y)));
            
            context.write(key, new Text(Double.toString(correlation)));
        }
    }



    
    public static void main(String[] args) throws Exception {
        //JobControl jbcntl = new JobControl("seila");
        
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(UserRatingReducer.class);
        job.setReducerClass(UserRatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "word count 2");
        job2.setJarByClass(WordCount.class);
        job2.setMapperClass(CombinationMapper.class);
        job2.setCombinerClass(FinalReducer.class);
        job2.setReducerClass(FinalReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);


        //ControlledJob ctjob1 = new ControlledJob(job, null);
        //ControlledJob ctjob2 = new ControlledJob(job2, null);
        
        //ctjob2.addDependingJob(ctjob1);

//        jbcntl.addJob(ctjob1);
//        jbcntl.addJob(ctjob2);
//        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("path"));
        
        JobClient.runJob(job);
        
        FileInputFormat.addInputPath(job2, new Path("path"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        JobClient.runJob(job2);

//        jbcntl.run();
//        
//        while (!ctjob2.isCompleted());

//        System.exit(1);
    }
}
