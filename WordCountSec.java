import java.io.IOException;
import java.util.*;

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

public class WordCountSec {
    
    
    public static class CombinationMapper
    extends Mapper<Object, Text, Text, Text>{
        
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String aux = value.toString();
            String movies_line = aux.split(",,")[1];
            String [] movies = movies_line.split(",");
            String [] movie1 = null;
            String [] movie2 = null;
            
            for (int i=0; i<movies.length; i++) {
                for (int j=i+1; j<movies.length; j++) {
                    //if (movies[i].split(";").length == 2 && movies[j].split(";").length == 2) {
                        if (movies[i].compareTo(movies[j]) <= 0) {
                            movie1 = movies[i].split(";");
                            movie2 = movies[j].split(";"); 
                        } else {
                            movie2 = movies[i].split(";");
                            movie1 = movies[j].split(";");
                        }
                        context.write((new Text(movie1[0] + ";" + movie2[0])), (new Text(movie1[1] + ";" + movie2[1])));
                    //}
                }
            }

        }
    }

    public static class FinalReducer
    extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
            double sum_xx = 0.0, sum_xy = 0.0, sum_yy = 0.0, sum_x = 0.0, sum_y = 0.0;
            int n = 0;
            double correlation = 0.0;
            String [] moviePair = key.toString().split(";");

            double r1 = 0.0;
            double r2 = 0.0;
            
            for (Text rating : values) {
                String[] ratings = rating.toString().split(";");
                
                if (ratings.length > 1){
                    r1 = Double.parseDouble(ratings[0]);
                    r2 = Double.parseDouble(ratings[1]);
                    
                    sum_xx += r1 * r1;
                    sum_yy += r2 * r2;
                    sum_xy += r1 * r2;
                    sum_y += r2;
                    sum_x += r1;
                    n += 1;
                }
            }
            double numerador = (n*sum_xy - sum_x*sum_y);
            double denominador = Math.sqrt(n*sum_xx - (sum_x*sum_x)) * Math.sqrt(n*sum_yy - (sum_y*sum_y));
            if (denominador == 0) {
                correlation = 0.0;
            }else {
                correlation = numerador / denominador;
            }
            correlation = (correlation + 1)/2;

            String out = ""+correlation;
            context.write(key, new Text(out));
        }
    }

    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCountSec.class);
        job.setMapperClass(CombinationMapper.class);
        //job.setCombinerClass(FinalReducer.class);
        job.setReducerClass(FinalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);    }
}
