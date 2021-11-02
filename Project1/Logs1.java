package Logs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Logs1 {
    public static class log1map extends Mapper<LongWritable,Text,Text,IntWritable>{
        private Text name=new Text();
        private final static IntWritable one = new IntWritable(1);
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
            StringTokenizer stok=new StringTokenizer(value.toString(),"\n");
            while(stok.hasMoreTokens()){
                String [] sentence=stok.nextToken().split("\"");
                String address=sentence[1];
                if (address.contains("/assets/img/home-logo.png")){
                    name.set("/assets/img/home-logo.png");
                    context.write(name, one);
                }
                }
                }
                }
    public static class log1reduce extends  Reducer <Text,IntWritable,Text,IntWritable> {
        private IntWritable answer=new IntWritable();
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int add=0;
            for(IntWritable value :values){
                add=add+value.get();
            }
            answer.set(add);
            context.write(key, answer);
            }
            }
    public static void main(String []args) throws IOException, ReflectiveOperationException, InterruptedException{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf, "Logs1");
        job.setJarByClass(Logs1.class);
        job.setMapperClass(log1map.class);
        job.setReducerClass(log1reduce.class);
        job.setCombinerClass(log1reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}