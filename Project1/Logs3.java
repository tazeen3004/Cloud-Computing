package Logs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Logs3 {
    public static class logs3Mapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        private final static IntWritable one=new IntWritable(1);
        private Text name=new Text();
        public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException{
            StringTokenizer stok=new StringTokenizer(value.toString(),"\n");
            while(stok.hasMoreTokens()){
                String [] sentence=stok.nextToken().split("\"");
                String address=sentence[1];
                name.set(address);
                context.write(name, one);
            }
            }
            }
    public static class logs3Reduce extends  Reducer <Text,IntWritable,Text,IntWritable> {
        private IntWritable answer =new IntWritable();
        int num=0;
        Text txt=new Text();
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
            int add=0;
            for(IntWritable value :values){
                add=add+value.get();
            }
            if(add>num){
                num=add;
                txt.set(key);
            }
            }
        protected void cleanup(Context context)throws IOException, InterruptedException{
            answer.set(num);
            context.write(txt, answer);
        }
    }
    public static void main(String []args) throws IOException, ReflectiveOperationException, InterruptedException{
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf, "Logs3");
        job.setJarByClass(Logs3.class);
        job.setMapperClass(logs3Mapper.class);
        job.setReducerClass(logs3Reduce.class);
        job.setCombinerClass(logs3Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}