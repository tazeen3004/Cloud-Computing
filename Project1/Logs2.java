package Logs;

import java.io.IOException;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.regex.Pattern;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.util.regex.Matcher;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Logs2{
    public static class logs2Mapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one=new IntWritable(1);
        private Text name=new Text();
        private static Pattern log_pat=Pattern
                .compile("([^ ]*) ([^ ]*) ([^ ]*) \\[([^]]*)\\]"
                        + " \"([^\"]*)\""
                        + " ([^ ]*) ([^ ]*).*");
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String sentence=((Text) value).toString();
            Matcher match=log_pat.matcher(sentence);
            if (match.matches()) {
                String ip=match.group(1);
                if(ip.equals("10.153.239.5")){
                    name.set(ip);
                    context.write(name,one);
                }
                }
                }
                }
    public static class log2Reducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable answer=new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int add = 0;
            for (IntWritable val : values) {
                add+=val.get();
            }
            answer.set(add);
            context.write(key, answer);
            }
            }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Job job = Job.getInstance(conf, "Logs2");
        job.setJarByClass(Logs2.class);
        job.setMapperClass(logs2Mapper.class);
        job.setCombinerClass(log2Reducer.class);
        job.setReducerClass(log2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
