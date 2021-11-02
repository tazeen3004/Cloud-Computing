package ngrams;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class Ngrams {

    public static class ngramsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text name = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            int i;
            int size=2;
            StringTokenizer stok = new StringTokenizer(value.toString());
            while (stok.hasMoreTokens()) {
                String txt=stok.nextToken();
                int length=txt.length();
                for (i=0; i<=(length-size); i++) {
                    String found=txt.substring(i,i+(size));
                    name.set(found);
                    context.write(name, one);
                }
                }
                }
                }

    public static class ngramsReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable answer=new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int add=0;
            for (IntWritable value : values) {
                add+=value.get();
            }
            answer.set(add);
            context.write(key, answer);
        }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Job job = Job.getInstance(conf, "ngram");
        job.setJarByClass(Ngrams.class);
        job.setMapperClass(ngramsMapper.class);
        job.setCombinerClass(ngramsReducer.class);
        job.setReducerClass(ngramsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}