package gemma.org.PhraseCounter;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;

public class PhraseCounter {

    public static class SplitWords extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Configuration conf = context.getConfiguration();
            int phraseLength = conf.getInt("phraseLength", 1);

            StringTokenizer itr = new StringTokenizer(value.toString());
            int numberOfTokens = itr.countTokens();
            String [] words = new String[numberOfTokens];
            for (int i = 0; i < numberOfTokens; i++) {
                words[i] = itr.nextToken();
            }

            for (int i = 0; i <= numberOfTokens - phraseLength; i++) {
                StringBuilder phrase = new StringBuilder();
                for (int j = 0; j < phraseLength; j++) {
                    phrase.append(" " + words[i + j]);
                }
                context.write(new Text(phrase.toString()), one);
            }

        }
    }

    public static class CountPhrases extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("mapred.job.queue.name", "default");

        if (args.length != 3) {
            throw new RuntimeException("Need three arguments (input, output folders on HDFS, and the phrase length)");
        }

        Path input_folder = new Path(args[0]);
        Path output_folder = new Path(args[1]);

        int phraseLength = Integer.parseInt(args[2]);

        conf.setInt("phraseLength", phraseLength);

        // configuration should contain reference to your namenode
        FileSystem fs = FileSystem.get(new Configuration());
        // true stands for recursively deleting the folder you gave
        if (!fs.exists(input_folder)) {
            throw new RuntimeException("Input folder does not exist on HDFS filesystem");
        }

        // Delete output folder, if it exists
        if (fs.exists(output_folder)) {
            //throw new RuntimeException("Output folder already exist on HDFS filesystem");
            fs.delete(output_folder, true);
        }

        Job job = Job.getInstance(conf, "Count phrases");

        job.setJarByClass(PhraseCounter.class);
        // map class
        job.setMapperClass(SplitWords.class);
        // reduce class
        job.setReducerClass(CountPhrases.class);
        // return types from map and reduce classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, input_folder);
        FileOutputFormat.setOutputPath(job, output_folder);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
