import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
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

public class WordCount {

    public static class TokenMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable counter = new IntWritable(1);
        private final Text currentWord = new Text();

        private static final Set<String> filterWords = new HashSet<>(Arrays.asList(
            "the", "and", "a", "an", "of", "is", "to", "in", "on", "for", "with", "as", "by"
        ));

        @Override
        protected void map(Object key, Text line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line.toString().toLowerCase());
            while (tokenizer.hasMoreTokens()) {
                String rawWord = tokenizer.nextToken().replaceAll("[^a-z]", "");
                if (!rawWord.isEmpty() && !filterWords.contains(rawWord)) {
                    currentWord.set(rawWord);
                    context.write(currentWord, counter);
                }
            }
        }
    }

    public static class FrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable total = new IntWritable();

        @Override
        protected void reduce(Text word, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable freq : values) {
                count += freq.get();
            }
            total.set(count);
            context.write(word, total);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job wordCountJob = Job.getInstance(conf, "word count");

        wordCountJob.setJarByClass(WordCount.class);
        wordCountJob.setMapperClass(TokenMapper.class);
        wordCountJob.setCombinerClass(FrequencyReducer.class);
        wordCountJob.setReducerClass(FrequencyReducer.class);

        wordCountJob.setOutputKeyClass(Text.class);
        wordCountJob.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(wordCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(wordCountJob, new Path(args[1]));

        System.exit(wordCountJob.waitForCompletion(true) ? 0 : 1);
    }
}
