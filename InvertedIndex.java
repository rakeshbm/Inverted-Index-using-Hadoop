import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndex {

        public static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>
        {
                private Text word = new Text();
                public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
                {
                        String[] parts = value.toString().split("\\t",2);
                        String docID = parts[0];
                        String content = parts[1];
                        content = content.replaceAll("[^a-zA-Z]+"," ");
                        content = content.toLowerCase();
                        StringTokenizer tokenizer = new StringTokenizer(content);
                        while (tokenizer.hasMoreTokens())
                        {
                                word.set(tokenizer.nextToken());
                                context.write(word, new Text(docID));
                        }
                }
        }

	public static class WordCountReducer extends Reducer<Text, Text, Text, Text>
        {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException           
                {
                        HashMap < String, Integer > postings = new HashMap < String, Integer > ();
                        int sum = 0;
                        for( Text val: values )
                        {
                                if (postings.containsKey(val.toString()))
                                {
                                        sum = postings.get(val.toString()) + 1;
                                        postings.put(val.toString(), sum);
                                }
                                else
                                {
                                        postings.put(val.toString(), 1);
                                }
                        }
                        Map < String, Integer > map = new TreeMap<>(postings);
                        String temp = "";
                        for(Map.Entry < String, Integer > entry : map.entrySet()) {
                                String doc_key = entry.getKey();
                                Integer freq_value = entry.getValue();
                                temp += " " + doc_key + ":" + freq_value;
                        }
                        context.write(key, new Text(temp));
                }
        }
        
	public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "inverted index");
                job.setJarByClass(InvertedIndex.class);
                job.setMapperClass(WordCountMapper.class);
                job.setReducerClass(WordCountReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);
                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}