package ucsc.sandhya.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class wordcount extends Configured implements Tool {

	public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable totalWordCount = new IntWritable();

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
		InterruptedException {
			int wordCount = 0;
			Iterator<IntWritable> it = values.iterator();
			while (it.hasNext()) {
				wordCount += it.next().get();
			}
			totalWordCount.set(wordCount);
			context.write(key, totalWordCount);
		}
	}

	public static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
		private Text word = new Text();
		private final static IntWritable one = new IntWritable(1);

		private static enum Counters { INPUT_WORDS }

		public void map(Object key, Text value,  OutputCollector<Text, IntWritable> output, Reporter reporter) 
				throws IOException, InterruptedException {
			//System.err.println(String.format("[map] key: (%s), value: (%s)", key, value));
			// break each sentence into words, using the punctuation characters shown
			StringTokenizer tokenizer = new StringTokenizer(value.toString(), " \t\n\r\f,.:;?![]'()_-&");
			while (tokenizer.hasMoreTokens())
			{
				// make the words lowercase so words like "an" and "An" are counted as one word
				String s = tokenizer.nextToken().toLowerCase().trim();
				System.err.println(String.format("[map, in loop] token: (%s)", s));
				word.set(s);
				output.collect(word, one);
				reporter.incrCounter(Counters.INPUT_WORDS, 1);
			}
	 }
	}

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}



		/*
		 * 
		 * public static int run(Configuration conf,
                      Tool tool,
                      String[] args)
               throws Exception
			Runs the given Tool by Tool.run(String[]), after parsing with the given generic arguments. 
			Uses the given Configuration, or builds one if null. 
			Sets the Tool's configuration with the possibly modified version of the conf.
		 */

		wordcount c = new wordcount();

		int res = ToolRunner.run(new Configuration(), c, args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		Job job = Job.getInstance(getConf());
		job.setJarByClass(wordcount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(WordMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setCombinerClass(SumReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputDirRecursive(job, true);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		return 0;
	}

}