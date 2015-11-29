package hu.sztaki.workshop.hadoop.d02.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.Iterator;

public class Converter extends Configured implements Tool {
    /**
     * @todo Complete class.
     */
    public static class Map extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String values[] = value.toString().split("[ \t\r\n\f]");

            context.write(new Text(values[0]), new Text(values[1]));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String links = "";
            Iterator iterator = values.iterator();
            while(iterator.hasNext()){
                links += iterator.next().toString();
                if(iterator.hasNext())
                    links += ",";
            }

            context.write(key, new Text(links));
        }
    }

    public int run(String[] args) throws Exception {
        FileSystem.get(getConf()).delete(new Path(args[1]), true);

        Job job = Job.getInstance(getConf());
        job.getConfiguration().set("mapreduce.output.textoutputformat.separator", "#");

        job.setJobName("Convert graph-format for PageRank");
        job.setJarByClass(Converter.class);

        SequenceFileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(Converter.Map.class);
        job.setReducerClass(Converter.Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new Converter(), args));
    }
}
