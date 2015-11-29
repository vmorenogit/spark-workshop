package hu.sztaki.workshop.hadoop.d02;

import hu.sztaki.workshop.hadoop.d02.pagerank.Mapper;
import hu.sztaki.workshop.hadoop.d02.pagerank.Rank;
import hu.sztaki.workshop.hadoop.d02.pagerank.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @exercise Implement the famous PageRank algorithm.
 * @input /resources/web-google-in.txt (Which is a KeyValueTextInputFormat)
 * @hint Check the input file and set configuration
 *       mapreduce.input.keyvaluelinerecordreader.key.value.separator according
 *       to the delimiter.
 * @hint Use the same separator for TextOutputFormat. Find the corresponding
 *       configuration.
 * @hint Set desired and current iteration number to a configuration as well.
 */
public class Pagerank extends Configured implements Tool {
    private static final Logger LOG = LogManager.getLogger(Pagerank.class);

    public static String separator = ",";
    public int iterations = 1;

    public int run(String args[]) throws Exception {
        Configuration conf = new Configuration();
        conf.set("hu.sztaki.workshop.hadoop.day3.pagerank.separator", separator);
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "#");
        conf.set("mapreduce.output.textoutputformat.separator", "#");
        iterations = Integer.parseInt(args[2]);
        conf.setInt("hu.sztaki.workshop.hadoop.day3.pagerank.iterations", iterations);

        FileSystem fileSystem = FileSystem.newInstance(conf);

        int i = 0;
        for(; i < iterations; i++){
            conf.setInt("hu.sztaki.workshop.hadoop.day3.pagerank.current_iteration", i);

            Job job = Job.getInstance(conf, "rank");
            job.setJarByClass(Pagerank.class);

            job.setMapperClass(Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Rank.class);

            job.setReducerClass(Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            String input_path;
            if (i == 0){
                input_path = args[0];
            } else {
                input_path = args[1] + "." + (i - 1) + ".out";
            }
            String output_path = args[1] + "." + i + ".out";
            if(fileSystem.exists(new Path(output_path))){
                fileSystem.delete(new Path(output_path), true);
            }

            FileInputFormat.addInputPath(job, new Path(input_path));
            FileOutputFormat.setOutputPath(job, new Path(output_path));

            job.waitForCompletion(true);
        }

        return 0;
    }

    public static void main(String[] args) throws Exception {
        LOG.setLevel(Level.INFO);
        LOG.info("Starting main.");
        System.exit(ToolRunner.run(new Pagerank(), args));
    }
}
