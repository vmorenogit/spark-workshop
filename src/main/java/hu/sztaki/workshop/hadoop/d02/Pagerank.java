package hu.sztaki.workshop.hadoop.d02;

import org.apache.hadoop.conf.Configured;
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

    public int run(String args[]) throws Exception{

        return 0;
    }

    public static void main(String[] args) throws Exception {
        LOG.setLevel(Level.INFO);
        LOG.info("Starting main.");
        System.exit(ToolRunner.run(new Pagerank(), args));
    }
}
