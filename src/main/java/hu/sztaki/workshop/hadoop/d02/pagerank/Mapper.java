package hu.sztaki.workshop.hadoop.d02.pagerank;

import hu.sztaki.workshop.hadoop.d02.Pagerank;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @todo Finish class.
 */
public class Mapper extends org.apache.hadoop.mapreduce.Mapper<Text, Text, Text, Rank> {
    private static final double d = 0.85;
    private static Integer n = 100000;

    /**
     * @todo Finish method.
     * @hint Distinguish your rank-computation based on first round.
     * @hint Then for each outlink write out...
     *       1) key's rank,
     *       2) and key with the outlink.
     */
    public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer st = new StringTokenizer(value.toString(), Pagerank.separator);
        int degree = st.countTokens();
        Double rank;

        int current_round =
                context.getConfiguration().getInt("hu.sztaki.workshop.hadoop.day3.pagerank.current_iteration", 0);
        if (current_round == 0){
            rank = (1.0/degree) * d * (1.0/n) + (1-d) * (1.0/n);
        }
        else {
            double currentRank = Double.parseDouble(st.nextToken());
            rank = (1.0/degree) * d * currentRank + (1-d) * (1.0/n);
        }

        Rank rankValue = new Rank(new DoubleWritable(rank));
        Rank outlinkValue;

            while (st.hasMoreTokens()){
                String outlink = st.nextToken();
                outlinkValue = new Rank(new Text(outlink));

                context.write(new Text(outlink), rankValue); //write outlink with partial rank component
                context.write(key, outlinkValue); //write key and its outlink
            }
    }
}
