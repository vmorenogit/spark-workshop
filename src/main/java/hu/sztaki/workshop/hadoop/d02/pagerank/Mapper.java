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
     * @hint Calculate initial rank as: rank = (1.0/degree) * d * (1.0/n) + (1-d) * (1.0/n)
     * @hint Calculate rank in other cases as: rank = (1.0/degree) * d * currentRank + (1-d) * (1.0/n);
     * @hint Set `d`, the dumping factor to 0.85.
     */
    public void map(Text key, Text value, Context context){
        StringTokenizer st = new StringTokenizer(value.toString(), Pagerank.separator);
        int degree = st.countTokens();
        Double rank;

        int current_round =
                context.getConfiguration()
                        .getInt("hu.sztaki.workshop.spark.d02.pagerank.current_iteration", 0);

        if(current_round == 0) {
            rank = (1.0/degree) * d * (1.0/n) + (1-d) * (1.0/n);
        } else {
            double currentRank = Double.parseDouble(st.nextToken());
            rank = (1.0/degree) * d * currentRank + (1-d) * (1.0/n);
        }

        Rank rankValue = new Rank(new DoubleWritable(rank));
        Rank outlinkValue;

        while(st.hasMoreTokens()) {
            String outlink = st.nextToken();
            outlinkValue = new Rank(new Text(outlink));

            try {
                context.write(new Text(outlink), rankValue);
                context.write(key, outlinkValue);
            } catch (IOException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
