package hu.sztaki.workshop.hadoop.d02.pagerank;

import org.apache.hadoop.io.Text;

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
        
    }
}
