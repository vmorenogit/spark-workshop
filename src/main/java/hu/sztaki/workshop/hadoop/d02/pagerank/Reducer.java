package hu.sztaki.workshop.hadoop.d02.pagerank;

import org.apache.hadoop.io.Text;

/**
 * @todo Finish class.
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Rank, Text, Text> {
    /**
     * @todo Implement method.
     * @hint We receive 2 different types of data associated with a given key:
     *       1) Key's PageRank values (doubles) that we need to sum together,
     *          which is actually the votes for this key.
     *       2) Outlinks from this key.
     * @hint All you need to do is to concatenate and write out in a correct format.
     */
    @Override
    protected void reduce(Text key, Iterable<Rank> values, Context context){

    }
}