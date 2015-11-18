package hu.sztaki.workshop.hadoop.d02.pagerank;

import hu.sztaki.workshop.hadoop.d02.Pagerank;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @todo Finish class.
 */
public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Rank, Text, Text> {
    /**
     * @todo Implement method.
     * @hint We receive 2 different types of data associated with a given key:
     *       1) Key's PageRank values (doubles) that we need to sum together.
     *       2) Outlinks from this key.
     * @hint All you need to do is to concatenate and write out in a correct format.
     */
    @Override
    protected void reduce(Text key, Iterable<Rank> values, Context context) throws IOException, InterruptedException {
        Double sum = 0.0;
        ArrayList<String> outlinks = new ArrayList<String>();

        for (Rank value : values){

            if (!value.isOutlink()){
                sum += value.getRank().get();
            }
            else {
                Text copy = new Text(value.getOutlink());
                outlinks.add(copy.toString());
            }
        }

        String val = sum.toString();
        for (String outlink : outlinks){
            val += Pagerank.separator;
            val += outlink;
        }

            context.write(key, new Text(val));
    }
}