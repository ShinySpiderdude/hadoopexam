import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 *
 * This reducer gets the maximum of the values mapped in the mapper phase and emits it with the pair
 *
 */
public class Phase3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable() ;

    public void reduce(Text sitePair, Iterable<IntWritable> similarities, Context context) throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE ;
        for (IntWritable iw : similarities) {
            int num = iw.get() ;
            if (num > max) {
                max = num ;
            }
        }
        result.set(max);
        context.write(sitePair, result);
    }
}
