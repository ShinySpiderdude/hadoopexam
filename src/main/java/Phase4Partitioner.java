import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 *
 * Make sure all siteX go to the same reducer
 *
 */
public class Phase4Partitioner extends Partitioner<SiteSimilarity, NullWritable> {


    @Override
    public int getPartition(SiteSimilarity siteSimilarityPair,  NullWritable nullWritable, int numberOfPartitions) {
        //Make sure the hash is non-negative
        return Math.abs(siteSimilarityPair.siteX.hashCode()) % numberOfPartitions ;
    }
}
