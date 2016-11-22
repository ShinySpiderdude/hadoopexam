import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by ilan on 11/22/16.
 */
public class Phase4Partitioner extends Partitioner<SiteSimilarityPair, Text> {


    @Override
    public int getPartition(SiteSimilarityPair siteSimilarityPair, Text text, int i) {
        //Make sure the hash is non-negative
        return Math.abs(siteSimilarityPair.site.hashCode()) % i ;
    }
}
