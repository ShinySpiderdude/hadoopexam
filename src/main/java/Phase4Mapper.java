import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ilan on 11/22/16.
 */
public class Phase4Mapper extends Mapper<Object, Text, SiteSimilarityPair, Text> {

    private SiteSimilarityPair ssp = new SiteSimilarityPair() ;
    private Text siteY = new Text() ;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        ssp.setSite(tokens[0]); ;
        siteY.set(tokens[1]) ;
        ssp.setSimilarity(Integer.parseInt(tokens[2]));

        context.write(ssp, siteY);
    }
}
