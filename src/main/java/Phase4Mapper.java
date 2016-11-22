import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by ilan on 11/22/16.
 */
public class Phase4Mapper extends Mapper<Object, Text, SiteSimilarity, NullWritable> {

    private SiteSimilarity ssp = new SiteSimilarity() ;
    private NullWritable nullValue = NullWritable.get() ;

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t");
        ssp.setSiteX(tokens[0]); ;
        ssp.setSiteY(tokens[1]); ;
        ssp.setSimilarity(Integer.parseInt(tokens[2]));

        context.write(ssp, nullValue);
    }
}
