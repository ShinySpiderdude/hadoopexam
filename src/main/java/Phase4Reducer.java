import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ilan on 11/22/16.
 */
public class Phase4Reducer  extends Reducer<SiteSimilarity, NullWritable, Text, IntWritable> {

    private String currentSite = "" ;
    private int counter = 0 ;

    private Text sites = new Text() ;
    private IntWritable similarity = new IntWritable() ;

    @Override
    protected void reduce(SiteSimilarity key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //Reset the current siteX every time we get a new one
        if (!key.siteX.toString().equals(currentSite)) {
            currentSite = key.siteX.toString() ;
            counter = 0 ;
        }

        //Emit the result only if the counter is smaller than the "TOP N" (10) sites we want
        if (counter < HadoopExam.TOP_N) {
            String siteX = key.siteX.toString();
            //There should be just one value per entry
            String siteY = key.siteY.toString() ;

            sites.set(siteX + "\t" + siteY);

            similarity.set(key.similarity);
            context.write(sites, similarity);
            counter++ ;
        }
    }
}
