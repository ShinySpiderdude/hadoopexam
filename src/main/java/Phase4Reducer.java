import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ilan on 11/22/16.
 */
public class Phase4Reducer  extends Reducer<SiteSimilarityPair, Text, Text, IntWritable> {

    private String currentSite = "" ;
    private int counter = 0 ;

    private Text sites = new Text() ;
    private IntWritable similarity = new IntWritable() ;

    @Override
    protected void reduce(SiteSimilarityPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //Reset the current site every time we get a new one
        if (!key.site.toString().equals(currentSite)) {
            currentSite = key.site.toString() ;
            counter = 0 ;
        }

        //Emit the result only if the counter is smaller than the "TOP N" (10) sites we want
        if (counter < HadoopExam.TOP_X) {
            String siteX = key.site.toString();
            //There should be just one value per entry
            String siteY = values.iterator().next().toString() ;

            sites.set(siteX + "\t" + siteY);

            similarity.set(key.similarity);
            context.write(sites, similarity);
            counter++ ;
        }
    }
}
