import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Map to site -> [taglist]
 */
public class Phase1Mapper extends Mapper<Object, Text, Text, Text> {

    private Text site = new Text();
    private Text tag = new Text();

    //
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\t") ;
        site.set(tokens[0]);
        tag.set(tokens[1]);

        context.write(site, tag);
    }

}
