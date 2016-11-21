import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ilan on 11/21/16.
 */
public class Phase2Mapper extends Mapper<Object, Text, Text, Text> {

    private Text siteText = new Text() ;
    private Text tagsText = new Text() ;

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String tokens[] = value.toString().split("\t");
        String site = tokens[0] ;
        List<String> tags = new ArrayList<>() ;
        for (int i = 1; i < tokens.length; i++) {
            tags.add(tokens[i]);
        }

        siteText.set(site);

        //Now permute on the tags and emit:
        //(tag1) -> siteX
        //(tag1 tag2) -> siteX
        //pairs
        for (int i = 0; i < tags.size(); i++) {
            StringBuilder sb = new StringBuilder();
            for (int j = i; j < tags.size(); j++) {
                sb.append(tags.get(j));
                tagsText.set(sb.toString());
                context.write(tagsText, siteText);
                sb.append("\t") ;
            }
        }
    }

}
