import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * Doesn't really do much. just writes out the output of the Mapper
 * Created by ilan on 11/21/16.
 */
public class Phase1Reducer extends Reducer<Text, Text, Text, Text> {

    private Text tabbedTags = new Text() ;

    public void reduce(Text site, Iterable<Text> tags, Context context) throws IOException, InterruptedException {
        SortedSet<String> set = new TreeSet<>() ;
        tags.forEach(tag -> set.add(tag.toString()));

        StringBuilder sb = new StringBuilder() ;
        set.forEach(tag -> sb.append(tag).append("\t"));
        tabbedTags.set(sb.toString());
        context.write(site, tabbedTags);

    }

}
