import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 *
 * This reducer gets all sites that are similar by the given tags.
 * It will sum up the tags and emit (common-tags-number) -> sites
 *
 */
public class Phase2Reducer extends Reducer<Text, Text, IntWritable, Text> {

    private IntWritable sumOfTags = new IntWritable() ;
    private Text sitesText = new Text() ;

    public void reduce(Text tagsText, Iterable<Text> sites, Context context) throws IOException, InterruptedException {
        sumOfTags.set(tagsText.toString().split("\t").length);
        SortedSet<String> set = new TreeSet<>() ;
        sites.forEach(site -> set.add(site.toString()));
        StringBuilder sb = new StringBuilder() ;
        set.forEach(site -> sb.append(site.toString()).append("\t"));
        sitesText.set(sb.toString());
        //Because we don't need to emit a set of tags that has just siteX
        if (set.size() > 1) {
            context.write(sumOfTags, sitesText);
        }
    }

}
