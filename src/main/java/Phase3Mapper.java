import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ilan on 11/21/16.
 */
public class Phase3Mapper extends Mapper<Object, Text, Text, IntWritable> {

    private Text sitePair = new Text() ;
    private IntWritable numberOfCommonTags = new IntWritable() ;

    private void writeOut(String site1, String site2, Context context) throws IOException, InterruptedException {
        sitePair.set(site1 + "\t" + site2);
        context.write(sitePair, numberOfCommonTags);
        sitePair.set(site2 + "\t" + site1);
        context.write(sitePair, numberOfCommonTags);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        //break into the "commonTags" integer and the sites associated with it
        String[] tokens = value.toString().split("\t");
        String commonTags = tokens[0] ;
        numberOfCommonTags.set(Integer.parseInt(commonTags));

        List<String> sites = new LinkedList<>() ;
        for (int i = 1; i < tokens.length; i++) {
            sites.add(tokens[i]);
        }

        //Split the sites array to a list of "TOP_N" (10) size and one with the rest
        List<String> head ;
        List<String> tail = new ArrayList<>();
        if (sites.size() > HadoopExam.TOP_N) {
            head = sites.subList(0, HadoopExam.TOP_N);
            tail = sites.subList(HadoopExam.TOP_N + 1, sites.size());
        } else {
            head = sites.subList(0, sites.size()) ;
        }

        //Pair each siteX of the head with all sites in the tail and emit (headSite, tailSite) -> numberOfCommonTags
        for (String headSite : head) {
            for (String tailSite : tail) {
                writeOut(headSite, tailSite, context);
            }
        }

        //Now pair all sites from head with themselves
        for (int i = 0; i < head.size() -1; i++) {
            for (int j = i +1; j < head.size(); j++) {
                writeOut(head.get(i), head.get(j), context);
            }
        }
    }
}
