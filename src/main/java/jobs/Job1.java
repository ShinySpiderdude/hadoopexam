package jobs;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ilan on 11/20/16.
 */
public class Job1 {

    public static class TagToSitesMapper extends Mapper<Object, Text, Text, Text> {

        private Text tag = new Text();
        private Text site = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t") ;
            site.set(tokens[0]);
            tag.set(tokens[1]);

            context.write(tag, site);
        }
    }

    public static class SiteCombinationsReducer extends Reducer<Text, Text, Text, Text> {

        private Text keyOut1 = new Text() ;
        private Text keyOut2 = new Text() ;

        public void reduce(Text tag, Iterable<Text> sites, Context context) throws IOException, InterruptedException {

            //As stated above, i need to copy all the iterable sites into a list
            //I'm using an ArrayList to cache the "Iterable" (Rather than, say, a "LinkedList") since i'm going to do
            //a lot more "gets" than "add"s
            List<String> list = new ArrayList<>();
            sites.forEach(value -> list.add(value.toString()));

            //Emit pairs of [(siteX, siteY), tag]). If a tag has just one site associated, nothing will be emitted
            for (int i = 0; i < list.size() - 1; i++) {
                for (int j = i + 1; j < list.size(); j++) {

                    //Site1\tSite2
                    keyOut1.set(list.get(i) + "\t" + list.get(j));
                    context.write(keyOut1, tag);

                    //Site2\tSite1
                    keyOut2.set(list.get(j) + "\t" + list.get(i));
                    context.write(keyOut2, tag);
                }
            }

        }
    }

}
