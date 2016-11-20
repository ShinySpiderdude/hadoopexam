package jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by ilan on 11/20/16.
 */
public class Job2 {

    public static class SiteSumMapper extends Mapper<Object, Text, Text, Text> {

        private Text siteAndSimilar = new Text() ;
        private Text tag = new Text() ;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t") ;
            siteAndSimilar.set(tokens[0] + "\t" + tokens[1]);
            tag.set(tokens[2]);

            context.write(siteAndSimilar, tag);
        }

    }

    public static class SiteSumReducer extends Reducer<Text, Text, Text, IntWritable> {

        private IntWritable sum = new IntWritable();

        public void reduce(Text siteAndSimilar, Iterable<Text> tags, Context context) throws IOException, InterruptedException {
            int totalSimilarTags = 0 ;
            for (Text tag : tags) {
                totalSimilarTags++ ;
            }
            sum.set(totalSimilarTags);
            context.write(siteAndSimilar, sum);

        }


    }
}
