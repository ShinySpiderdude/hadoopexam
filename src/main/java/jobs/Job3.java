package jobs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ilan on 11/20/16.
 */
public class Job3 {

    public static int TOP_SIMILAR_SITES_FOR_SITE = 2 ;

    public static class SiteAndTagCount implements WritableComparable<SiteAndTagCount> {
        // Some data
        private String site;
        private int tagCounter;

        public void set(String site, int tagCounter) {
            this.site = site ;
            this.tagCounter = tagCounter ;
        }

        public String getSite() {
            return site ;
        }

        public int getTagCounter() {
            return tagCounter ;
        }


        public void write(DataOutput out) throws IOException {
            out.writeUTF(site);
            out.writeInt(tagCounter);
        }

        public void readFields(DataInput in) throws IOException {
            site = in.readUTF();
            tagCounter = in.readInt();
        }

        public int compareTo(SiteAndTagCount o) {
            if (!this.site.equals(o.site)) {
                return this.site.compareTo(o.site) ;
            }
            //We multiply by -1 to make the order descending (rather than ascending)
            return new Integer(this.tagCounter).compareTo(o.tagCounter) * -1 ;
        }

        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + site.hashCode();
            result = prime * result + (tagCounter ^ (tagCounter >>> 32));
            return result ;
        }

    }

    public static class CompositeKeyMapper extends Mapper<Object, Text, SiteAndTagCount, Text> {

        private SiteAndTagCount siteAndTagCount = new SiteAndTagCount() ;
        private Text similarText = new Text() ;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t") ;
            String site = tokens[0] ;
            String similar = tokens[1] ;
            int tagCounter = Integer.parseInt(tokens[2]) ;

            siteAndTagCount.set(site, tagCounter);
            similarText.set(similar);
            context.write(siteAndTagCount, similarText);
        }

    }

    public static class SiteCombinationsReducer extends Reducer<SiteAndTagCount, Text, Text, IntWritable> {

        //The site we're currently probing
        private String currentSite = "" ;
        //How many times have we seen this site already
        private int counter = 0 ;

        private Text siteAndSimilar = new Text() ;
        private IntWritable tagCount = new IntWritable() ;

        public void reduce(SiteAndTagCount siteAndTagCounter, Iterable<Text> similars, Context context) throws IOException, InterruptedException {
            String site = siteAndTagCounter.getSite() ;
            if (!site.equals(currentSite)) {
                currentSite = site ;
                counter = 0 ;
            }

            //Just an optimization, we can do without it as well. If we have already seen the top 10 similar sites,
            //we don't need to enter the loop that follows...
            if (counter >= TOP_SIMILAR_SITES_FOR_SITE) return ;

            for (Text similar : similars) {
                if (counter < TOP_SIMILAR_SITES_FOR_SITE) {
                    siteAndSimilar.set(site + "\t" + similar);
                    tagCount.set(siteAndTagCounter.getTagCounter());
                    context.write(siteAndSimilar, tagCount);
                    counter++;
                }
            }
        }
    }
}
