/*
SimilarGroup test � Big Data:
Requirements: in this task you should write hadoop java map reduce job/s
The Task: calculate similar sites by count of common tags
The input will be text file (tsv format ):
Site1	tag1
Site1	tag2
Site3	tag3
...
The final output should be in text file ( top 10 similar sites per site ) � should be sorted by secondary sort.
Site1	Similar1	count-of-common-tags
Site1	Similar2	count-of-common-tags
Site2	Similar1	count-of-common-tags
�

Data:
Each website have 10 tags.
There are 20M websites.
Can be popular tags with 10k sites per one tag.

*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * So the basic idea here goes something like this:
 * 1. Map the input to the form:
 * tag1 -> site1 site2 site6
 * tag2 -> site2 site3
 * etc..
 *
 * 2. Reduce the map like so:
 * for each tag: emit all combinations of 2-sites to tag combinations. like so:
 * (site1, site2) -> tag1
 * (site2, site1) -> tag1
 * (site1, site6) -> tag1
 * (site6, site1) -> tag1
 * etc...
 * (Note that it is possible to implement "WritableComparable" here but it is easier to just output as string)
 *
 *
 * !!!
 * There's a caveat in this step. The java MR API returns an "Iterable" Object, rather than some list. The problem
 * here is that in order to emit pairs as mentioned above i have to traverse the list more than once. This is impossible
 * with the API and the only plausible way around it is to cache the results in some array, Which will consume extra
 * memory. In itself, not a very good idea but the exercise DOES state the upper bound for sites per tag is around 10K.
 * If we assume that the name of a site name uses around 20B per name (site name of around 10 characters * 2 for UTF8
 * encoding) then the total list size should be around 200KB, which is not too bad. If we want to take this number down
 * at the expense of speed we can keep a map of sitename->Long (We need an 8 byte integer since there're 20M sites) at
 * some external storage, maybe...
 * !!!
 *
 * 3. Now we need to count the number of tags for each site, we take the output above and send it through a mapper,
 * as is, so our expected output will be:
 * (site1, site2) -> tag1 tag5 tag7
 * (site2, site1) -> tag1 tag5 tag7
 * (site1, site3) -> tag3 tag8
 * etc...
 *
 * 4. We count the lot -> send it to a reducer so our output will be:
 * (site1, site2) -> 3
 * (site2, site1) -> 3
 * (site1, site3) -> 2
 * etc...
 *
 * 5. We now sort the output, we will use secondary sort by creating a composite key of the form:
 * (site1, 3) -> site2
 * (site2, 3) -> site1
 * etc...
 * This will be sorted as it is sent to the reducer. Unfortunately, we cannot rely on string comparison any longer
 * (since [site1 3] > [site1 13]). We will have to implement a comparable object to service as the key.
 *
 * 6. We are almost done. Since the input to the reducer is sorted by the [composite] key, we will have
 * sorted entries by both site and common-tag-number. We will iterate through the 10 highest entries and drop
 * the rest.
 *
 */
class HadoopExam {

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


    public static int job1(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job1");
        job.setJarByClass(HadoopExam.class);
        job.setMapperClass(HadoopExam.TagToSitesMapper.class);
        job.setReducerClass(HadoopExam.SiteCombinationsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static int job2(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job2");
        job.setJarByClass(HadoopExam.class);
        job.setMapperClass(HadoopExam.SiteSumMapper.class);
        job.setReducerClass(HadoopExam.SiteSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        if (job1(args) == 0) {
            System.exit(job2(args));
        } else {
            System.exit(1);
        }
//
//        JobControl jobControl = new JobControl("jobChain");
//
//        //System.exit(job1.waitForCompletion(true) ? 1 : 0);
//
//        ControlledJob cj1 = job1(args) ;
//
//        jobControl.addJob(cj1) ;
//
//
//        Thread jobControlThread = new Thread(jobControl);
//        jobControlThread.start();
//
//        while (!jobControl.allFinished()) {
//            System.out.println("Jobs in waiting state: " + jobControl.getWaitingJobList().size());
//            System.out.println("Jobs in ready state: " + jobControl.getReadyJobsList().size());
//            System.out.println("Jobs in running state: " + jobControl.getRunningJobList().size());
//            System.out.println("Jobs in success state: " + jobControl.getSuccessfulJobList().size());
//            System.out.println("Jobs in failed state: " + jobControl.getFailedJobList().size());
//            try {
//                Thread.sleep(5000);
//            } catch (Exception e) {
//
//            }
//
//        }
    }
}