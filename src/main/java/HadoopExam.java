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

import jobs.Job1;
import jobs.Job2;
import jobs.Job3;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
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

    //A method to delete a directory, shamelessly copied from Stack overflow
    public static boolean deleteDirectory(String dir) {
        File directory = new File(dir) ;
        if(directory.exists()){
            File[] files = directory.listFiles();
            if(null!=files){
                for(int i=0; i<files.length; i++) {
                    if(files[i].isDirectory()) {
                        deleteDirectory(files[i].getAbsolutePath());
                    }
                    else {
                        files[i].delete();
                    }
                }
            }
        }
        return(directory.delete());
    }

    // A generic method to create a MapReduce job
    public static Job job(String jobName, String inPath, String outPath, Class mapper, Class reducer,
                          Class outputKeyClass, Class outputValueClass) throws IOException {
        Configuration conf = new Configuration() ;
        Job job = Job.getInstance(conf, jobName) ;
        job.setJarByClass(HadoopExam.class);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job ;
    }

    public static void main(String[] args) throws Exception {

        String inPath = args[0] ;
        String intermed1 = args[1] ;
        String intermed2 = args[2] ;
        String out = args[3] ;

        //Delete intermediates and output if they already exist
        deleteDirectory(intermed1) ;
        deleteDirectory(intermed2) ;
        deleteDirectory(out) ;

        List<Job> jobs = new LinkedList<>() ;
        jobs.add(job("job1", inPath, intermed1, Job1.TagToSitesMapper.class, Job1.SiteCombinationsReducer.class, Text.class, Text.class)) ;
        jobs.add(job("job2", intermed1, intermed2, Job2.SiteSumMapper.class, Job2.SiteSumReducer.class, Text.class, Text.class));
        jobs.add(job("job3", intermed2, out, Job3.CompositeKeyMapper.class, Job3.SiteCombinationsReducer.class, Job3.SiteAndTagCount.class, Text.class)) ;

        for (Job job : jobs) {
            if (job.waitForCompletion(false) == false) {
                //Job failed, exit
                System.exit(1);
            }
        }

    }
}