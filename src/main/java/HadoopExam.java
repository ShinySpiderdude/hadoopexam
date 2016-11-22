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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class HadoopExam {

    public static int TOP_X = 2 ;

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
        if (reducer != null) {
            job.setReducerClass(reducer);
        }
        job.setOutputKeyClass(outputKeyClass);
        job.setOutputValueClass(outputValueClass);
        FileInputFormat.addInputPath(job, new Path(inPath));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        return job ;
    }

    public static void main(String[] args) throws Exception {

        String inPath = args[0] ;
        String temp = args[1] ;

        String intermed1 = temp + "/intermed1" ;
        String intermed2 = temp + "/intermed2" ;
        String intermed3 = temp + "/intermed3" ;

        String out = args[2] ;

        //Delete intermediates and output if they already exist
        deleteDirectory(temp) ;
        deleteDirectory(out) ;

        List<Job> jobs = new LinkedList<>() ;
        jobs.add(job("phase-1", inPath, intermed1, Phase1Mapper.class, Phase1Reducer.class, Text.class, Text.class)) ;
        jobs.add(job("phase-2", intermed1, intermed2, Phase2Mapper.class, Phase2Reducer.class, Text.class, Text.class)) ;
        jobs.add(job("phase-3", intermed2, intermed3, Phase3Mapper.class, Phase3Reducer.class, Text.class, IntWritable.class)) ;

        Job phase4 = job("phase-4", intermed3, out, Phase4Mapper.class, Phase4Reducer.class, SiteSimilarityPair.class, Text.class) ;
        phase4.setPartitionerClass(Phase4Partitioner.class);
        jobs.add(phase4);

        //Chain the jobs
        for (Job job : jobs) {
            if (job.waitForCompletion(false) == false) {
                //Job failed, exit
                System.exit(1);
            }
        }

    }
}