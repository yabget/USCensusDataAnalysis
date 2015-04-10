package Jobs;

import JobTypes.JobType;
import JobTypes.JobTypeFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public class AnalyzeCensus {

    public static void runJob(JobType jobType, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobTypeFactory.getInstance().getJobType(jobType).getJob();

        job.setJarByClass(AnalyzeCensus.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path("/analyzeCensus/output/" + jobType));

        job.waitForCompletion(false);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        for(JobType jobName : JobType.values()){
            if(jobName.equals(JobType.NOTHING)) continue;

            System.out.println("RUNNNING JOB " + jobName);
            runJob(jobName, args);
            System.out.println("JOB " + jobName + " COMPLETED\n\n\n");
        }

    }
}
