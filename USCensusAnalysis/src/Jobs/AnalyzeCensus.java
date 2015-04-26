package Jobs;

import JobTypes.JobType;
import JobTypes.JobTypeFactory;
import Mappers.GeneralMapper;
import Partitioners.StatePartitioner;
import Reducers.GeneralReducer;
import Writables.IntArrayWritable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public class AnalyzeCensus {

    public static void runJob(int jobType, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = JobTypeFactory.getInstance().getJobType(jobType).getJob();

        job.setJarByClass(AnalyzeCensus.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path("/home/analyzeCensus/output/" + jobType));

        job.waitForCompletion(true);
    }

    public static void runGeneralJobs(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance(new Configuration(), "Q0 - Q6");

        job.setMapperClass(GeneralMapper.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(IntArrayWritable.class);

        job.setPartitionerClass(StatePartitioner.class);

        job.setNumReduceTasks(50);

        job.setReducerClass(GeneralReducer.class);

        job.setJarByClass(AnalyzeCensus.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path("/home/analyzeCensus/output/ALL_JOBS"));

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Runs all GeneralMapper/GeneralReducer jobs
        runGeneralJobs(args);

        // Run avg num rooms
        runJob(JobType.AVG_NUM_ROOMS, args);

        // Run Elderly people
        runJob(JobType.ELDERLY_PEOPLE, args);


    }
}
