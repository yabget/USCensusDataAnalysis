import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by ydubale on 4/6/15.
 */
public class AnalyzeCensus {

    public static Job getJob(String jobAbbr) throws IOException {

        switch (jobAbbr){
            //TODO: MAKE GET JOB STATIC
            case "RvO":
                return (new RentVsOwned()).getJob();

            case "NM":
                return (new NeverMarried()).getJob();

            case "GAD":
                return (new GenderAgeDistribution()).getJob();

            case "RvU":
                return (new RuralVsUrban()).getJob();

            case "MHV":
                return (new MedianHouseValue()).getJob();

            case "MR":
                return (new MedianRent()).getJob();

            case "ANR":
                return (new AvgNumRooms()).getJob();

            case "EP":
                return (new ElderlyPeople()).getJob();

            default:
                Util.printValidCommands();
                System.exit(1);
        }
        return null;
    }

    public static void runJob(String jobName, String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = getJob(jobName);

        job.setJarByClass(AnalyzeCensus.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path("/analyzeCensus/output/" + jobName));

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String[] jobNames = {"RvO", "NM", "GAD", "RvU", "MHV", "MR", "ANR", "EP" };

        for(String jobName : jobNames){
            runJob(jobName, args);
            System.out.println("JOB " + jobName + " COMPLETED!!!!!!!!!!!!!!!!!!!!!");
        }

    }
}
