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

            default:
                Util.printValidCommands();
                System.exit(1);
        }
        return null;
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = getJob(args[0]);

        job.setJarByClass(AnalyzeCensus.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
