/**
 * Created by ydubale on 4/6/15.
 */
public class JobTypeFactory {
    private static JobTypeFactory ourInstance = new JobTypeFactory();

    public static JobTypeFactory getInstance() {
        return ourInstance;
    }

    private JobTypeFactory() {
    }

    public JobType getJobType(int analysisType){

        JobType jobType = null;

        switch (analysisType){
            case AnalysisType.NEVER_MARRIED:
                return new NeverMarried();
            case AnalysisType.OWNED_RENTED:
                return new RentVsOwned();
            case AnalysisType.GENDER_AGE_DIST:
                return new GenderAgeDistribution();
        }

        return jobType;
    }
}
