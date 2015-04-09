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

            case AnalysisType.RURAL_URBAN:
                return new RuralVsUrban();

            case AnalysisType.MEDIAN_HOUSE_VALUE:
                return new MedianHouseValue();

            case AnalysisType.MEDIAN_RENT:
                return new MedianRent();
        }

        return jobType;
    }
}
