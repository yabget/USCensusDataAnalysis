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

    public JobType getJobType(AnalysisType analysisType){

        JobType jobType = null;

        switch (analysisType){
            case NEVER_MARRIED:
                return new NeverMarried();

            case OWNED_RENTED:
                return new RentVsOwned();

            case GENDER_AGE_DIST:
                return new GenderAgeDistribution();

            case RURAL_URBAN:
                return new RuralVsUrban();

            case MEDIAN_HOUSE_VALUE:
                return new MedianHouseValue();

            case MEDIAN_RENT:
                return new MedianRent();

            case AVG_NUM_ROOMS:
                return new AvgNumRooms();

            case ELDERLY_PEOPLE:
                return new ElderlyPeople();
        }

        return jobType;
    }
}
