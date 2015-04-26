package JobTypes;

import Jobs.*;

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

    public GenericJob getJobType(int jobType){
        
        switch (jobType){
            case JobType.NEVER_MARRIED:
                return new NeverMarried();

            case JobType.OWNED_RENTED:
                return new RentVsOwned();

            case JobType.GENDER_AGE_DIST:
                return new GenderAgeDistribution();

            case JobType.RURAL_URBAN:
                return new RuralVsUrban();

            case JobType.MEDIAN_HOUSE_VALUE:
                return new MedianHouseValue();

            case JobType.MEDIAN_RENT:
                return new MedianRent();

            case JobType.AVG_NUM_ROOMS:
                return new AvgNumRooms();

            case JobType.ELDERLY_PEOPLE:
                return new ElderlyPeople();

        }

        return null;
    }
}
