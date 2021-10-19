package utility;

import org.apache.commons.math3.stat.regression.SimpleRegression;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Locale;


public class Utilities {

    public static double getSlope(ArrayList<Double> items,int size) {
        // creating regression object, passing true to have intercept term
        SimpleRegression simpleRegression = new SimpleRegression(true);
        // passing data to the model
        // model will be fitted automatically by the class
        for( int i=0;i<size;i++)
         simpleRegression.addData(i,items.get(i));
        // querying for model parameters
        return  simpleRegression.getSlope();
    }
    public static String getWeek(String data) throws ParseException {
        //2020-02-24
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ITALIAN);
        LocalDate date = LocalDate.parse(data, df);
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = date.get(weekFields.weekOfWeekBasedYear());
        return "W"+week;
    }

    public static Integer getDay(String date){
        //mm/gg/aa
        String[] x=date.split("/");
        return Integer.parseInt(x[1]);
    }
    public static Integer getMonth(String date){
        String[] x=date.split("/");
        return Integer.parseInt(x[0]);
    }

}
