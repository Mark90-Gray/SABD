package entity;

import java.io.Serializable;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.Locale;

public class Outlet implements Serializable {

    private String data;
    private String healed;
    private String swabs;

    public Outlet(String data, String guariti, String tamponi) {
        this.data = data;
        this.healed = guariti;
        this.swabs = tamponi;

    }

    public String getData() {
        return data;
    }

   public LocalDate getDateTime(){
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ITALIAN);
        LocalDate date = LocalDate.parse(data, df);
        return date;
    }

    public String getWeek() throws ParseException {


        //2020-02-24T18:00:00
        DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss", Locale.ITALIAN);
        LocalDate date = LocalDate.parse(data, df);
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = date.get(weekFields.weekOfWeekBasedYear());
        String w= "W"+week;
        return w;
    }

    public Integer getHealed() {
        int guar;
        try {
            guar = Integer.parseInt(healed);
        }
        catch (NumberFormatException e)
        {
            guar = 0;
        }
        return guar;
    }

    public Integer getSwabs() {
        int tamp;
        try {
            tamp = Integer.parseInt(swabs);
        }
        catch (NumberFormatException e)
        {
            tamp = 0;
        }
        return tamp;
    }



    @Override
    public String toString(){
        try {
            return getWeek() + ", " + getHealed() + ", " + getSwabs();
        } catch (ParseException e) {
            e.printStackTrace();
            return "";
        }
    }

}
