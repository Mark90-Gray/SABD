package Parser;

import entity.State;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Locale;

public class StateParser {

     public static State parseCSV2(String csvLine) {

        State state = null;
        Integer x=0,y,z,r;
        String[] csvValues = csvLine.split(",");
        if(csvValues[x].isEmpty() || csvValues[x].equals(" ") || csvValues[x].toLowerCase().equals("nan"))
            x=1;
        ArrayList<Integer> sick_number=new ArrayList<>();
        for(int i=4;i<csvValues.length;i++){
            if(i==4) {
                sick_number.add(Integer.parseInt(csvValues[i]));
            }
            else {
                y=Integer.parseInt(csvValues[i]);
                z=Integer.parseInt(csvValues[(i-1)]);
                //Francia: 167605, 165093--> 167605, 167605 elimino il dato di quel giorno considerando 0 contagiati
                if(y<z)
                    r=0;
                else
                    r=y-z;

                sick_number.add(r);
            }


        }
        state = new State(
                csvValues[x].toLowerCase(), // stato se c'è solo country metti anche qui country
                csvValues[1].toLowerCase(), // country
                sick_number
        );

        return state;
    }

    public static String getWeek(String data) throws ParseException {
        //2020-02-24
        String[] d=data.split("/");

        LocalDate date = LocalDate.of(Integer.parseInt(d[2]),Integer.parseInt(d[0]),Integer.parseInt(d[1]));
        WeekFields weekFields = WeekFields.of(Locale.getDefault());
        int week = date.get(weekFields.weekOfWeekBasedYear());
        return "W"+week;

    }

    public static ArrayList<String> convertDatetoWeek(ArrayList<String> date){
        ArrayList<String> dates_week=new ArrayList<>();
        for(int i=0;i<date.size();i++) {
            try {
                dates_week.add(getWeek(date.get(i)));
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        return dates_week;
    }

}
