package entity;

import java.io.Serializable;
import java.util.ArrayList;

public class State implements Serializable {

    private String state;
    private String country;
    //private String lon;
    //private String lat;
    private ArrayList<Integer> sick_number;


    public State(String state, String country, ArrayList<Integer> sick_number) {
        this.state = state;
        this.country=country;
//        this.lon = lon;
//        this.lat = lat;
        this.sick_number = sick_number;
    }

    public String getCountry() {
        return country;
    }

    public String getState() {
        return state;
    }


//    public Float getLon() {
//        return Float.parseFloat(lon);
//    }
//
//    public Float getLat() {
//        return Float.parseFloat(lat);
//    }



    public ArrayList<Integer> getSick_number() {
        return sick_number;
    }

    @Override
    public String toString() {
        return "State{" +
                "state'=" + state + '\'' +
                "country='" + country + '\'' +
                ", sick_number='" + sick_number + '\'' +
                '}';
    }
}
