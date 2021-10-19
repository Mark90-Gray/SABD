package Parser;

import entity.Outlet;

public class OutletParser {

    public static Outlet parseCSV(String csvLine) {

        String[] csvValues = csvLine.split(",");

        Outlet outlet = new Outlet(
                csvValues[0], // data
                csvValues[1], // guariti
                csvValues[2] // tamponi
        );

        return outlet;
    }
}
