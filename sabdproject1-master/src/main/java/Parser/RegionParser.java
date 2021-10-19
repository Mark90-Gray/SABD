package Parser;


import entity.Region;

public class RegionParser {

    public static Region parseCSVRegion(String line) {
        String[] strings= line.split(",");
        Region r = new Region(
                strings[1].toLowerCase().trim(), //continente
                strings[0].toLowerCase().trim()  //country
        );
        return r;

    }

}

