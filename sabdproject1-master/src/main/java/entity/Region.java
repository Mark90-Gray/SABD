package entity;

public class Region {
    private String region;
    private String country_region;

    public Region(String region, String country) {
        this.region = region;
        this.country_region = country;
    }

    public String getRegion() {
        return region;
    }

    public String getCountryRegion() {
        return country_region;
    }
}
