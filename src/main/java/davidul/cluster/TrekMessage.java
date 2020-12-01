package davidul.cluster;

public class TrekMessage {
    private final String location;
    private final String specie;
    private final String character;

    public TrekMessage(String location, String specie, String character) {
        this.location = location;
        this.specie = specie;
        this.character = character;
    }

    public String getLocation() {
        return location;
    }

    public String getSpecie() {
        return specie;
    }

    public String getCharacter() {
        return character;
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("TrekMessage{");
        sb.append("location='").append(location).append('\'');
        sb.append(", specie='").append(specie).append('\'');
        sb.append(", character='").append(character).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
