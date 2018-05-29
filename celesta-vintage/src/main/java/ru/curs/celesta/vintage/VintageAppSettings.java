package ru.curs.celesta.vintage;

import ru.curs.celesta.JythonAppSettings;
import ru.curs.celesta.java.AppSettings;

import java.util.Properties;
import java.util.Set;

public class VintageAppSettings extends JythonAppSettings {

    private final String javaScorePath;
    private final Set<String> celestaScan;

    public VintageAppSettings(Properties properties) {
        super(properties);
        this.javaScorePath = properties.getProperty("score.java.path", "").trim();
        this.celestaScan = AppSettings.extractCelestaScanFromProperties(properties);
    }


    public String getJavaScorePath() {
        return javaScorePath;
    }

    public Set<String> getCelestaScan() {
        return celestaScan;
    }
}
