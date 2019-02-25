package ru.curs.celesta;

import java.util.*;

/**
 * Application settings.
 */
public final class AppSettings extends BaseAppSettings {

    private final Set<String> celestaScan;

    public AppSettings(Properties properties) {
        super(properties);
        this.celestaScan = extractCelestaScanFromProperties(properties);
    }

    public static Set<String> extractCelestaScanFromProperties(Properties properties) {
        final String celestaScanProperty = properties.getProperty("celestaScan");
        final Set<String> celestaScanVar = new LinkedHashSet<>();

        if (celestaScanProperty != null) {
            Arrays.stream(celestaScanProperty.split(","))
                    .map(String::trim)
                    .forEach(celestaScanVar::add);
        }

        return Collections.unmodifiableSet(celestaScanVar);
    }

    public Set<String> getCelestaScan() {
        return this.celestaScan;
    }

}
