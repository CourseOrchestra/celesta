package ru.curs.celesta;

import java.util.*;

public class AppSettings extends BaseAppSettings {

    private final Set<String> celestaScan;

    AppSettings(Properties properties) {
        super(properties);

        final String celestaScanProperty = properties.getProperty("celestaScan");
        final Set<String> celestaScanVar = new LinkedHashSet();

        if (celestaScanProperty != null) {
            Arrays.stream(celestaScanProperty.split(","))
                    .map(String::trim)
                    .forEach(celestaScanVar::add);
        }

        this.celestaScan = Collections.unmodifiableSet(celestaScanVar);
    }

    public Set<String> getCelestaScan() {
        return this.celestaScan;
    }
}
