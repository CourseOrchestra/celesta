package ru.curs.celesta;

import java.util.*;

public class AppSettings extends BaseAppSettings {

    private final Set<String> celestaScan = new LinkedHashSet<>();

    public AppSettings(Properties properties) {
        super(properties);

        String celestaScanProperty = properties.getProperty("celestaScan");
        if (celestaScanProperty != null)
            Arrays.stream(celestaScanProperty.split(","))
                    .map(String::trim)
                    .forEach(celestaScan::add);
    }

    public Set<String> getCelestaScan() {
        return Collections.unmodifiableSet(celestaScan);
    }
}
