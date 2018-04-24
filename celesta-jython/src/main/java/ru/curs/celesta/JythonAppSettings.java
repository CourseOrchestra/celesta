package ru.curs.celesta;

import java.util.Properties;

/**
 * Класс, хранящий параметры приложения. Разбирает .properties-файл.
 */
public final class JythonAppSettings extends AppSettings {
    private static final String DEFAULT_PYLIB_PATH = "pylib";

    private final String pyLibPath;
    private final String javaLibPath;

    public JythonAppSettings(Properties properties) {
        super(properties);

        StringBuffer sb = new StringBuffer();

        pyLibPath = properties.getProperty("pylib.path", DEFAULT_PYLIB_PATH).trim();
        checkEntries(pyLibPath, "pylib.path", sb);

        javaLibPath = properties.getProperty("javalib.path", "").trim();
        checkEntries(javaLibPath, "javalib.path", sb);

        if (sb.length() > 0)
            throw new CelestaException(sb.toString());
    }

    /**
     * Значение параметра "pylib.path".
     */
    public String getPyLibPath() {
        return pyLibPath;
    }

    /**
     * Значение параметра "javalib.path".
     */

    public String getJavaLibPath() {
        return javaLibPath;
    }
}
