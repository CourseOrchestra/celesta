package ru.curs.celesta.score.discovery;

import java.util.Collections;
import java.util.Set;

import ru.curs.celesta.score.io.Resource;

public final class ScoreByScoreResourceDiscovery implements ScoreDiscovery {
    
    /*
     * TODO:
     * 1. This app. discovery
     * 2. JAR on the classpath discovery
     */

    @Override
    public Set<Resource> discoverScore() {
        //          ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        // TODO:
        return Collections.emptySet();
    }
    /*
        private static Map<String, List<String>> loadSpringFactories(@Nullable ClassLoader classLoader) {
                MultiValueMap<String, String> result = cache.get(classLoader);
                if (result != null) {
                        return result;
                }

                try {
                        Enumeration<URL> urls = (classLoader != null ?
                                        classLoader.getResources(FACTORIES_RESOURCE_LOCATION) :
                                        ClassLoader.getSystemResources(FACTORIES_RESOURCE_LOCATION));
                        result = new LinkedMultiValueMap<>();
                        while (urls.hasMoreElements()) {
                                URL url = urls.nextElement();
                                UrlResource resource = new UrlResource(url);
                                Properties properties = PropertiesLoaderUtils.loadProperties(resource);
                                for (Map.Entry<?, ?> entry : properties.entrySet()) {
                                        List<String> factoryClassNames = Arrays.asList(
                                                        StringUtils.commaDelimitedListToStringArray((String) entry.getValue()));
                                        result.addAll((String) entry.getKey(), factoryClassNames);
                                }
                        }
                        cache.put(classLoader, result);
                        return result;
                }
                catch (IOException ex) {
                        throw new IllegalArgumentException("Unable to load factories from location [" +
                                        FACTORIES_RESOURCE_LOCATION + "]", ex);
                }
        }
     */

}
