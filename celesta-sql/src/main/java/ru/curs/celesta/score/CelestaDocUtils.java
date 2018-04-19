package ru.curs.celesta.score;

import org.json.JSONArray;
import org.json.JSONObject;
import ru.curs.celesta.CelestaException;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public final class CelestaDocUtils {

    public static final String OPTION = "option";
    public static final String IMPLEMENTS = "implements";

    private CelestaDocUtils() {
        throw new AssertionError();
    }

    /**
     * Extracts first occurence of JSON object string from CelestaDoc.
     *
     * @throws CelestaException Broken or truncated JSON.
     */
    public static String getCelestaDocJSON(String celestaDoc) throws CelestaException {

        if (celestaDoc == null)
            return "{}";
        StringBuilder sb = new StringBuilder();
        int state = 0;
        int bracescount = 0;
        for (int i = 0; i < celestaDoc.length(); i++) {
            char c = celestaDoc.charAt(i);
            switch (state) {
                case 0:
                    if (c == '{') {
                        sb.append(c);
                        bracescount++;
                        state = 1;
                    }
                    break;
                case 1:
                    sb.append(c);
                    if (c == '{') {
                        bracescount++;
                    } else if (c == '}') {
                        if (--bracescount == 0)
                            return sb.toString();
                    } else if (c == '"') {
                        state = 2;
                    }
                    break;
                case 2:
                    sb.append(c);
                    if (c == '\\') {
                        state = 3;
                    } else if (c == '"') {
                        state = 1;
                    }
                    break;
                case 3:
                    sb.append(c);
                    state = 2;
                    break;
                default:
            }
        }
        // No valid json!
        if (state != 0)
            throw new CelestaException("Broken or truncated JSON: %s", sb.toString());
        return "{}";
    }


    public static List<String> getList(String celestaDoc, String key) throws CelestaException {
        String json = getCelestaDocJSON(celestaDoc);

        JSONObject metadata = new JSONObject(json);
        if (metadata.has(key)) {
            JSONArray options = metadata.getJSONArray(key);
            return options.toList().stream().map(String::valueOf).collect(Collectors.toList());
        } else {
            return Collections.emptyList();
        }
    }

}
