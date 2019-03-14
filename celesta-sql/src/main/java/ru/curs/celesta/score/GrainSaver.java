package ru.curs.celesta.score;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import ru.curs.celesta.score.io.Resource;

/**
 * Persists grain to a writable resource.
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-03-10
 */
public final class GrainSaver {

    /**
     * Saves metadata content of score back to SQL-files rewriting their content.
     */
    public void save(AbstractScore score, Resource scorePath) throws IOException {
        for (Grain g : score.getGrains().values()) {
            save(g, scorePath);
        }
    }

    /**
     * Saves grain to file(s).
     *
     * @ io error
     */
    public void save(Grain grain, Resource scorePath) throws IOException {

        final String grainName = grain.getName();
        Resource output = scorePath.createRelative(grain.getNamespace())
                .createRelative(grainName + ".sql");

        for (GrainPart gp : grain.getGrainParts()) {
            Resource source = gp.getSource();
            if (scorePath.contains(source) && !source.equals(output)) {
                source.delete();
            }
        }

        OutputStream outputStream = output.getOutputStream();
        if (outputStream == null) {
            throw new IOException(String.format(
                    "Cannot save '%s' grain script to resouce %s. The resource is not writable!",
                    grainName, output.toString()));
        }
        try (PrintWriter pw = new PrintWriter(
                new OutputStreamWriter(outputStream, StandardCharsets.UTF_8))) {

            CelestaSerializer serializer = new CelestaSerializer(pw);
            serializer.save(grain);
        }

    }

}
