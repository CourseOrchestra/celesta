package ru.curs.celesta.score;

/**
 * Grain name space.
 * <p>
 * A name space is defined as a set of sets of at least one lower case letter or digit separated by dots, f.e.
 * <b>name.space.1</b>
 * <p>
 * <i>
 * Since a grain may consist of several grain parts the name space concept deems it to be applied
 * to grain parts rather than to grains.
 * </i>
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-03-09
 */
public final class Namespace {

    public static final Namespace DEFAULT = new Namespace();

    private final String value;

    private Namespace() {
        value = "";
    }

    public Namespace(String value) {
        if (!value.matches("[a-z0-9]+(\\.[a-z0-9]+)*")) {
            throw new IllegalArgumentException("Incorrect name space format encountered: " + value);
        }
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

}
