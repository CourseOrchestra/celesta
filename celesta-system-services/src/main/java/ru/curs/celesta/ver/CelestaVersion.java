package ru.curs.celesta.ver;

/**
 * Celesta version holder.
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-04-11
 */
public final class CelestaVersion {

    /**
     * Celesta version, f.e. <code>7.1.2</code>.
     */
    public static final String VERSION;

    static {
        VERSION = CelestaVersion.class.getPackage().getSpecificationVersion();
    }

    private CelestaVersion() {
    }

}
