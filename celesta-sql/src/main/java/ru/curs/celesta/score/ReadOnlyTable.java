package ru.curs.celesta.score;

/**
 * Read Only Table object in metadata.
 *
 * @author Pavel Perminov (packpaul@mail.ru)
 * @since 2019-07-14
 */
public final class ReadOnlyTable extends BasicTable {

    public ReadOnlyTable(GrainPart gp, String name) throws ParseException {
        super(gp, name, true);
    }

    @Override
    public boolean hasPrimeKey() {
        return !this.pk.getElements().isEmpty();
    }

}
