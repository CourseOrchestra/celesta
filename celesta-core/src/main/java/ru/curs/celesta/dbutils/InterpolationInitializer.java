package ru.curs.celesta.dbutils;

import ru.curs.celesta.DBType;
import ru.curs.celesta.dbutils.adaptors.DBAdaptor;
import ru.curs.lyra.grid.KeyInterpolator;

import java.math.BigInteger;
import java.util.concurrent.ThreadLocalRandom;

public abstract class InterpolationInitializer {

    private static final int MAX_REFINEMENTS_COUNT = 100;
    private static final int DEFAULT_AMOUNT_OF_INTERPOLATION_POINTS = 10;

    private int refinementsCount = 0;

    private final KeyInterpolator interpolator;
    private final DBAdaptor dbAdaptor;
    private final ThreadLocalRandom rnd = ThreadLocalRandom.current();

    public InterpolationInitializer(KeyInterpolator interpolator, DBAdaptor dbAdaptor) {
        this.interpolator = interpolator;
        this.dbAdaptor = dbAdaptor;
    }

    /**
     * Initializes the interpolation table.
     *
     * @param c {@link BasicCursor}
     * @param count
     * @return true - initialized completed, false - no need for initialization
     */
    public boolean initialize(final BasicCursor c, int count) {
        if (count == 0) {
            return false;
        } else if (DBType.POSTGRESQL.equals(dbAdaptor.getType())) {
            return initializePostgres(c, count);
        } else {
            return initializeCommon(c);
        }
    }

    private boolean initializeCommon(BasicCursor c) {
        if (refinementsCount > MAX_REFINEMENTS_COUNT) {
            return false;
        }
        refinementsCount++;
        BigInteger lav = interpolator.getLeastAccurateValue();
        if (lav == null) {
            // no refinements needed at all at the moment,
            return false;
        } else {
            setCursorOrdinal(c, lav);
            if (rnd.nextBoolean()) {
                c.navigate("=>+");
            } else {
                c.navigate("=<-");
            }
            int result = c.position();
            //System.out.printf("COUNT ISSUED %d%n", refinementsCount);
            BigInteger key = getCursorOrdinal(c);
            interpolator.setPoint(key, result);

            return true;
        }
    }

    private boolean initializePostgres(BasicCursor c, int count) {
        if (refinementsCount > 0) {
            return false;
        }
        refinementsCount = 1;

        int amountOfInterpolationPoints = (count > DEFAULT_AMOUNT_OF_INTERPOLATION_POINTS)
                ? DEFAULT_AMOUNT_OF_INTERPOLATION_POINTS : count;

        int offset = count / amountOfInterpolationPoints;

        //первая и последняя строчки заполняются извне.
        // Уменьшаем число точек на 1, т.к для равномерного покрытия N записей необходимо n + 1 точек
        amountOfInterpolationPoints -= 1;

        c.first();
        //TODO: refactor this cycle!
        for (int point = 1; point <= amountOfInterpolationPoints; ++point) {
            int rowNumber = point * offset;

            //Срабатывает, когда число точек в интерполяционной таблице совпадает с числом записей в таблице БД
            if (rowNumber == count) {
                break;
            }

            c.navigate(">", offset);
            //System.out.printf("OFFSET SELECT ISSUED %d%n", point);
            BigInteger key = getCursorOrdinal(c);
            interpolator.setPoint(key, rowNumber);
        }

        return true;
    }

    abstract void setCursorOrdinal(BasicCursor c, BigInteger key);

    abstract BigInteger getCursorOrdinal(BasicCursor c);

}
