package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class VersionStringTest {

    @Test
    public void test1() {
        assertAll(
                () -> assertThrows(ParseException.class,
                        () -> new VersionString("")),
                () -> assertThrows(ParseException.class,
                        () -> new VersionString("aerwreqw")));
    }

    @Test
    public void test2() throws ParseException {
        VersionString vs, vs2;
        vs = new VersionString("1.23,TITAN3.34");

        vs2 = new VersionString("1.23,TITAN3.35");
        assertEquals(VersionString.ComparisionState.LOWER, vs.compareTo(vs2));

        vs2 = new VersionString("1.24,TITAN3.34");
        assertEquals(VersionString.ComparisionState.LOWER, vs.compareTo(vs2));

        vs2 = new VersionString("1.23,TITAN3.34,PLUTO1.00");
        assertEquals(VersionString.ComparisionState.LOWER, vs.compareTo(vs2));

        vs2 = new VersionString("TITAN3.34,1.23");
        assertEquals(VersionString.ComparisionState.EQUALS, vs.compareTo(vs2));
        assertEquals(vs.hashCode(), vs2.hashCode());

        vs2 = new VersionString("1.22,TITAN3.34");
        assertEquals(VersionString.ComparisionState.GREATER, vs.compareTo(vs2));
        vs2 = new VersionString("1.22");
        assertEquals(VersionString.ComparisionState.GREATER, vs.compareTo(vs2));

        vs2 = new VersionString("1.22,TITAN3.36");
        assertEquals(VersionString.ComparisionState.INCONSISTENT,
                vs.compareTo(vs2));

        vs2 = new VersionString("1.22,TITAN3.34,PLUTO1.00");
        assertEquals(VersionString.ComparisionState.INCONSISTENT,
                vs.compareTo(vs2));

        vs2 = new VersionString("1.23,PLUTO1.00");
        assertEquals(VersionString.ComparisionState.INCONSISTENT,
                vs.compareTo(vs2));

        vs2 = new VersionString("1.25");
        assertEquals(VersionString.ComparisionState.INCONSISTENT,
                vs.compareTo(vs2));
    }

    @Test
    public void test3() throws ParseException {
        VersionString vs, vs2;
        vs = new VersionString("A1.222,B2.334,C3.45");

        vs2 = new VersionString("A1.222,C3.45,B2.334");
        assertEquals(vs, vs2);
        assertEquals(vs.hashCode(), vs2.hashCode());

        vs2 = new VersionString("C3.45,A1.222,B2.334");
        assertEquals(vs, vs2);
        assertEquals(vs.hashCode(), vs2.hashCode());

        vs2 = new VersionString("C3.435,A1.222,B2.334");
        assertFalse(vs.equals(vs2));
        assertFalse(vs.hashCode() == vs2.hashCode());
    }
}
