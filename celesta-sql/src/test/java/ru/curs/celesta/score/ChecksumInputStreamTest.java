package ru.curs.celesta.score;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayInputStream;

public class ChecksumInputStreamTest {

    @Test
    public void testCompositeChecksum() {
        String firstPart = "aaa";
        String secondPart = "bbb";

        ChecksumInputStream is1 = new ChecksumInputStream(new ByteArrayInputStream(firstPart.getBytes()));
        ChecksumInputStream is2 = new ChecksumInputStream(new ByteArrayInputStream(secondPart.getBytes()), is1);
        ChecksumInputStream is3 = new ChecksumInputStream(new ByteArrayInputStream(firstPart.concat(secondPart).getBytes()));

        assertAll(
                () -> assertEquals(is2.getCRC32(), is3.getCRC32()),
                () -> assertEquals(is2.getCount(), is3.getCount())
        );
    }
}
