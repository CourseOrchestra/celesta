package ru.curs.lyra;

import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

public class TestFormProperties {
	@Test
	public void formPropertiesOverrideParent(){
		LyraFormProperties p1 = new LyraFormProperties();
		
		LyraFormProperties p2 = new LyraFormProperties(p1);
		
		p1.setGridwidth("100");
		p1.setGridheight("200");
		p2.setGridwidth("300");

		assertEquals("100", p1.getGridwidth());
		assertEquals("200", p1.getGridheight());
		assertNull(p1.getProfile());
		
		assertEquals("300", p2.getGridwidth());
		assertEquals("200", p2.getGridheight());
		assertNull(p2.getProfile());
		
		p1.setProfile("bar");
		assertEquals("bar", p1.getProfile());
		assertEquals("bar", p2.getProfile());
		
		p2.setProfile("foo");
		assertEquals("bar", p1.getProfile());
		assertEquals("foo", p2.getProfile());
	}
}
