package se.simonevertsson.runner;

import junit.framework.TestCase;
import se.simonevertsson.runner.AliasDictionary;

/**
 * Created by simon.evertsson on 2015-09-02.
 */
public class AliasDictionaryTest extends TestCase {

    public void testShouldReturnCorrectAliases() {
        // Given

        // When
        String A = AliasDictionary.createAlias(0);
        String B = AliasDictionary.createAlias(1);
        String Z = AliasDictionary.createAlias(25);
        String AA = AliasDictionary.createAlias(26);
        String AB = AliasDictionary.createAlias(27);
        String AZ = AliasDictionary.createAlias(51);
        String AAC = AliasDictionary.createAlias(54);

        // Then
        assertEquals("A", A);
        assertEquals("B", B);
        assertEquals("Z", Z);
        assertEquals("AA", AA);
        assertEquals("AB", AB);
        assertEquals("AZ", AZ);
        assertEquals("AAC", AAC);
    }
}
