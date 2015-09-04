package se.simonevertsson;

import junit.framework.TestCase;
import se.simonevertsson.experiments.ExperimentUtils;

/**
 * Created by simon.evertsson on 2015-09-02.
 */
public class ExperimentUtilsTest extends TestCase {

    public void testShouldReturnCorrectAliases() {
        // Given

        // When
        String A = ExperimentUtils.createAlias(0);
        String B = ExperimentUtils.createAlias(1);
        String Z = ExperimentUtils.createAlias(25);
        String AA = ExperimentUtils.createAlias(26);
        String AB = ExperimentUtils.createAlias(27);
        String AZ = ExperimentUtils.createAlias(51);
        String AAC = ExperimentUtils.createAlias(54);

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
