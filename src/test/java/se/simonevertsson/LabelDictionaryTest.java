package se.simonevertsson;

import junit.framework.TestCase;

/**
 * Created by simon on 2015-05-13.
 */
public class LabelDictionaryTest extends TestCase {

    public void testInsertLabelShouldNotIncreaseIdForSameLabel() throws Exception {
        // Given
        LabelDictionary labelDictionary = new LabelDictionary();

        // When
        int firstResult = labelDictionary.insertLabel("TEST_LABEL");
        int secondResult = labelDictionary.insertLabel("TEST_LABEL");

        // Then
        assertEquals(firstResult, secondResult);
    }

    public void testInsertLabelShouldIncreaseIdForDifferentLabels() throws Exception {
        // Given
        LabelDictionary labelDictionary = new LabelDictionary();

        // When
        int firstResult = labelDictionary.insertLabel("TEST_LABEL1");
        int secondResult = labelDictionary.insertLabel("TEST_LABEL2");

        // Then
        assertEquals(true, (firstResult != secondResult));
    }
}