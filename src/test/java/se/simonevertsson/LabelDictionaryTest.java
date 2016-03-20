package se.simonevertsson;

import junit.framework.TestCase;
import se.simonevertsson.gpu.query.dictionary.LabelDictionary;

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

    public void testToString() throws Exception {
        // Given
        LabelDictionary labelDictionary = new LabelDictionary();
        int firstResult = labelDictionary.insertLabel("TEST_LABEL1");
        int secondResult = labelDictionary.insertLabel("TEST_LABEL2");

        // When
       String result = labelDictionary.toString();

        // Then
        assertEquals("TEST_LABEL1: 1\nTEST_LABEL2: 2\n", result);
    }
}