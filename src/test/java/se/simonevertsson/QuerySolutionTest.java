package se.simonevertsson;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import se.simonevertsson.gpu.QuerySolution;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by simon.evertsson on 2015-07-09.
 */
public class QuerySolutionTest extends TestCase {

    public void testEqualsShouldReturnFalseWhenComparedObjectIsNotAQuerySolution() {
        // Given
        QuerySolution querySolution = new QuerySolution(null);
        Integer dummyObject = new Integer(1337);

        // When
        boolean result = querySolution.equals(dummyObject);

        // Then
        assertEquals(false, result);
    }


    public void testEqualsShouldReturnFalseWhenComparedObjectIsNotOfSameSize() {
        // Given
        Map.Entry solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        Map.Entry solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);
        Map.Entry solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", 3);

        List<Map.Entry<String, Integer>> solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution querySolution = new QuerySolution(solutionElements);

        solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);

        solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);

        QuerySolution otherQuerySolution = new QuerySolution(solutionElements);

        // When
        boolean firstResult = querySolution.equals(otherQuerySolution);
        boolean secondResult = otherQuerySolution.equals(querySolution);

        // Then
        assertEquals(false, firstResult);
        assertEquals(false, secondResult);
    }

    public void testEqualsShouldReturnFalseWhenComparedObjectHasDifferentSolutionElementValues() {
        // Given
        Map.Entry solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        Map.Entry solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);
        Map.Entry solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", 3);

        List<Map.Entry<String, Integer>> solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution querySolution = new QuerySolution(solutionElements);

        solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);
        solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", 4);

        solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution otherQuerySolution = new QuerySolution(solutionElements);

        // When
        boolean firstResult = querySolution.equals(otherQuerySolution);
        boolean secondResult = otherQuerySolution.equals(querySolution);

        // Then
        assertEquals(false, firstResult);
        assertEquals(false, secondResult);
    }

    public void testEqualsShouldReturnFalseWhenComparedObjectHasDifferentSolutionElementKeys() {
        // Given
        Map.Entry solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        Map.Entry solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);
        Map.Entry solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", 3);

        List<Map.Entry<String, Integer>> solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution querySolution = new QuerySolution(solutionElements);

        solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);
        solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("D", 3);

        solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution otherQuerySolution = new QuerySolution(solutionElements);

        // When
        boolean firstResult = querySolution.equals(otherQuerySolution);
        boolean secondResult = otherQuerySolution.equals(querySolution);

        // Then
        assertEquals(false, firstResult);
        assertEquals(false, secondResult);
    }

    public void testEqualsShouldReturnTrueWhenComparedObjectHasEqualSolutionElements() {
        // Given
        Map.Entry solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        Map.Entry solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);
        Map.Entry solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", 3);

        List<Map.Entry<String, Integer>> solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution querySolution = new QuerySolution(solutionElements);

        solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);
        solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", 3);

        solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution otherQuerySolution = new QuerySolution(solutionElements);

        // When
        boolean firstResult = querySolution.equals(otherQuerySolution);
        boolean secondResult = otherQuerySolution.equals(querySolution);

        // Then
        assertEquals(true, firstResult);
        assertEquals(true, secondResult);
    }

    public void testEqualsShouldReturnTrueWhenComparedToItself() {
        // Given
        Map.Entry solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1);
        Map.Entry solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", 2);
        Map.Entry solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", 3);

        List<Map.Entry<String, Integer>> solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution querySolution = new QuerySolution(solutionElements);

        // When
        boolean result = querySolution.equals(querySolution);

        // Then
        assertEquals(true, result);
    }

    public void testEqualsShouldReturnTrueWhenComparedObjectHasEqualSolutionElementsWithHighAndLowValues() {
        // Given
        Map.Entry solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1337);
        Map.Entry solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", Integer.MAX_VALUE);
        Map.Entry solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", Integer.MIN_VALUE);

        List<Map.Entry<String, Integer>> solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution querySolution = new QuerySolution(solutionElements);

        solutionElementA = new AbstractMap.SimpleEntry<String, Integer>("A", 1337);
        solutionElementB = new AbstractMap.SimpleEntry<String, Integer>("B", Integer.MAX_VALUE);
        solutionElementC = new AbstractMap.SimpleEntry<String, Integer>("C", Integer.MIN_VALUE);

        solutionElements = new ArrayList<Map.Entry<String, Integer>>();
        solutionElements.add(solutionElementA);
        solutionElements.add(solutionElementB);
        solutionElements.add(solutionElementC);

        QuerySolution otherQuerySolution = new QuerySolution(solutionElements);

        // When
        boolean firstResult = querySolution.equals(otherQuerySolution);
        boolean secondResult = otherQuerySolution.equals(querySolution);

        // Then
        assertEquals(true, firstResult);
        assertEquals(true, secondResult);
    }

}
