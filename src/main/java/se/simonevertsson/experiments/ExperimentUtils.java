package se.simonevertsson.experiments;

/**
 * Created by simon.evertsson on 2015-09-02.
 */
public class ExperimentUtils {

    public static final int ALPHABET_SIZE = 26;

    public static final int ASCII_CHARACTER_START = 65;

    public static String createAlias(int currentNode) {
        StringBuilder builder = new StringBuilder();

        int aliasLength = (currentNode / ALPHABET_SIZE) + 1;
        int rest = currentNode;
        for(int i = 0; i < aliasLength; i++) {
            int aliasCharacter = rest % ALPHABET_SIZE;
            builder.append((char)(ASCII_CHARACTER_START + aliasCharacter));

            rest = rest - aliasCharacter - ALPHABET_SIZE ;
        }

        return builder.reverse().toString();
    }

}
