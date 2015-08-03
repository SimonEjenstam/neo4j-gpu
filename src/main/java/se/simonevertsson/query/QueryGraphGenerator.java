package se.simonevertsson.query;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import se.simonevertsson.experiments.RelationshipTypes;

import java.util.ArrayList;

/**
 * Created by simon.evertsson on 2015-05-19.
 */
public class QueryGraphGenerator {

//    A1 ----> B2
//    |      / |
//    |    /   |
//    V  y     V
//    A3 ----> C4
    public static QueryGraph generateMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A1 = new QueryNode(0);
        A1.addLabel(new QueryLabel("A"));

        QueryNode B2 = new QueryNode(1);
        B2.addLabel(new QueryLabel("B"));

        QueryNode A3 = new QueryNode(2);
        A3.addLabel(new QueryLabel("A"));

        QueryNode C4 = new QueryNode(3);
        C4.addLabel(new QueryLabel("C"));

        queryGraph.nodes.add(A1);
        queryGraph.nodes.add(B2);
        queryGraph.nodes.add(A3);
        queryGraph.nodes.add(C4);

        queryGraph.aliasDictionary.insertAlias(A1, "A1");
        queryGraph.aliasDictionary.insertAlias(B2, "B2");
        queryGraph.aliasDictionary.insertAlias(A3, "A3");
        queryGraph.aliasDictionary.insertAlias(C4, "C4");

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A1_B2 = A1.createRelationshipTo(B2, 0, RelationshipTypes.KNOWS);
        Relationship A1_A3 = A1.createRelationshipTo(A3, 1, RelationshipTypes.KNOWS);
        Relationship B2_A3 = B2.createRelationshipTo(A3, 2, RelationshipTypes.KNOWS);
        Relationship B2_C4 = B2.createRelationshipTo(C4, 3, RelationshipTypes.KNOWS);
        Relationship A3_C4 = A3.createRelationshipTo(C4, 4, RelationshipTypes.KNOWS);

        queryGraph.relationships.add(A1_B2);
        queryGraph.relationships.add(A1_A3);
        queryGraph.relationships.add(B2_A3);
        queryGraph.relationships.add(B2_C4);
        queryGraph.relationships.add(A3_C4);



        return queryGraph;
    }

//    O ----> O
//    |      / |
//    |    /   |
//    V  y     V
//    O ----> O
    public static QueryGraph generateUnlabeledMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A1 = new QueryNode(0);

        QueryNode B2 = new QueryNode(10);

        QueryNode A3 = new QueryNode(20);

        QueryNode C4 = new QueryNode(30);

        queryGraph.nodes.add(A1);
        queryGraph.nodes.add(B2);
        queryGraph.nodes.add(A3);
        queryGraph.nodes.add(C4);

        queryGraph.aliasDictionary.insertAlias(A1, "A1");
        queryGraph.aliasDictionary.insertAlias(B2, "B2");
        queryGraph.aliasDictionary.insertAlias(A3, "A3");
        queryGraph.aliasDictionary.insertAlias(C4, "C4");

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A1_B2 = A1.createRelationshipTo(B2, 0, null);
        Relationship A1_A3 = A1.createRelationshipTo(A3, 10, null);
        Relationship B2_A3 = B2.createRelationshipTo(A3, 20, null);
        Relationship B2_C4 = B2.createRelationshipTo(C4, 30, null);
        Relationship A3_C4 = A3.createRelationshipTo(C4, 40, null);

        queryGraph.relationships.add(A1_B2);
        queryGraph.relationships.add(A1_A3);
        queryGraph.relationships.add(B2_A3);
        queryGraph.relationships.add(B2_C4);
        queryGraph.relationships.add(A3_C4);

        return queryGraph;
    }

    //                 (A)
    //                  O
    //                /  \
    //              /     \
    //            /        \
    //          /           \
    //        v              v
    // (C) O <-------------- O (B)
    public static QueryGraph generateTriangleMockQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A = new QueryNode(0);

        QueryNode B = new QueryNode(1);

        QueryNode C = new QueryNode(2);

        queryGraph.nodes.add(A);
        queryGraph.nodes.add(B);
        queryGraph.nodes.add(C);

        queryGraph.aliasDictionary.insertAlias(A, "A");
        queryGraph.aliasDictionary.insertAlias(B, "B");
        queryGraph.aliasDictionary.insertAlias(C, "C");

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A_B = A.createRelationshipTo(B, 0, null);
        Relationship A_C = A.createRelationshipTo(C, 1, null);
        Relationship B_C = B.createRelationshipTo(C, 2, null);

        queryGraph.relationships.add(A_B);
        queryGraph.relationships.add(A_C);
        queryGraph.relationships.add(B_C);

        return queryGraph;
    }


    public static QueryGraph generateQueryGraphWithManyRelationships() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A62 = new QueryNode(62);
        QueryNode B1 = new QueryNode(1);
        QueryNode C114 = new QueryNode(114);
        QueryNode D4 = new QueryNode(4);

        queryGraph.nodes.add(A62);
        queryGraph.nodes.add(B1);
        queryGraph.nodes.add(C114);
        queryGraph.nodes.add(D4);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A62_COMPANION_OF_B1 = A62.createRelationshipTo(B1, 130, RelationshipTypes.COMPANION_OF);
        Relationship A62_LOVES_B1 = A62.createRelationshipTo(B1, 131, RelationshipTypes.LOVES);
        Relationship A62_ENEMY_OF_C114 = A62.createRelationshipTo(C114, 396, RelationshipTypes.ENEMY_OF);

        Relationship B1_ENEMY_OF_C114 = B1.createRelationshipTo(C114, 394, RelationshipTypes.ENEMY_OF);
        Relationship B1_ENEMY_OF_D4 = B1.createRelationshipTo(D4, 1985, RelationshipTypes.ENEMY_OF);
        Relationship B1_LOVES_D4 = B1.createRelationshipTo(D4, 2, RelationshipTypes.LOVES);

        Relationship C114_ENEMY_OF_A62 = C114.createRelationshipTo(A62, 397, RelationshipTypes.ENEMY_OF);
        Relationship C114_ENEMY_OF_B1 = C114.createRelationshipTo(B1, 395, RelationshipTypes.ENEMY_OF);

        Relationship D4_LOVES_B1 = D4.createRelationshipTo(B1, 296, RelationshipTypes.LOVES);
        Relationship D4_COMPANION_OF_B1 = D4.createRelationshipTo(B1, 1747, RelationshipTypes.COMPANION_OF);
        Relationship D4_ENEMY_OF_B1 = D4.createRelationshipTo(B1, 1984, RelationshipTypes.ENEMY_OF);
        Relationship D4_ALLY_OF_B1 = D4.createRelationshipTo(B1, 295, RelationshipTypes.ALLY_OF);


        queryGraph.relationships.add(A62_COMPANION_OF_B1);
        queryGraph.relationships.add(A62_LOVES_B1);
        queryGraph.relationships.add(A62_ENEMY_OF_C114);
        queryGraph.relationships.add(B1_ENEMY_OF_C114);
        queryGraph.relationships.add(B1_ENEMY_OF_D4);
        queryGraph.relationships.add(B1_LOVES_D4);
        queryGraph.relationships.add(C114_ENEMY_OF_A62);
        queryGraph.relationships.add(C114_ENEMY_OF_B1);
        queryGraph.relationships.add(D4_LOVES_B1);
        queryGraph.relationships.add(D4_COMPANION_OF_B1);
        queryGraph.relationships.add(D4_ENEMY_OF_B1);
        queryGraph.relationships.add(D4_ALLY_OF_B1);

        return queryGraph;
    }

    public static QueryGraph generateQueryGraphWithLoop() {
        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A = new QueryNode(52);

        QueryNode B = new QueryNode(12);

        QueryNode C = new QueryNode(900);

        QueryNode D = new QueryNode(30);

        queryGraph.nodes.add(A);
        queryGraph.nodes.add(B);
        queryGraph.nodes.add(C);
        queryGraph.nodes.add(D);

        queryGraph.aliasDictionary.insertAlias(A, "A");
        queryGraph.aliasDictionary.insertAlias(B, "B");
        queryGraph.aliasDictionary.insertAlias(C, "C");
        queryGraph.aliasDictionary.insertAlias(D, "D");

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A_B = A.createRelationshipTo(B, 1337, null);
        Relationship A_C = A.createRelationshipTo(C, 10, null);
        Relationship B_C = B.createRelationshipTo(C, 56, null);
        Relationship B_D = B.createRelationshipTo(D, 2, null);
        Relationship D_B = D.createRelationshipTo(B, 40, null);

        queryGraph.relationships.add(A_B);
        queryGraph.relationships.add(A_C);
        queryGraph.relationships.add(B_C);
        queryGraph.relationships.add(B_D);
        queryGraph.relationships.add(D_B);

        return queryGraph;
    }

    public static QueryGraph generateFailingDataGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A906 = new QueryNode(906);
        QueryNode BD1 = new QueryNode(1);
        QueryNode C4 = new QueryNode(4);

        queryGraph.nodes.add(A906);
        queryGraph.nodes.add(BD1);
        queryGraph.nodes.add(C4);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship A906_ALLY_OF_BD1 = A906.createRelationshipTo(BD1, 1973, RelationshipTypes.ALLY_OF);
        Relationship A906_ENEMY_OF_BD1 = A906.createRelationshipTo(BD1, 1879, RelationshipTypes.ENEMY_OF);

        Relationship BD1_ENEMY_OF_A906 = BD1.createRelationshipTo(A906, 1880, RelationshipTypes.ENEMY_OF);
        Relationship BD1_ALLY_OF_A906 = BD1.createRelationshipTo(A906, 1974, RelationshipTypes.ALLY_OF);
        Relationship BD1_LOVES_D4 = BD1.createRelationshipTo(C4, 2, RelationshipTypes.LOVES);
        Relationship BD1_ENEMY_OF_D4 = BD1.createRelationshipTo(C4, 1985, RelationshipTypes.ENEMY_OF);

        Relationship C4_LOVES_BD1 = C4.createRelationshipTo(BD1, 296, RelationshipTypes.LOVES);
        Relationship C4_COMPANION_OF_BD1 = C4.createRelationshipTo(BD1, 1747, RelationshipTypes.COMPANION_OF);
        Relationship C4_ENEMY_OF_BD1 = C4.createRelationshipTo(BD1, 1984, RelationshipTypes.ENEMY_OF);
        Relationship C4_ALLY_OF_BD1 = C4.createRelationshipTo(BD1, 295, RelationshipTypes.ALLY_OF);


        queryGraph.relationships.add(A906_ALLY_OF_BD1);
        queryGraph.relationships.add(A906_ENEMY_OF_BD1);

        queryGraph.relationships.add(BD1_ENEMY_OF_A906);
        queryGraph.relationships.add(BD1_ALLY_OF_A906);
        queryGraph.relationships.add(BD1_LOVES_D4);
        queryGraph.relationships.add(BD1_ENEMY_OF_D4);

        queryGraph.relationships.add(C4_LOVES_BD1);
        queryGraph.relationships.add(C4_COMPANION_OF_BD1);
        queryGraph.relationships.add(C4_ENEMY_OF_BD1);
        queryGraph.relationships.add(C4_ALLY_OF_BD1);

        return queryGraph;
    }

    public static QueryGraph generateFailingDataGraph2() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode Doctor = new QueryNode(1);
        QueryNode Rose = new QueryNode(3);
        QueryNode Devil = new QueryNode(89);

        queryGraph.nodes.add(Doctor);
        queryGraph.nodes.add(Rose);
        queryGraph.nodes.add(Devil);

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship Doctor_LOVES_Rose = Doctor.createRelationshipTo(Rose, 1, RelationshipTypes.LOVES);
        Relationship Doctor_ENEMY_OF_Devil = Doctor.createRelationshipTo(Devil, 342, RelationshipTypes.ENEMY_OF);



        Relationship Rose_LOVES_Doctor = Rose.createRelationshipTo(Doctor, 116, RelationshipTypes.LOVES);
        Relationship Rose_COMPANION_OF_Doctor = Rose.createRelationshipTo(Doctor, 115, RelationshipTypes.COMPANION_OF);
        Relationship Rose_ENEMY_OF_Devil = Rose.createRelationshipTo(Devil, 344, RelationshipTypes.ENEMY_OF);

        Relationship Devil_ENEMY_OF_Doctor = Devil.createRelationshipTo(Doctor, 343, RelationshipTypes.ENEMY_OF);
        Relationship Devil_ENEMY_OF_Rose = Devil.createRelationshipTo(Rose, 345, RelationshipTypes.ENEMY_OF);


        queryGraph.relationships.add(Doctor_LOVES_Rose);
        queryGraph.relationships.add(Doctor_ENEMY_OF_Devil);
        queryGraph.relationships.add(Rose_LOVES_Doctor);
        queryGraph.relationships.add(Rose_COMPANION_OF_Doctor);
        queryGraph.relationships.add(Rose_ENEMY_OF_Devil);

        queryGraph.relationships.add(Devil_ENEMY_OF_Doctor);
        queryGraph.relationships.add(Devil_ENEMY_OF_Rose);

        return queryGraph;
    }

    public static QueryGraph generateFailingQueryGraph() {

        QueryGraph queryGraph = new QueryGraph();

        queryGraph.nodes = new ArrayList<Node>();

        QueryNode A = new QueryNode(52);

        QueryNode B = new QueryNode(12);

        QueryNode C = new QueryNode(900);

        QueryNode D = new QueryNode(30);

        queryGraph.nodes.add(A);
        queryGraph.nodes.add(B);
        queryGraph.nodes.add(C);
        queryGraph.nodes.add(D);

        queryGraph.aliasDictionary.insertAlias(A, "A");
        queryGraph.aliasDictionary.insertAlias(B, "B");
        queryGraph.aliasDictionary.insertAlias(C, "C");
        queryGraph.aliasDictionary.insertAlias(D, "D");

        queryGraph.relationships = new ArrayList<Relationship>();

        Relationship B_A = B.createRelationshipTo(A, 1337, null);
        Relationship B_C = B.createRelationshipTo(C, 10, null);
        Relationship C_B = C.createRelationshipTo(B, 56, null);
        Relationship D_A = D.createRelationshipTo(A, 2, null);
        Relationship D_C = D.createRelationshipTo(C, 40, null);

        queryGraph.relationships.add(B_A);
        queryGraph.relationships.add(B_C);
        queryGraph.relationships.add(C_B);
        queryGraph.relationships.add(D_A);
        queryGraph.relationships.add(D_C);

        return queryGraph;
    }
}
