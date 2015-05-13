package se.simonevertsson;

import com.nativelibs4java.opencl.*;
import org.bridj.Pointer;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Scanner;

/**
 * Created by simon on 2015-05-12.
 */
public class Experiments {

    public static final String DB_PATH = "target/drwho";
    public static final String DB_CONFIG_PATH = "target";

    public static void main(String[] args) throws IOException {
        DatabaseService dbService = new DatabaseService(DB_PATH, DB_CONFIG_PATH);
//        Iterable<Relationship> allRelationships = dbService.getAllRelationships();
//        for(Relationship relationship : allRelationships)  {
//            System.out.println(relationship);
//        }
        ResourceIterable<Node> allNodes = dbService.getAllNodes();
        GpuGraphModel g = GraphModelConverter.convertNodesToGpuGraphModel(allNodes);
        System.out.println("foo");
    }

}
