/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package se.simonevertsson.mocking;

import java.util.Iterator;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.graphdb.*;

import static java.util.Arrays.asList;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.Iterables.asResourceIterable;

public class GraphMock
{
    public static Label[] labels( String... names )
    {
        Label[] labels = new Label[names.length];
        for ( int i = 0; i < labels.length; i++ )
        {
            labels[i] = DynamicLabel.label( names[i] );
        }
        return labels;
    }

    public static Node node( long id, Properties properties, int degreeOutgoing, String... labels )
    {
        return mockNode( id, labels( labels ), properties, degreeOutgoing );
    }

    public static Relationship relationship( long id, Properties properties, Node start, String type, Node end )
    {
        return mockRelationship( id, start, type, end, properties );
    }

    public static Link link( Relationship relationship, Node node )
    {
        return Link.link( relationship, node );
    }

    private static Node mockNode( long id, Label[] labels, Properties properties, int degreeOutgoing )
    {
        Node node = mockPropertyContainer( Node.class, properties );
        when( node.getId() ).thenReturn( id );
        when( node.getLabels() ).thenReturn( asResourceIterable( asList( labels ) ) );
        when( node.getDegree(Direction.OUTGOING) ).thenReturn(degreeOutgoing);
        return node;
    }

    private static Relationship mockRelationship( long id, Node start, String type, Node end, Properties properties )
    {
        Relationship relationship = mockPropertyContainer( Relationship.class, properties );
        when( relationship.getId() ).thenReturn( id );
        when( relationship.getStartNode() ).thenReturn( start );
        when( relationship.getEndNode() ).thenReturn( end );
        when( relationship.getType() ).thenReturn( DynamicRelationshipType.withName( type ) );
        return relationship;
    }

    private static <T extends PropertyContainer> T mockPropertyContainer( Class<T> type, Properties properties )
    {
        T container = mock( type );
        when( container.getProperty( anyString() ) ).thenAnswer( properties );
        when( container.getProperty( anyString(), any() ) ).thenAnswer( properties );
        when( container.getPropertyKeys() ).thenReturn( properties );
        return container;
    }
}
