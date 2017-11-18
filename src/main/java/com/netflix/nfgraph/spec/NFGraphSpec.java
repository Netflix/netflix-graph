/*
 *  Copyright 2013 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.nfgraph.spec;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.netflix.nfgraph.exception.NFGraphException;

/**
 * An <code>NFGraphSpec</code> defines the schema for a graph.  It contains a mapping of node type 
 * name to {@link NFNodeSpec}s.<p>
 *
 * The example code below will create two node types "a" and "b".  An "a" node can be connected to "b" nodes 
 * via the properties "a-to-one-b" and/or "a-to-many-b".  A "b" node can be connected to "a" nodes via the property
 * "b-to-many-a".<p>
 * 
 * <pre>
 * {@code
 * 
 * NFGraphSpec spec = new NFGraphSpec(
 *     new NFNodeSpec(
 *         "a",
 *         new NFPropertySpec("a-to-one-b",  "b", NFPropertySpec.SINGLE),
 *         new NFPropertySpec("a-to-many-b", "b", NFPropertySpec.MULTIPLE | NFPropertySpec.COMPACT)
 *     ),
 *     new NFNodeSpec(
 *         "b",
 *         new NFPropertySpec("b-to-many-a", "a", NFPropertySpec.MULTIPLE | NFPropertySpec.HASH)
 *     )
 * );
 * 
 * }
 * </pre>
 * 
 * @see NFNodeSpec
 * @see NFPropertySpec
 */
public class NFGraphSpec implements Iterable<NFNodeSpec> {
    
    private final Map<String, NFNodeSpec> nodeSpecs;

    /**
     * Instantiate a graph specification with no {@link NFNodeSpec}s.
     */
    public NFGraphSpec() {
        this.nodeSpecs = new HashMap<String, NFNodeSpec>();
    }
    
    /**
     * Instantiate a graph specification with the given {@link NFNodeSpec}. 
     */
    public NFGraphSpec(NFNodeSpec... nodeTypes) {
        this();
        
        for(NFNodeSpec spec : nodeTypes) {
            addNodeSpec(spec);
        }
    }

    /**
     * @return the {@link NFNodeSpec} for the specified node type.
     */
    public NFNodeSpec getNodeSpec(String nodeType) {
        NFNodeSpec spec = nodeSpecs.get(nodeType);
        if(spec == null)
            throw new NFGraphException("Node spec " + nodeType + " is undefined");
        return spec;
    }
    
    /**
     * Add a node type to this graph specification. 
     */
    public void addNodeSpec(NFNodeSpec nodeSpec) {
        nodeSpecs.put(nodeSpec.getNodeTypeName(), nodeSpec);
    }
    
    /**
     * @return the number of node types defined by this graph specification.
     */
    public int size() {
        return nodeSpecs.size();
    }
    
    /**
     * @return a {@link List} containing the names of each of the node types.
     */
    public List<String> getNodeTypes() {
    	return new ArrayList<String>(nodeSpecs.keySet());
    }

    /**
     * Returns an {@link Iterator} over the {@link NFNodeSpec}s contained in this graph specification.
     */
    @Override
    public Iterator<NFNodeSpec> iterator() {
        return nodeSpecs.values().iterator();
    }
    
}
