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

import java.util.Iterator;

import com.netflix.nfgraph.exception.NFGraphException;
import com.netflix.nfgraph.util.ArrayIterator;

/**
 * An <code>NFNodeSpec</code> specifies the schema for a node type.
 * 
 * It is defined by a node name and a number of {@link NFPropertySpec}.
 */
public class NFNodeSpec implements Iterable<NFPropertySpec> {

	private final String nodeTypeName;
    private final NFPropertySpec propertySpecs[];
    
    private final int numSingleProperties;
    private final int numMultipleProperties;

    /**
     * The constructor for an <code>NFNodeSpec</code>.
     * 
     * @param nodeTypeName the name of the node type
     * @param propertySpecs a complete listing of the properties available for this node type.
     */
    public NFNodeSpec(String nodeTypeName, NFPropertySpec... propertySpecs) {
    	this.nodeTypeName = nodeTypeName;
        this.propertySpecs = propertySpecs;
        
        int numSingleProperties = 0;
        int numMultipleProperties = 0;
        
        for(NFPropertySpec propertySpec : propertySpecs) {
        	propertySpec.setPropertyIndex(propertySpec.isSingle() ? numSingleProperties++ : numMultipleProperties++);
        }
        
        this.numSingleProperties = numSingleProperties;
        this.numMultipleProperties = numMultipleProperties;
    }
    
    public String getNodeTypeName() {
    	return nodeTypeName;
    }

    public NFPropertySpec[] getPropertySpecs() {
        return propertySpecs;
    }
    
    public NFPropertySpec getPropertySpec(String propertyName) {
        for(NFPropertySpec spec : propertySpecs) {
            if(spec.getName().equals(propertyName))
                return spec;
        }
        throw new NFGraphException("Property " + propertyName + " is undefined for node type " + nodeTypeName);
    }
    
    public int getNumSingleProperties() {
    	return numSingleProperties;
    }
    
    public int getNumMultipleProperties() {
    	return numMultipleProperties;
    }

    @Override
    public Iterator<NFPropertySpec> iterator() {
        return new ArrayIterator<NFPropertySpec>(propertySpecs);
    }
    
}
