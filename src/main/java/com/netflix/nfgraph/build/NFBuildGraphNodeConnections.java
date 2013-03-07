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

package com.netflix.nfgraph.build;

import java.util.Arrays;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.compressed.SingleOrdinalIterator;
import com.netflix.nfgraph.compressed.SingleOrdinalSet;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;

/**
 * Represents the connections for a node in an {@link NFBuildGraph} for a single connection model.
 * 
 * It is unlikely that this class will need to be used externally.
 */
class NFBuildGraphNodeConnections {

	private static final int EMPTY_ORDINAL_ARRAY[] = new int[0];
	
    private final int singleValues[];
    private final int multipleValues[][];
    private final int multipleValueSizes[];

    NFBuildGraphNodeConnections(NFNodeSpec nodeSpec) {
    	singleValues = new int[nodeSpec.getNumSingleProperties()];
    	multipleValues = new int[nodeSpec.getNumMultipleProperties()][];
    	multipleValueSizes = new int[nodeSpec.getNumMultipleProperties()];
    	
    	Arrays.fill(singleValues, -1);
    	Arrays.fill(multipleValues, EMPTY_ORDINAL_ARRAY);
    }
    
    int getConnection(NFPropertySpec spec) {
    	if(spec.isSingle())
    		return singleValues[spec.getPropertyIndex()];

    	if(multipleValues[spec.getPropertyIndex()].length > 0)
    	    return multipleValues[spec.getPropertyIndex()].length;
    	
    	return -1;
    }
    
    OrdinalSet getConnectionSet(NFPropertySpec spec) {
    	if(spec.isMultiple()) {
    		return new NFBuildGraphOrdinalSet(multipleValues[spec.getPropertyIndex()], multipleValueSizes[spec.getPropertyIndex()]);
    	}
    	return new SingleOrdinalSet(singleValues[spec.getPropertyIndex()]);
    }
    
    OrdinalIterator getConnectionIterator(NFPropertySpec spec) {
    	if(spec.isMultiple()) {
    		return new NFBuildGraphOrdinalIterator(multipleValues[spec.getPropertyIndex()], multipleValueSizes[spec.getPropertyIndex()]);
    	}

    	return new SingleOrdinalIterator(singleValues[spec.getPropertyIndex()]);
    }

    void addConnection(NFPropertySpec spec, int ordinal) {
        if (spec.isMultiple()) {
            addMultipleProperty(spec, ordinal);
        } else {
        	singleValues[spec.getPropertyIndex()] = ordinal;
        }
    }

    void addMultipleProperty(NFPropertySpec spec, int ordinal) {
    	int values[] = multipleValues[spec.getPropertyIndex()];
    	int propSize = multipleValueSizes[spec.getPropertyIndex()];
    	
    	if(values.length == 0) {
    	    values = new int[2];
    	    multipleValues[spec.getPropertyIndex()] = values;
    	} else if(values.length == propSize) {
    		values = Arrays.copyOf(values, values.length * 3 / 2);
    		multipleValues[spec.getPropertyIndex()] = values;
    	}
    	
    	values[propSize] = ordinal;
    	multipleValueSizes[spec.getPropertyIndex()]++;
    }
}
