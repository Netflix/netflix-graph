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

package com.netflix.nfgraph;

import java.util.Iterator;

import com.netflix.nfgraph.util.OrdinalMap;

/**
 * <code>NFGraphModelHolder</code> maintains an ordering over the models in a given NFGraph.<p>
 * 
 * An {@link NFGraph} may contain one or more "connection models".  A "connection model" is a grouping of the set of connections
 * between nodes in the graph.<p>
 * 
 * Connections added for a connection model will be visible only for that model.  Use of multiple connection models will 
 * add a minimum of one byte per model-specific connection set per node.  As a result, this feature should be used only 
 * when the number of connection models is and will remain low.<p>
 * 
 * It is unlikely that this class will need to be used externally.
 */
public class NFGraphModelHolder implements Iterable<String> {

    public static final String CONNECTION_MODEL_GLOBAL = "global";

    private OrdinalMap<String> modelMap;
	
	public NFGraphModelHolder() {
	    modelMap = new OrdinalMap<String>();
	    modelMap.add(CONNECTION_MODEL_GLOBAL);
	}
	
	public int size() {
		return modelMap.size();
	}
	
	public int getModelIndex(String connectionModel) {
	    return modelMap.add(connectionModel);
	}
	
	public String getModel(int modelIndex) {
	    return modelMap.get(modelIndex);
	}
	
	public Iterator<String> iterator() {
	    return modelMap.iterator();
	}

}
