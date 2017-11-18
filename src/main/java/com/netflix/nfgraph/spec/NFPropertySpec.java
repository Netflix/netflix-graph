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

import com.netflix.nfgraph.build.NFBuildGraph;
import com.netflix.nfgraph.compressed.BitSetOrdinalSet;
import com.netflix.nfgraph.compressed.CompactOrdinalSet;
import com.netflix.nfgraph.compressed.NFCompressedGraph;

/**
 * This class defines a specification for a single property.<p>
 * 
 * The recommended interface for creating a property is to instantiate with the flag method below.<p>
 * 
 * By default, an <code>NFPropertySpec</code> is {@link #GLOBAL}, {@link #MULTIPLE}, and {@link #COMPACT}.<p>
 * 
 * <pre>
 * {@code
 * import static com.netflix.nfgraph.spec.NFPropertySpec.*;
 * 
 * ...
 * 
 * NFPropertySpec spec1 = new NFPropertySpec( "property1", "foreignNodeType1", MULTIPLE | HASH );
 * NFPropertySpec spec2 = new NFPropertySpec( "property2", "foreignNodeType2", MULTIPLE | COMPACT | MODEL_SPECIFIC);
 * NFPropertySpec spec3 = new NFPropertySpec( "property2", "foreignNodeType3", SINGLE );
 * }
 * </pre>
 *  
 */
public class NFPropertySpec {

    /**
     * A property spec instantiated with this flag will not be separable into connection models.
     */
	public static final int GLOBAL = 0x00;
	
    /**
     * A property spec instantiated with this flag will be separable into connection models.
     */
	public static final int MODEL_SPECIFIC = 0x01;
	
    /**
     * A property spec instantiated with this flag will be allowed multiple connections.
     */
	public static final int MULTIPLE = 0x00;
	
    /**
     * A property spec instantiated with this flag will be allowed only a single connection.
     */
	public static final int SINGLE = 0x02;

    /**
     * A {@link #MULTIPLE} property instantiated with this flag will be represented as a {@link BitSetOrdinalSet} in an {@link NFCompressedGraph}.
     * 
     * @see BitSetOrdinalSet
     */
	public static final int HASH = 0x04;
	
    /**
     * A {@link #MULTIPLE} property instantiated with this flag will be represented as a {@link CompactOrdinalSet} in an {@link NFCompressedGraph}.
     * 
     * @see CompactOrdinalSet
     */
	public static final int COMPACT = 0x00;
	
    private final boolean isGlobal;
    private final boolean isMultiple;
    private final boolean isHashed;
    
    private final String name;
    private final String toNodeType;
    
    private int propertyIndex;

    /**
     * The recommended constructor.
     * 
     * @param name the name of the property.
     * @param toNodeType the node type to which this property connects
     * @param flags a bitwise-or of the various flags defined as constants in {@link NFPropertySpec}.<br>For example, a global, multiple, compact property would take the value <code>NFPropertySpec.GLOBAL | NFPropertySpec.MULTIPLE | NFPropertySpec.COMPACT</code>
     * 
     */
    public NFPropertySpec(String name, String toNodeType, int flags) {
    	this.name = name;
    	this.toNodeType = toNodeType;
    	this.isGlobal = (flags & MODEL_SPECIFIC) == 0;
    	this.isMultiple = (flags & SINGLE) == 0;
    	this.isHashed = (flags & HASH) != 0;
    }
    
    public NFPropertySpec(String name, String toNodeType, boolean isGlobal, boolean isMultiple, boolean isHashed) {
    	this.name = name;
    	this.toNodeType = toNodeType;
    	this.isGlobal = isGlobal;
        this.isMultiple = isMultiple;
        this.isHashed = isHashed;
    }

    public boolean isConnectionModelSpecific() {
    	return !isGlobal;
    }
    
    public boolean isGlobal() {
        return isGlobal;
    }

    public boolean isMultiple() {
        return isMultiple;
    }
    
    public boolean isSingle() {
    	return !isMultiple;
    }
    
    public boolean isHashed() {
    	return isHashed;
    }
    
    public boolean isCompact() {
        return !isHashed;
    }

    public String getName() {
        return name;
    }
    
    public String getToNodeType() {
    	return toNodeType;
    }
    
    void setPropertyIndex(int propertyIndex) {
    	this.propertyIndex = propertyIndex;
    }
    
    /**
     * Used by the {@link NFBuildGraph}.
     * 
     * It is unlikely that this method will be required externally. 
     */
    public int getPropertyIndex() {
    	return this.propertyIndex;
    }

}
