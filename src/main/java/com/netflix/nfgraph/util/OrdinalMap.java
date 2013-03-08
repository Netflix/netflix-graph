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

package com.netflix.nfgraph.util;

import java.util.Arrays;
import java.util.Iterator;

/**
 * An <code>OrdinalMap</code> will generate and maintain a mapping between objects added and an integer value between 
 * 0 and n, where n is the number of objects in the map.<p/>
 * 
 * The values mapped to the objects will be the order in which the objects are inserted.<p/>
 * 
 * The <code>OrdinalMap</code> is memory-efficient and can retrieve an object given an ordinal, or an ordinal given an object, both in <code>O(1)</code> time.<p/>
 *
 * If, for example, some application refers to graph nodes as Strings, the OrdinalMap can be used as follows:<p/>
 * 
 * <pre>
 * {@code
 * 
 * OrdinalMap<String> ordinalMap = new OrdinalMap<String>();
 * 
 * int ord0 = ordinalMap.add("node0");  // returns 0
 * int ord1 = ordinalMap.add("node1");  // returns 1
 * int ord2 = ordinalMap.add("node2");  // returns 2
 * int ord3 = ordinalMap.add("node1");  // returns 1
 * 
 * ordinalMap.get("node2"); // returns 2
 * ordinalMap.get(ord2);    // returns "node2"
 * 
 * }
 * </pre>
 */
public class OrdinalMap<T> implements Iterable<T> {

    private int hashedOrdinalArray[];
    private T objectsByOrdinal[];
    
    private int size;
    
    public OrdinalMap() {
        this(10);
    }
    
    @SuppressWarnings("unchecked")
    public OrdinalMap(int expectedSize) {
        int mapArraySize = 1 << (32 - Integer.numberOfLeadingZeros(expectedSize * 4 / 3));
        int ordinalArraySize = mapArraySize * 3 / 4;
        
        hashedOrdinalArray = newHashedOrdinalArray(mapArraySize);
        objectsByOrdinal = (T[]) new Object[ordinalArraySize];
    }
    
    /**
     * Add an object into this <code>OrdinalMap</code>.  If the same object (or an {@link Object#equals(Object)} object) is
     * already in the map, then no changes will be made.
     * 
     * @return the ordinal of <code>obj</code>
     */
    public int add(T obj) {
        int ordinal = get(obj);
        if(ordinal != -1)
            return ordinal;
        
        if(size == objectsByOrdinal.length)
            growCapacity();
        
        objectsByOrdinal[size] = obj;
        hashOrdinalIntoArray(size, hashedOrdinalArray);
        
        return size++;
    }
    
    /**
     * @return the ordinal of an object previously added to the map.  If the object has not been added to the map, returns -1 instead. 
     */
    public int get(T obj) {
        int hash = Mixer.hashInt(obj.hashCode());
        
        int bucket = hash % hashedOrdinalArray.length;
        int ordinal = hashedOrdinalArray[bucket];
        
        while(ordinal != -1) {
            if(objectsByOrdinal[ordinal].equals(obj))
                return ordinal;
            
            bucket = (bucket + 1) % hashedOrdinalArray.length;
            ordinal = hashedOrdinalArray[bucket];
        }
        
        return -1;
    }
    
    /**
     * @return the object for a given ordinal.  If the ordinal does not yet exist, returns null.
     */
    public T get(int ordinal) {
        if(ordinal >= size)
            return null;
        return objectsByOrdinal[ordinal];
    }
    
    /**
     * @return the number of objects in this map.
     */
    public int size() {
        return size;
    }
    
    private void growCapacity() {
        int newHashedOrdinalArray[] = newHashedOrdinalArray(hashedOrdinalArray.length * 2);
        
        for(int i=0;i<objectsByOrdinal.length;i++) {
            hashOrdinalIntoArray(i, newHashedOrdinalArray);
        }
        
        objectsByOrdinal = Arrays.copyOf(objectsByOrdinal, objectsByOrdinal.length * 2);
        hashedOrdinalArray = newHashedOrdinalArray;
    }
    
    private void hashOrdinalIntoArray(int ordinal, int hashedOrdinalArray[]) {
        int hash = Mixer.hashInt(objectsByOrdinal[ordinal].hashCode());
        
        int bucket = hash % hashedOrdinalArray.length;
        
        while(hashedOrdinalArray[bucket] != -1) {
            bucket = (bucket + 1) % hashedOrdinalArray.length;
        }
        
        hashedOrdinalArray[bucket] = ordinal;
    }
    
    private int[] newHashedOrdinalArray(int length) {
        int arr[] = new int[length];
        Arrays.fill(arr, -1);
        return arr;
    }

    /**
     * @return an {@link Iterator} over the objects in this mapping.
     */
    @Override
    public Iterator<T> iterator() {
        return new ArrayIterator<T>(objectsByOrdinal, size);
    }
    
}
