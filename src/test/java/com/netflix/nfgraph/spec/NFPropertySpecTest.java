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

import static com.netflix.nfgraph.spec.NFPropertySpec.HASH;
import static com.netflix.nfgraph.spec.NFPropertySpec.MODEL_SPECIFIC;
import static com.netflix.nfgraph.spec.NFPropertySpec.SINGLE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class NFPropertySpecTest {

	@Test
	public void testInstantiateWithFlagsDefault() {
		NFPropertySpec propertySpec = new NFPropertySpec(null, null, 0);
		
		assertTrue(propertySpec.isGlobal());
		assertTrue(propertySpec.isMultiple());
		assertFalse(propertySpec.isHashed());
	}
	
	
	@Test
	public void testInstantiateWithFlags() {
		NFPropertySpec propertySpec = new NFPropertySpec(null, null, MODEL_SPECIFIC | HASH | SINGLE);
		
		assertFalse(propertySpec.isGlobal());
		assertFalse(propertySpec.isMultiple());
		assertTrue(propertySpec.isHashed());
	}
	
}
