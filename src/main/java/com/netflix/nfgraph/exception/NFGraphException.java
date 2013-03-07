package com.netflix.nfgraph.exception;

import com.netflix.nfgraph.NFGraph;

/**
 * This Exception is thrown when an invalid connection model, node type, or property type is specified in {@link NFGraph} API calls.  
 */
public class NFGraphException extends RuntimeException {

    private static final long serialVersionUID = -9177454492889434892L;
    
    public NFGraphException(String message) {
        super(message);
    }

}
