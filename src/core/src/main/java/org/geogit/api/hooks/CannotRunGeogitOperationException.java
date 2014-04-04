/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.api.hooks;

public class CannotRunGeogitOperationException extends Exception {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new {@code CannotRunGeogitOperationException} with the given message.
     * 
     * @param msg the message for the exception
     */
    public CannotRunGeogitOperationException(String msg) {
        super(msg);
    }

}
