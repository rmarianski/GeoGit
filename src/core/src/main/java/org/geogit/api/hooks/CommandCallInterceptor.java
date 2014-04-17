/* Copyright (c) 2011 TOPP - www.openplans.org. All rights reserved.
 * This code is licensed under the LGPL 2.1 license, available at the root
 * application directory.
 */
package org.geogit.api.hooks;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.geogit.api.AbstractGeoGitOp;

/**
 * An interceptor for the call() method in GeoGit operations that allow hooks
 * 
 */
public class CommandCallInterceptor implements MethodInterceptor {

    @Override
    public Object invoke(MethodInvocation invocation) throws Throwable {

        AbstractGeoGitOp<?> operation = (AbstractGeoGitOp<?>) invocation.getThis();

        CommandHookChain callChain = CommandHookChain.builder().command(operation).build();

        return callChain.run();

    }
}
