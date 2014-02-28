/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.geotools.cli.test.sqlserver.functional;

import org.geogit.test.integration.OnlineTestProperties;


public class IniSQLServerProperties extends OnlineTestProperties{

    private static final String []DEFAULTS = {//
        "database.host", "localhost",//
        "database.port", "1433",//
        "database.schema", "dbo",//
        "database.database", "database",//
        "database.user", "sa",//
        "database.password", "sa"//
    };

    public IniSQLServerProperties(){
        super(".geogit-sqlserver-tests.properties", DEFAULTS);
    }
}
