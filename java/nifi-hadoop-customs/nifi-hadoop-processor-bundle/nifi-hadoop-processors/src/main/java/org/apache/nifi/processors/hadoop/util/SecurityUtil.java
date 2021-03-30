// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.nifi.processors.hadoop.util;

import java.io.IOException;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.conf.Configuration;

public class SecurityUtil
{
    public static final String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static final String KERBEROS = "kerberos";
    
    public static synchronized UserGroupInformation loginKerberos(final Configuration config, final String principal, final String keyTab) throws IOException {
        Validate.notNull((Object)config);
        Validate.notNull((Object)principal);
        Validate.notNull((Object)keyTab);
        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab(principal.trim(), keyTab.trim());
        return UserGroupInformation.getCurrentUser();
    }
    
    public static synchronized UserGroupInformation loginSimple(final Configuration config) throws IOException {
        Validate.notNull((Object)config);
        UserGroupInformation.setConfiguration(config);
        return UserGroupInformation.getLoginUser();
    }
    
    public static boolean isSecurityEnabled(final Configuration config) {
        Validate.notNull((Object)config);
        return "kerberos".equalsIgnoreCase(config.get("hadoop.security.authentication"));
    }
}
