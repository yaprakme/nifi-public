// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.nifi.processors.hadoop.util;

import java.util.ArrayList;
import java.util.List;
import org.apache.nifi.logging.ComponentLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import java.io.File;

public class KerberosProperties
{
    private final File kerberosConfigFile;
    private final Validator kerberosConfigValidator;
    private final PropertyDescriptor kerberosPrincipal;
    private final PropertyDescriptor kerberosKeytab;
    
    public KerberosProperties(final File kerberosConfigFile) {
        this.kerberosConfigFile = kerberosConfigFile;
        this.kerberosConfigValidator = (Validator)new Validator() {
            public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
                if (kerberosConfigFile == null) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation("you are missing the nifi.kerberos.krb5.file property which must be set in order to use Kerberos").build();
                }
                if (!kerberosConfigFile.canRead()) {
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(String.format("unable to read Kerberos config [%s], please make sure the path is valid and nifi has adequate permissions", kerberosConfigFile.getAbsoluteFile())).build();
                }
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            }
        };
        this.kerberosPrincipal = new PropertyDescriptor.Builder().name("Kerberos Principal").required(false).description("Kerberos principal to authenticate as. Requires nifi.kerberos.krb5.file to be set in your nifi.properties").addValidator(this.kerberosConfigValidator).addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
        this.kerberosKeytab = new PropertyDescriptor.Builder().name("Kerberos Keytab").required(false).description("Kerberos keytab associated with the principal. Requires nifi.kerberos.krb5.file to be set in your nifi.properties").addValidator(StandardValidators.FILE_EXISTS_VALIDATOR).addValidator(this.kerberosConfigValidator).addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR).expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY).build();
    }
    
    public File getKerberosConfigFile() {
        return this.kerberosConfigFile;
    }
    
    public Validator getKerberosConfigValidator() {
        return this.kerberosConfigValidator;
    }
    
    public PropertyDescriptor getKerberosPrincipal() {
        return this.kerberosPrincipal;
    }
    
    public PropertyDescriptor getKerberosKeytab() {
        return this.kerberosKeytab;
    }
    
    public static List<ValidationResult> validatePrincipalAndKeytab(final String subject, final Configuration config, final String principal, final String keytab, final ComponentLog logger) {
        final List<ValidationResult> results = new ArrayList<ValidationResult>();
        final boolean isSecurityEnabled = SecurityUtil.isSecurityEnabled(config);
        final boolean blankPrincipal = principal == null || principal.isEmpty();
        if (isSecurityEnabled && blankPrincipal) {
            results.add(new ValidationResult.Builder().valid(false).subject(subject).explanation("Kerberos Principal must be provided when using a secure configuration").build());
        }
        final boolean blankKeytab = keytab == null || keytab.isEmpty();
        if (isSecurityEnabled && blankKeytab) {
            results.add(new ValidationResult.Builder().valid(false).subject(subject).explanation("Kerberos Keytab must be provided when using a secure configuration").build());
        }
        if (!isSecurityEnabled && (!blankPrincipal || !blankKeytab)) {
            logger.warn("Configuration does not have security enabled, Keytab and Principal will be ignored");
        }
        return results;
    }
}
