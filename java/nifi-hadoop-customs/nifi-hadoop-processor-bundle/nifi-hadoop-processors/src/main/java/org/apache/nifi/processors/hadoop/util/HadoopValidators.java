// 
// Decompiled by Procyon v0.5.36
// 

package org.apache.nifi.processors.hadoop.util;

import java.io.File;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.Validator;

public interface HadoopValidators
{
    public static final Validator ONE_OR_MORE_FILE_EXISTS_VALIDATOR = new Validator() {
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }
            final String[] split;
            final String[] files = split = input.split(",");
            for (final String filename : split) {
                try {
                    final File file = new File(filename.trim());
                    final boolean valid = file.exists() && file.isFile();
                    if (!valid) {
                        final String message = "File " + file + " does not exist or is not a file";
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                    }
                }
                catch (SecurityException e) {
                    final String message2 = "Unable to access " + filename + " due to " + e.getMessage();
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message2).build();
                }
            }
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }
    };
    public static final Validator UMASK_VALIDATOR = new Validator() {
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            try {
                final short shortVal = Short.parseShort(value, 8);
                if (shortVal < 0) {
                    reason = "octal umask [" + value + "] cannot be negative";
                }
                else if (shortVal > 511) {
                    reason = "octal umask [" + value + "] is not a valid umask";
                }
            }
            catch (NumberFormatException e) {
                reason = "[" + value + "] is not a valid short octal number";
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };
    public static final Validator POSITIVE_SHORT_VALIDATOR = new Validator() {
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            try {
                final short shortVal = Short.parseShort(value);
                if (shortVal <= 0) {
                    reason = "short integer must be greater than zero";
                }
            }
            catch (NumberFormatException e) {
                reason = "[" + value + "] is not a valid short integer";
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
        }
    };
}
