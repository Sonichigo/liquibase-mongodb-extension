package liquibase.ext.mongodb.statement;

import liquibase.statement.AbstractSqlStatement;

/**
 * Statement for executing JavaScript code via mongosh.
 * Contains the raw JavaScript/MongoDB shell commands to be executed.
 */
public class MongoshStatement extends AbstractSqlStatement {
    
    private final String javascript;
    private final String endDelimiter;
    
    /**
     * Create a mongosh statement with JavaScript code
     * @param javascript the JavaScript/MongoDB shell code to execute
     */
    public MongoshStatement(String javascript) {
        this(javascript, ";");
    }
    
    /**
     * Create a mongosh statement with JavaScript code and custom delimiter
     * @param javascript the JavaScript/MongoDB shell code to execute 
     * @param endDelimiter the statement end delimiter
     */
    public MongoshStatement(String javascript, String endDelimiter) {
        this.javascript = javascript;
        this.endDelimiter = endDelimiter != null ? endDelimiter : ";";
    }
    
    /**
     * @return the JavaScript code to execute
     */
    public String getJavaScript() {
        return javascript;
    }
    
    /**
     * @return the statement end delimiter with proper escape sequence handling
     */
    public String getEndDelimiter() {
        return endDelimiter.replace("\\r", "\r").replace("\\n", "\n");
    }
    
    /**
     * @return the JavaScript code (for logging/display purposes)
     */
    @Override
    public String toString() {
        return javascript;
    }
    
    /**
     * Generate MongoDB shell representation for logging
     * @return formatted shell command representation
     */
    public String toJs() {
        return javascript + getEndDelimiter();
    }
}
