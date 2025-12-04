package liquibase.nosql.executor;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.mongodb.statement.MongoshStatement;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import liquibase.sqlgenerator.core.AbstractSqlGenerator;

/**
 * SQL generator for MongoshStatement that produces JavaScript output for logging.
 * Converts MongoshStatement into displayable JavaScript code.
 */
public class MongoshGenerator extends AbstractSqlGenerator<MongoshStatement> {
    
    @Override
    public int getPriority() {
        return PRIORITY_DEFAULT;
    }
    
    @Override
    public boolean supports(MongoshStatement statement, Database database) {
        return statement instanceof MongoshStatement;
    }
    
    @Override
    public ValidationErrors validate(MongoshStatement statement, Database database, 
                                   SqlGeneratorChain<MongoshStatement> sqlGeneratorChain) {
        ValidationErrors validationErrors = new ValidationErrors();
        
        if (statement.getJavaScript() == null || statement.getJavaScript().trim().isEmpty()) {
            validationErrors.addError("JavaScript content is required for mongosh statement");
        }
        
        return validationErrors;
    }
    
    @Override
    public Sql[] generateSql(MongoshStatement statement, Database database, 
                           SqlGeneratorChain<MongoshStatement> sqlGeneratorChain) {
        
        // Format JavaScript for display/logging purposes
        String javascript = statement.getJavaScript();
        
        if (javascript == null || javascript.trim().isEmpty()) {
            return new Sql[0];
        }
        
        // Clean up the JavaScript for display
        String formattedJs = formatJavaScriptForDisplay(javascript);
        
        return new Sql[]{
            new UnparsedSql(formattedJs)
        };
    }
    
    /**
     * Format JavaScript code for display in logs and UI
     * 
     * @param javascript the raw JavaScript code
     * @return formatted JavaScript for display
     */
    private String formatJavaScriptForDisplay(String javascript) {
        if (javascript == null) {
            return "";
        }
        
        // Remove trailing semicolons for cleaner display
        String formatted = javascript.trim();
        if (formatted.endsWith(";")) {
            formatted = formatted.substring(0, formatted.length() - 1);
        }
        
        // Add comment prefix for clarity in logs
        return "// MongoDB JavaScript:\n" + formatted;
    }
}
