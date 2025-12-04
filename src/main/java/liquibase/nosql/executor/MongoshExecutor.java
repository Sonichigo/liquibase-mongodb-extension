package liquibase.nosql.executor;

import liquibase.Scope;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.executor.AbstractExecutor;
import liquibase.ext.mongodb.change.MongoshChange;
import liquibase.ext.mongodb.change.MongoshFileChange;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.ext.mongodb.statement.MongoshStatement;
import liquibase.ext.mongodb.tools.MongoshRunner;
import liquibase.logging.Logger;
import liquibase.nosql.database.AbstractNoSqlDatabase;
import liquibase.servicelocator.LiquibaseService;
import liquibase.sql.Sql;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.sqlgenerator.SqlGeneratorFactory;
import liquibase.statement.SqlStatement;
import liquibase.change.Change;
import liquibase.change.core.EmptyChange;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;

/**
 * Executor for running MongoDB changesets via mongosh binary.
 * Handles validation, change type restrictions, and binary execution.
 */
@LiquibaseService
public class MongoshExecutor extends AbstractExecutor {
    
    public static final String EXECUTOR_NAME = "mongosh";
    private final Logger log = Scope.getCurrentScope().getLog(getClass());

    private ChangeSet currentChangeSet;
    
    public MongoshExecutor() {
    }
    
    @Override
    public String getName() {
        return EXECUTOR_NAME;
    }
    
    @Override
    public int getPriority() {
        return PRIORITY_SPECIALIZED;
    }
    
    @Override
    public boolean supports(Database database) {
        return database instanceof MongoLiquibaseDatabase;
    }
    
    @Override
    public ValidationErrors validate(ChangeSet changeSet) {
        this.currentChangeSet = changeSet;
        ValidationErrors validationErrors = new ValidationErrors();
        
        if (changeSet != null) {
            for (Change change : changeSet.getChanges()) {
                log.info("Validating change: " + change.getClass().getSimpleName());
                validateChange(changeSet, validationErrors, change, EXECUTOR_NAME);
            }
        }
        
        log.info("Validation completed with " + validationErrors.getErrorMessages().size() + " errors");
        return validationErrors;
    }

    private <T extends AbstractNoSqlDatabase> T getDatabase() {
        return (T) this.database;
    }

    protected void validateChange(ChangeSet changeSet, ValidationErrors validationErrors, 
                                 Change change, String executorType) {
        
        // Only allow mongosh-compatible change types
        if (!(change instanceof MongoshChange || 
              change instanceof MongoshFileChange || 
              change instanceof EmptyChange)) {
            
            String details = "In changeset '" + changeSet.getId() + "::" + changeSet.getAuthor() + 
                           "' there is an unsupported change type '" + change.getClass().getSimpleName() + "'";
            String message = String.format(
                "Changeset validation failed: %s. A changeset with runWith='%s' attribute may only contain " +
                "'mongo' or 'mongoFile' change types.",
                details, executorType);
            validationErrors.addError(message);
        }
    }
    
    @Override
    public void execute(SqlStatement statement, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        ChangeSet currentChangeSet = this.currentChangeSet;
        
        // Validate statement type
        if (!(statement instanceof MongoshStatement)) {
            log.warning("Statement should be type MongoshStatement, but is " + statement.getClass().getName());
        }
        
        try {
            // Generate JavaScript from statement
            log.info("Generating JavaScript from statement...");
            Sql[] generatedSql = SqlGeneratorFactory.getInstance().generateSql(statement, getDatabase());
            
            if (generatedSql != null && generatedSql.length > 0) {
                log.info("Executing " + generatedSql.length + " JavaScript statements via mongosh...");

                MongoshRunner runner = new MongoshRunner(currentChangeSet, generatedSql);
                runner.executeCommand(getDatabase());

                log.info(String.format("Successfully executed changeset '%s' by '%s' via mongosh", currentChangeSet.getId(), currentChangeSet.getAuthor()));
            } else {
                log.info("No JavaScript content generated - skipping execution");
            }
        } catch (Exception e) {
            String errorMsg = String.format("Changeset '%s' by '%s' failed to deploy with mongosh.", currentChangeSet.getId(), currentChangeSet.getAuthor());
            log.warning(errorMsg);
            throw new DatabaseException(errorMsg, e);
        }
    }
    
    @Override
    public <T> T queryForObject(SqlStatement sql, Class<T> requiredType) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }
    
    @Override
    public <T> T queryForObject(SqlStatement sql, Class<T> requiredType, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }
    
    @Override
    public long queryForLong(SqlStatement sql) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }
    
    @Override
    public long queryForLong(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }

    @Override
    public int queryForInt(SqlStatement sql) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }

    @Override
    public int queryForInt(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }

    @Override
    public List<Object> queryForList(SqlStatement sql, Class elementType) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }
    
    @Override
    public List<Object> queryForList(SqlStatement sql, Class elementType, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }

    @Override
    public List<Map<String, ?>> queryForList(SqlStatement sql) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }

    @Override
    public List<Map<String, ?>> queryForList(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        throw new DatabaseException("Query operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for query operations.");
    }

    @Override
    public void execute(SqlStatement sql) throws DatabaseException {
        // Delegate to main execute method with empty visitor list
        execute(sql, new ArrayList<>());
    }

    @Override
    public int update(SqlStatement sql) throws DatabaseException {
        throw new DatabaseException("Update operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for update operations or use 'mongo'/'mongoFile' change types.");
    }
    
    @Override
    public int update(SqlStatement sql, List<SqlVisitor> sqlVisitors) throws DatabaseException {
        throw new DatabaseException("Update operations are not supported by mongosh executor. " +
                                  "Use runWith='jdbc' for update operations or use 'mongo'/'mongoFile' change types.");
    }
    
    @Override
    public void comment(String message) throws DatabaseException {
        log.info("Mongosh executor comment: " + message);
    }
    
    @Override
    public boolean updatesDatabase() {
        return true;
    }
}
