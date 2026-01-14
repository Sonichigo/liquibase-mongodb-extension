package liquibase.nosql.executor;

import liquibase.change.core.EmptyChange;
import liquibase.changelog.ChangeSet;
import liquibase.database.Database;
import liquibase.exception.DatabaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.mongodb.change.CreateCollectionChange;
import liquibase.ext.mongodb.change.MongoshChange;
import liquibase.ext.mongodb.change.MongoshFileChange;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.ext.mongodb.statement.MongoshStatement;
import liquibase.sql.visitor.SqlVisitor;
import liquibase.statement.SqlStatement;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoshExecutorTest {

    private MongoshExecutor executor;
    private MongoLiquibaseDatabase database;
    private ChangeSet changeSet;

    @BeforeEach
    void setUp() {
        executor = new MongoshExecutor();
        database = mock(MongoLiquibaseDatabase.class);
        changeSet = mock(ChangeSet.class);
        executor.setDatabase(database);
    }

    @Test
    void getName_shouldReturnMongosh() {
        assertThat(executor.getName()).isEqualTo("mongosh");
    }

    @Test
    void getPriority_shouldReturnSpecializedPriority() {
        assertThat(executor.getPriority()).isEqualTo(executor.PRIORITY_SPECIALIZED);
    }

    @Test
    void supports_withMongoLiquibaseDatabase_shouldReturnTrue() {
        final Database mongoDatabase = new MongoLiquibaseDatabase();
        
        assertThat(executor.supports(mongoDatabase)).isTrue();
    }

    @Test
    void supports_withNonMongoDatabase_shouldReturnFalse() {
        final Database otherDatabase = mock(Database.class);
        
        assertThat(executor.supports(otherDatabase)).isFalse();
    }

    @Test
    void validate_withNullChangeSet_shouldReturnNoErrors() {
        final ValidationErrors errors = executor.validate(null);
        
        assertThat(errors.hasErrors()).isFalse();
    }

    @Test
    void validate_withMongoshChange_shouldReturnNoErrors() {
        final MongoshChange mongoshChange = new MongoshChange();
        when(changeSet.getChanges()).thenReturn(Arrays.asList(mongoshChange));
        
        final ValidationErrors errors = executor.validate(changeSet);
        
        assertThat(errors.hasErrors()).isFalse();
    }

    @Test
    void validate_withMongoshFileChange_shouldReturnNoErrors() {
        final MongoshFileChange mongoshFileChange = new MongoshFileChange();
        when(changeSet.getChanges()).thenReturn(Arrays.asList(mongoshFileChange));
        
        final ValidationErrors errors = executor.validate(changeSet);
        
        assertThat(errors.hasErrors()).isFalse();
    }

    @Test
    void validate_withEmptyChange_shouldReturnNoErrors() {
        final EmptyChange emptyChange = new EmptyChange();
        when(changeSet.getChanges()).thenReturn(Arrays.asList(emptyChange));
        
        final ValidationErrors errors = executor.validate(changeSet);
        
        assertThat(errors.hasErrors()).isFalse();
    }

    @Test
    void validate_withUnsupportedChange_shouldReturnValidationError() {
        final CreateCollectionChange unsupportedChange = new CreateCollectionChange();
        when(changeSet.getChanges()).thenReturn(Arrays.asList(unsupportedChange));
        when(changeSet.getId()).thenReturn("test-changeset");
        when(changeSet.getAuthor()).thenReturn("test-author");
        
        final ValidationErrors errors = executor.validate(changeSet);
        
        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages())
                .anyMatch(msg -> msg.contains("unsupported change type")
                        && msg.contains("CreateCollectionChange")
                        && msg.contains("runWith='mongosh'"));
    }

    @Test
    void validate_withMultipleChanges_shouldValidateAll() {
        final MongoshChange validChange = new MongoshChange();
        final CreateCollectionChange invalidChange = new CreateCollectionChange();
        when(changeSet.getChanges()).thenReturn(Arrays.asList(validChange, invalidChange));
        when(changeSet.getId()).thenReturn("mixed-changeset");
        when(changeSet.getAuthor()).thenReturn("test-author");
        
        final ValidationErrors errors = executor.validate(changeSet);
        
        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages()).hasSize(1);
        assertThat(errors.getErrorMessages().get(0)).contains("CreateCollectionChange");
    }

    @Test
    void execute_withNonMongoshStatement_shouldThrowException() throws DatabaseException {
        final SqlStatement nonMongoshStatement = mock(SqlStatement.class);
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        // Set the changeset first to avoid null pointer
        executor.validate(changeSet);
        
        // Non-MongoshStatement should fail because SqlGeneratorFactory cannot handle unknown statement types
        assertThatThrownBy(() -> executor.execute(nonMongoshStatement, visitors))
                .isInstanceOf(DatabaseException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
    }

    @Test
    void execute_withValidMongoshStatement_shouldAttemptExecution() throws DatabaseException {
        final MongoshStatement statement = new MongoshStatement("db.test.find()");
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        when(changeSet.getId()).thenReturn("test-changeset");
        when(changeSet.getAuthor()).thenReturn("test-author");
        
        // Set the changeset first to avoid null pointer
        executor.validate(changeSet);
        
        // Should throw DatabaseException because mongosh binary is not available in test environment
        // This verifies the executor attempts to execute MongoshStatements (vs ignoring them)
        assertThatThrownBy(() -> executor.execute(statement, visitors))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("mongosh");
    }

    @Test
    void execute_withEmptyStatement_shouldSkipExecution() throws DatabaseException {
        final MongoshStatement statement = new MongoshStatement("");
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        // Set the changeset first to avoid null pointer
        executor.validate(changeSet);
        
        // Empty statements should be handled gracefully without throwing exceptions
        // If this method completes without exception, the test passes
        executor.execute(statement, visitors);
    }

    @Test
    void execute_withSqlStatement_shouldDelegateToMainExecuteMethod() throws DatabaseException {
        final SqlStatement statement = new MongoshStatement("db.test.find()");
        
        // Set the changeset first to avoid null pointer  
        executor.validate(changeSet);
        
        // Single-parameter execute should delegate to the two-parameter version and attempt execution
        // Since mongosh is not available, it should fail with a meaningful error
        assertThatThrownBy(() -> executor.execute(statement))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("mongosh");
    }

    @Test
    void queryForObject_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        
        assertThatThrownBy(() -> executor.queryForObject(statement, String.class))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForObjectWithVisitors_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        assertThatThrownBy(() -> executor.queryForObject(statement, String.class, visitors))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForLong_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        
        assertThatThrownBy(() -> executor.queryForLong(statement))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForLongWithVisitors_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        assertThatThrownBy(() -> executor.queryForLong(statement, visitors))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForInt_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        
        assertThatThrownBy(() -> executor.queryForInt(statement))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForIntWithVisitors_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        assertThatThrownBy(() -> executor.queryForInt(statement, visitors))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForListWithElementType_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        
        assertThatThrownBy(() -> executor.queryForList(statement, String.class))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForListWithElementTypeAndVisitors_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        assertThatThrownBy(() -> executor.queryForList(statement, String.class, visitors))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForList_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        
        assertThatThrownBy(() -> executor.queryForList(statement))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void queryForListWithVisitors_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        assertThatThrownBy(() -> executor.queryForList(statement, visitors))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Query operations are not supported by mongosh executor");
    }

    @Test
    void update_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        
        assertThatThrownBy(() -> executor.update(statement))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Update operations are not supported by mongosh executor");
    }

    @Test
    void updateWithVisitors_shouldThrowDatabaseException() {
        final SqlStatement statement = mock(SqlStatement.class);
        final List<SqlVisitor> visitors = new ArrayList<>();
        
        assertThatThrownBy(() -> executor.update(statement, visitors))
                .isInstanceOf(DatabaseException.class)
                .hasMessageContaining("Update operations are not supported by mongosh executor");
    }

    @Test
    void comment_shouldLogMessage() throws DatabaseException {
        final String message = "Test comment message";
        
        // Should not throw exception
        executor.comment(message);
    }

    @Test
    void updatesDatabase_shouldReturnTrue() {
        assertThat(executor.updatesDatabase()).isTrue();
    }
}
