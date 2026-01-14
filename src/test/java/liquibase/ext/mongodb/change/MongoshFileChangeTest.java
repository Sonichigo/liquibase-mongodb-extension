package liquibase.ext.mongodb.change;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.exception.SetupException;
import liquibase.exception.UnexpectedLiquibaseException;
import liquibase.exception.ValidationErrors;
import liquibase.ext.mongodb.statement.MongoshStatement;
import liquibase.statement.SqlStatement;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoshFileChangeTest extends AbstractMongoChangeTest {

    private MongoshFileChange mongoshFileChange;

    @BeforeEach
    void setUp() {
        super.setUp();
        mongoshFileChange = new MongoshFileChange();
    }

    @Test
    void getConfirmationMessage_shouldIncludePath() {
        final String testPath = "scripts/test.js";
        mongoshFileChange.setPath(testPath);

        assertThat(mongoshFileChange.getConfirmationMessage())
                .isEqualTo("Mongosh commands in file " + testPath + " executed");
    }

    @Test
    void setPath_shouldSetCorrectPath() {
        final String testPath = "mongo-scripts/init.js";
        mongoshFileChange.setPath(testPath);

        assertThat(mongoshFileChange.getPath()).isEqualTo(testPath);
    }

    @Test
    void setRelativeToChangelogFile_shouldSetBooleanFlag() {
        mongoshFileChange.setRelativeToChangelogFile(true);
        assertThat(mongoshFileChange.getRelativeToChangelogFile()).isTrue();

        mongoshFileChange.setRelativeToChangelogFile(false);
        assertThat(mongoshFileChange.getRelativeToChangelogFile()).isFalse();
    }

    @Test
    void setEncoding_shouldSetCorrectEncoding() {
        final String encoding = "UTF-16";
        mongoshFileChange.setEncoding(encoding);

        assertThat(mongoshFileChange.getEncoding()).isEqualTo(encoding);
    }

    @Test
    void finishInitialization_withNullPath_shouldThrowSetupException() {
        mongoshFileChange.setPath(null);

        assertThatThrownBy(() -> mongoshFileChange.finishInitialization())
                .isInstanceOf(SetupException.class)
                .hasMessageContaining("'path' is required for mongoFile changes");
    }

    @Test
    void finishInitialization_withValidPath_shouldNotThrowException() throws SetupException {
        mongoshFileChange.setPath("test.js");

        // Should not throw any exception
        mongoshFileChange.finishInitialization();
    }

    @Test
    void validate_withNullPath_shouldReturnValidationError() {
        mongoshFileChange.setPath(null);

        final ValidationErrors errors = mongoshFileChange.validate(database);

        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages())
                .anyMatch(msg -> msg.contains("'path' is required for mongoFile changes"));
    }

    @Test
    void validate_withEmptyPath_shouldReturnValidationError() {
        mongoshFileChange.setPath("   ");

        final ValidationErrors errors = mongoshFileChange.validate(database);

        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages())
                .anyMatch(msg -> msg.contains("'path' is required for mongoFile changes"));
    }

    @Test
    void validate_withValidPath_shouldReturnNoErrors() {
        mongoshFileChange.setPath("valid-script.js");

        final ValidationErrors errors = mongoshFileChange.validate(database);

        assertThat(errors.hasErrors()).isFalse();
    }

    @Test
    @SneakyThrows
    void getSql_withNullPath_shouldReturnNull() {
        mongoshFileChange.setPath(null);

        assertThat(mongoshFileChange.getSql()).isNull();
    }

    @Test
    void getSql_withCachedContent_shouldReturnCachedValue() {
        final String cachedContent = "db.test.find();";
        mongoshFileChange.setSql(cachedContent);

        assertThat(mongoshFileChange.getSql()).isEqualTo(cachedContent);
    }

    @Test
    void setSql_shouldCallSetNoSql() {
        final String sqlContent = "db.users.find();";
        mongoshFileChange.setSql(sqlContent);

        // Verify that setSql calls setNoSql internally
        assertThat(mongoshFileChange.getSql()).isEqualTo(sqlContent);
    }

    @Test
    void generateStatements_withNullSql_shouldReturnEmptyArray() {
        // Don't set any path so getSql() returns null
        mongoshFileChange.setPath(null);

        final SqlStatement[] statements = mongoshFileChange.generateStatements(database);

        assertThat(statements).isEmpty();
    }

    @Test
    void generateStatements_withValidSql_shouldReturnMongoshStatement() {
        final String jsContent = "db.collection.insertOne({test: true});";
        mongoshFileChange.setSql(jsContent);

        final SqlStatement[] statements = mongoshFileChange.generateStatements(database);

        assertThat(statements).hasSize(1);
        assertThat(statements[0]).isInstanceOf(MongoshStatement.class);
        
        final MongoshStatement mongoshStatement = (MongoshStatement) statements[0];
        assertThat(mongoshStatement.getJavaScript()).isEqualTo(jsContent);
    }

    @Test
    void generateStatements_withCustomEndDelimiter_shouldUseCustomDelimiter() {
        final String jsContent = "db.test.find()";
        final String customDelimiter = "//";
        
        mongoshFileChange.setSql(jsContent);
        mongoshFileChange.setEndDelimiter(customDelimiter);

        final SqlStatement[] statements = mongoshFileChange.generateStatements(database);

        assertThat(statements).hasSize(1);
        final MongoshStatement mongoshStatement = (MongoshStatement) statements[0];
        assertThat(mongoshStatement.getEndDelimiter()).isEqualTo(customDelimiter);
    }

    @Test
    void openSqlStream_shouldCallOpenNoSqlStream() throws IOException {
        // This is primarily testing that the method delegation works
        mongoshFileChange.setPath(null);
        
        assertThat(mongoshFileChange.openSqlStream()).isNull();
    }

    @Test
    void openNoSqlStream_withNullPath_shouldReturnNull() throws IOException {
        mongoshFileChange.setPath(null);

        assertThat(mongoshFileChange.openNoSqlStream()).isNull();
    }

    @Test
    void getSerializableFields_shouldReturnCorrectFields() {
        assertThat(mongoshFileChange.getSerializableFields())
                .containsExactlyInAnyOrder("relativeToChangelogFile", "path", "dbms");
    }

    @Test
    void constructor_shouldInitializeWithDefaultValues() {
        final MongoshFileChange freshChange = new MongoshFileChange();
        
        assertThat(freshChange.getPath()).isNull();
        assertThat(freshChange.getRelativeToChangelogFile()).isNull();
        assertThat(freshChange.getEncoding()).isNull();
    }

    @Test
    void getSql_withInvalidPath_shouldThrowException() {
        mongoshFileChange.setPath("nonexistent-path-that-does-not-exist.js");
        
        // Missing files should throw UnexpectedLiquibaseException with clear error message
        assertThatThrownBy(() -> mongoshFileChange.getSql())
                .isInstanceOf(UnexpectedLiquibaseException.class)
                .hasMessageContaining("Error reading mongosh script file");
    }

    @Test
    void getSql_withEmptyPath_shouldThrowException() {
        mongoshFileChange.setPath("");
        
        // Empty path should also throw exception like missing files
        assertThatThrownBy(() -> mongoshFileChange.getSql())
                .isInstanceOf(UnexpectedLiquibaseException.class)
                .hasMessageContaining("Error reading mongosh script file");
    }

    @Test
    void openNoSqlStream_withRelativeToChangelogFile_shouldAttemptRelativeResolution() {
        mongoshFileChange.setPath("relative-script.js");
        mongoshFileChange.setRelativeToChangelogFile(true);
        
        // Setup changeset and changelog
        ChangeSet mockChangeSet = mock(ChangeSet.class);
        DatabaseChangeLog mockChangeLog = mock(DatabaseChangeLog.class);
        mongoshFileChange.setChangeSet(mockChangeSet);
        
        when(mockChangeSet.getChangeLog()).thenReturn(mockChangeLog);
        when(mockChangeLog.getPhysicalFilePath()).thenReturn("changelog/main.xml");
        
        // Should attempt to resolve relative to changelog file and throw appropriate exception
        assertThatThrownBy(() -> mongoshFileChange.openNoSqlStream())
                .isInstanceOf(UnexpectedLiquibaseException.class)
                .hasMessageContaining("Resource does not exist");
    }
}
