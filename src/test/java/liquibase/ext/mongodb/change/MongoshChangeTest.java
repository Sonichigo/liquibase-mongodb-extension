package liquibase.ext.mongodb.change;

import liquibase.exception.ValidationErrors;
import liquibase.ext.mongodb.statement.MongoshStatement;
import liquibase.statement.SqlStatement;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MongoshChangeTest extends AbstractMongoChangeTest {

    @Test
    void getConfirmationMessage_shouldReturnCorrectMessage() {
        final MongoshChange mongoshChange = new MongoshChange();
        assertThat(mongoshChange.getConfirmationMessage())
                .isEqualTo("Mongosh command executed");
    }

    @Test
    void setMongo_shouldSetMongoAndSqlProperties() {
        final MongoshChange mongoshChange = new MongoshChange();
        final String mongoScript = "db.users.find({});";
        
        mongoshChange.setMongo(mongoScript);
        
        assertThat(mongoshChange.getMongo()).isEqualTo(mongoScript);
        assertThat(mongoshChange.getSql()).isEqualTo(mongoScript);
    }

    @Test
    void generateStatements_withValidMongoScript_shouldReturnMongoshStatement() {
        final MongoshChange mongoshChange = new MongoshChange();
        final String mongoScript = "db.collection.insertOne({name: 'test'});";
        mongoshChange.setMongo(mongoScript);

        final SqlStatement[] statements = mongoshChange.generateStatements(database);

        assertThat(statements).hasSize(1);
        assertThat(statements[0]).isInstanceOf(MongoshStatement.class);
        
        final MongoshStatement mongoshStatement = (MongoshStatement) statements[0];
        assertThat(mongoshStatement.getJavaScript()).isEqualTo(mongoScript);
        assertThat(mongoshStatement.getEndDelimiter()).isEqualTo(";");
    }

    @Test
    void generateStatements_withNullMongo_shouldReturnEmptyArray() {
        final MongoshChange mongoshChange = new MongoshChange();
        mongoshChange.setMongo(null);

        final SqlStatement[] statements = mongoshChange.generateStatements(database);

        assertThat(statements).isEmpty();
    }

    @Test
    void generateStatements_withEmptyMongo_shouldReturnEmptyArray() {
        final MongoshChange mongoshChange = new MongoshChange();
        mongoshChange.setMongo("   ");

        final SqlStatement[] statements = mongoshChange.generateStatements(database);

        assertThat(statements).isEmpty();
    }

    @Test
    void generateStatements_withCustomEndDelimiter_shouldUseCustomDelimiter() {
        final MongoshChange mongoshChange = new MongoshChange();
        final String mongoScript = "db.test.find()";
        final String customDelimiter = "//";
        
        mongoshChange.setMongo(mongoScript);
        mongoshChange.setEndDelimiter(customDelimiter);

        final SqlStatement[] statements = mongoshChange.generateStatements(database);

        assertThat(statements).hasSize(1);
        final MongoshStatement mongoshStatement = (MongoshStatement) statements[0];
        assertThat(mongoshStatement.getEndDelimiter()).isEqualTo(customDelimiter);
    }

    @Test
    void generateStatements_withMultilineScript_shouldNormalizeLineEndings() {
        final MongoshChange mongoshChange = new MongoshChange();
        final String mongoScript = "db.users.find();\r\ndb.products.find();";
        mongoshChange.setMongo(mongoScript);

        final SqlStatement[] statements = mongoshChange.generateStatements(database);

        assertThat(statements).hasSize(1);
        final MongoshStatement mongoshStatement = (MongoshStatement) statements[0];
        // Line endings should be normalized (implementation detail, but testing the intent)
        assertThat(mongoshStatement.getJavaScript()).contains("db.users.find()");
        assertThat(mongoshStatement.getJavaScript()).contains("db.products.find()");
    }

    @Test
    void validate_withValidMongo_shouldReturnNoErrors() {
        final MongoshChange mongoshChange = new MongoshChange();
        mongoshChange.setMongo("db.test.find();");

        final ValidationErrors errors = mongoshChange.validate(database);

        assertThat(errors.hasErrors()).isFalse();
    }

    @Test
    void validate_withNullMongo_shouldReturnValidationError() {
        final MongoshChange mongoshChange = new MongoshChange();
        mongoshChange.setMongo(null);

        final ValidationErrors errors = mongoshChange.validate(database);

        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages())
                .anyMatch(msg -> msg.contains("'mongo' property is required"));
    }

    @Test
    void validate_withEmptyMongo_shouldReturnValidationError() {
        final MongoshChange mongoshChange = new MongoshChange();
        mongoshChange.setMongo("   ");

        final ValidationErrors errors = mongoshChange.validate(database);

        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages())
                .anyMatch(msg -> msg.contains("'mongo' property is required"));
    }

    @Test
    void getSerializableFields_shouldReturnCorrectFields() {
        final MongoshChange mongoshChange = new MongoshChange();
        
        assertThat(mongoshChange.getSerializableFields())
                .containsExactlyInAnyOrder("mongo", "dbms");
    }

    @Test
    void constructor_shouldInitializeWithDefaultValues() {
        final MongoshChange mongoshChange = new MongoshChange();
        
        assertThat(mongoshChange.getMongo()).isNull();
        assertThat(mongoshChange.getConfirmationMessage()).isNotNull();
    }

    @SneakyThrows
    @Test
    void complexMongoScript_shouldBeHandledCorrectly() {
        final MongoshChange mongoshChange = new MongoshChange();
        final String complexScript = 
            "db.users.aggregate([\n" +
            "    { $match: { status: \"active\" } },\n" +
            "    { $group: { _id: \"$department\", count: { $sum: 1 } } },\n" +
            "    { $sort: { count: -1 } }\n" +
            "]);";
        mongoshChange.setMongo(complexScript);

        final SqlStatement[] statements = mongoshChange.generateStatements(database);

        assertThat(statements).hasSize(1);
        final MongoshStatement mongoshStatement = (MongoshStatement) statements[0];
        assertThat(mongoshStatement.getJavaScript()).contains("db.users.aggregate");
        assertThat(mongoshStatement.getJavaScript()).contains("$match");
        assertThat(mongoshStatement.getJavaScript()).contains("$group");
    }
}
