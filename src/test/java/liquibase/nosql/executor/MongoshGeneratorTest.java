package liquibase.nosql.executor;

import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.ext.mongodb.statement.MongoshStatement;
import liquibase.sql.Sql;
import liquibase.sqlgenerator.SqlGeneratorChain;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class MongoshGeneratorTest {

    private MongoshGenerator generator;
    private Database database;
    private SqlGeneratorChain<MongoshStatement> sqlGeneratorChain;

    @BeforeEach
    void setUp() {
        generator = new MongoshGenerator();
        database = new MongoLiquibaseDatabase();
        sqlGeneratorChain = mock(SqlGeneratorChain.class);
    }

    @Test
    void getPriority_shouldReturnDefaultPriority() {
        assertThat(generator.getPriority()).isEqualTo(generator.PRIORITY_DEFAULT);
    }

    @Test
    void supports_withMongoshStatement_shouldReturnTrue() {
        final MongoshStatement statement = new MongoshStatement("db.test.find()");
        
        assertThat(generator.supports(statement, database)).isTrue();
    }

    @Test
    void supports_withMockedMongoshStatement_shouldReturnTrue() {
        final MongoshStatement mockStatement = mock(MongoshStatement.class);
        
        // Even mocked MongoshStatement should be supported since it's still a MongoshStatement
        assertThat(generator.supports(mockStatement, database)).isTrue();
    }

    @Test
    void validate_withValidJavaScript_shouldReturnNoErrors() {
        final MongoshStatement statement = new MongoshStatement("db.users.find({status: 'active'})");
        
        final ValidationErrors errors = generator.validate(statement, database, sqlGeneratorChain);
        
        assertThat(errors.hasErrors()).isFalse();
    }

    @Test
    void validate_withNullJavaScript_shouldReturnValidationError() {
        final MongoshStatement statement = new MongoshStatement(null);
        
        final ValidationErrors errors = generator.validate(statement, database, sqlGeneratorChain);
        
        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages())
                .anyMatch(msg -> msg.contains("JavaScript content is required"));
    }

    @Test
    void validate_withEmptyJavaScript_shouldReturnValidationError() {
        final MongoshStatement statement = new MongoshStatement("");
        
        final ValidationErrors errors = generator.validate(statement, database, sqlGeneratorChain);
        
        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages())
                .anyMatch(msg -> msg.contains("JavaScript content is required"));
    }

    @Test
    void validate_withWhitespaceOnlyJavaScript_shouldReturnValidationError() {
        final MongoshStatement statement = new MongoshStatement("   \t\n  ");
        
        final ValidationErrors errors = generator.validate(statement, database, sqlGeneratorChain);
        
        assertThat(errors.hasErrors()).isTrue();
        assertThat(errors.getErrorMessages())
                .anyMatch(msg -> msg.contains("JavaScript content is required"));
    }

    @Test
    void generateSql_withValidJavaScript_shouldReturnFormattedSql() {
        final String javascript = "db.products.insertOne({name: 'test', price: 100})";
        final MongoshStatement statement = new MongoshStatement(javascript);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).hasSize(1);
        assertThat(result[0].toSql()).contains("// MongoDB JavaScript:");
        assertThat(result[0].toSql()).contains("db.products.insertOne({name: 'test', price: 100})");
    }

    @Test
    void generateSql_withNullJavaScript_shouldReturnEmptyArray() {
        final MongoshStatement statement = new MongoshStatement(null);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).isEmpty();
    }

    @Test
    void generateSql_withEmptyJavaScript_shouldReturnEmptyArray() {
        final MongoshStatement statement = new MongoshStatement("");
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).isEmpty();
    }

    @Test
    void generateSql_withWhitespaceOnlyJavaScript_shouldReturnEmptyArray() {
        final MongoshStatement statement = new MongoshStatement("   \t\n  ");
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).isEmpty();
    }

    @Test
    void generateSql_withJavaScriptEndingSemicolon_shouldRemoveTrailingSemicolon() {
        final String javascript = "db.orders.find({status: 'pending'});";
        final MongoshStatement statement = new MongoshStatement(javascript);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).hasSize(1);
        final String generatedSql = result[0].toSql();
        assertThat(generatedSql).contains("// MongoDB JavaScript:");
        assertThat(generatedSql).contains("db.orders.find({status: 'pending'})");
        assertThat(generatedSql).doesNotEndWith(";");
    }

    @Test
    void generateSql_withMultilineJavaScript_shouldPreserveFormatting() {
        final String javascript = 
            "db.users.aggregate([\n" +
            "    { $match: { status: \"active\" } },\n" +
            "    { $group: { _id: \"$department\", count: { $sum: 1 } } }\n" +
            "])";
        final MongoshStatement statement = new MongoshStatement(javascript);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).hasSize(1);
        final String generatedSql = result[0].toSql();
        assertThat(generatedSql).contains("// MongoDB JavaScript:");
        assertThat(generatedSql).contains("db.users.aggregate");
        assertThat(generatedSql).contains("$match");
        assertThat(generatedSql).contains("$group");
    }

    @Test
    void generateSql_shouldAddCommentPrefix() {
        final String javascript = "db.logs.deleteMany({timestamp: {$lt: new Date()}})";
        final MongoshStatement statement = new MongoshStatement(javascript);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).hasSize(1);
        final String generatedSql = result[0].toSql();
        assertThat(generatedSql).startsWith("// MongoDB JavaScript:\n");
    }

    @Test
    void generateSql_withComplexJavaScript_shouldHandleCorrectly() {
        final String javascript = "db.inventory.updateMany({qty: {$lt: 50}}, {$set: {reorder: true}});";
        final MongoshStatement statement = new MongoshStatement(javascript);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).hasSize(1);
        final String generatedSql = result[0].toSql();
        assertThat(generatedSql).contains("// MongoDB JavaScript:");
        assertThat(generatedSql).contains("db.inventory.updateMany");
        assertThat(generatedSql).contains("{qty: {$lt: 50}}");
        assertThat(generatedSql).contains("{$set: {reorder: true}}");
        // Should remove trailing semicolon
        assertThat(generatedSql).doesNotEndWith(";");
    }

    @Test
    void formatJavaScriptForDisplay_withNullInput_shouldReturnEmptyString() {
        // This tests the private method indirectly
        final MongoshStatement statement = new MongoshStatement(null);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).isEmpty();
    }

    @Test
    void generateSql_withJavaScriptWithoutTrailingSemicolon_shouldNotModify() {
        final String javascript = "db.users.findOne({_id: ObjectId('507f1f77bcf86cd799439011')})";
        final MongoshStatement statement = new MongoshStatement(javascript);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).hasSize(1);
        final String generatedSql = result[0].toSql();
        assertThat(generatedSql).contains("// MongoDB JavaScript:");
        assertThat(generatedSql).contains(javascript);
    }

    @Test
    void generateSql_withJavaScriptHavingInternalSemicolons_shouldOnlyRemoveTrailing() {
        final String javascript = "var result = db.test.find(); print(result.count());";
        final MongoshStatement statement = new MongoshStatement(javascript);
        
        final Sql[] result = generator.generateSql(statement, database, sqlGeneratorChain);
        
        assertThat(result).hasSize(1);
        final String generatedSql = result[0].toSql();
        assertThat(generatedSql).contains("// MongoDB JavaScript:");
        assertThat(generatedSql).contains("var result = db.test.find(); print(result.count())");
        // Should only remove the trailing semicolon, not internal ones
        assertThat(generatedSql).contains("db.test.find();");
    }
}
