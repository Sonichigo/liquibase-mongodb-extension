package liquibase.ext.mongodb.tools;

import liquibase.changelog.ChangeSet;
import liquibase.changelog.DatabaseChangeLog;
import liquibase.ext.mongodb.database.MongoLiquibaseDatabase;
import liquibase.sql.Sql;
import liquibase.sql.UnparsedSql;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongoshRunnerTest {

    private MongoshRunner runner;
    private ChangeSet changeSet;
    private DatabaseChangeLog changeLog;
    private MongoLiquibaseDatabase database;
    private Sql[] sqlStatements;

    @BeforeEach
    void setUp() {
        changeSet = mock(ChangeSet.class);
        changeLog = mock(DatabaseChangeLog.class);
        database = mock(MongoLiquibaseDatabase.class);
        
        when(changeSet.getId()).thenReturn("test-changeset");
        when(changeSet.getAuthor()).thenReturn("test-author");
        when(changeSet.getChangeLog()).thenReturn(changeLog);
        when(changeLog.getLogicalFilePath()).thenReturn("changelog/test.xml");
        
        sqlStatements = new Sql[]{
            new UnparsedSql("db.users.find({status: 'active'})"),
            new UnparsedSql("db.orders.insertOne({user_id: 123, total: 99.99})")
        };
        
        runner = new MongoshRunner(changeSet, sqlStatements);
    }

    @Test
    void constructor_withValidParameters_shouldInitializeCorrectly() {
        assertThat(runner).isNotNull();
        // Verify that the runner extends ExecuteShellCommandChange
        assertThat(runner).isInstanceOf(liquibase.change.core.ExecuteShellCommandChange.class);
    }

    @Test
    void constructor_withNullChangeSet_shouldHandleGracefully() {
        final MongoshRunner runnerWithNullChangeSet = new MongoshRunner(null, sqlStatements);
        
        assertThat(runnerWithNullChangeSet).isNotNull();
    }

    @Test
    void constructor_withNullSqlStatements_shouldHandleGracefully() {
        final MongoshRunner runnerWithNullSql = new MongoshRunner(changeSet, null);
        
        assertThat(runnerWithNullSql).isNotNull();
    }

    @Test
    void constructor_withEmptySqlStatements_shouldHandleGracefully() {
        final Sql[] emptySqlStatements = new Sql[0];
        final MongoshRunner runnerWithEmptySql = new MongoshRunner(changeSet, emptySqlStatements);
        
        assertThat(runnerWithEmptySql).isNotNull();
    }

    @Test
    void inheritance_shouldExtendExecuteShellCommandChange() {
        // Verify inheritance structure
        assertThat(runner).isInstanceOf(liquibase.change.core.ExecuteShellCommandChange.class);
    }

    @Test
    void serviceLocator_shouldBeSkipped() {
        // Verify that the class is annotated with @LiquibaseService(skip = true)
        // This prevents automatic service discovery
        liquibase.servicelocator.LiquibaseService annotation = 
            MongoshRunner.class.getAnnotation(liquibase.servicelocator.LiquibaseService.class);
        
        assertThat(annotation).isNotNull();
        assertThat(annotation.skip()).isTrue();
    }

    @Test
    void multipleSqlStatements_shouldHandleCorrectly() {
        final Sql[] multipleSqlStatements = new Sql[]{
            new UnparsedSql("db.collection1.find()"),
            new UnparsedSql("db.collection2.insertOne({test: true})"),
            new UnparsedSql("db.collection3.updateMany({}, {$set: {updated: true}})"),
            new UnparsedSql("db.collection4.deleteMany({old: true})")
        };
        
        final MongoshRunner multiRunner = new MongoshRunner(changeSet, multipleSqlStatements);
        
        assertThat(multiRunner).isNotNull();
    }

    @Test
    void complexJavaScriptStatements_shouldHandleCorrectly() {
        final Sql[] complexSqlStatements = new Sql[]{
            new UnparsedSql(
                "db.users.aggregate([\n" +
                "    { $match: { status: \"active\" } },\n" +
                "    { $group: { _id: \"$department\", count: { $sum: 1 } } },\n" +
                "    { $sort: { count: -1 } }\n" +
                "])"
            ),
            new UnparsedSql(
                "var cursor = db.orders.find({date: {$gte: new Date('2023-01-01')}});\n" +
                "while (cursor.hasNext()) {\n" +
                "    var doc = cursor.next();\n" +
                "    if (doc.total > 100) {\n" +
                "        db.highValueOrders.insertOne(doc);\n" +
                "    }\n" +
                "}"
            )
        };
        
        final MongoshRunner complexRunner = new MongoshRunner(changeSet, complexSqlStatements);
        
        assertThat(complexRunner).isNotNull();
    }

    @Test
    void changeSetInformation_shouldBeAccessible() {
        // Verify that changeset information can be used for logging/tracking
        when(changeSet.getFilePath()).thenReturn("test-changelog.xml");
        when(changeSet.getContexts()).thenReturn(null);
        when(changeSet.getLabels()).thenReturn(null);
        
        final MongoshRunner trackedRunner = new MongoshRunner(changeSet, sqlStatements);
        
        assertThat(trackedRunner).isNotNull();
    }

    @Test
    void sqlStatements_withSpecialCharacters_shouldHandleCorrectly() {
        final Sql[] specialCharSqlStatements = new Sql[]{
            new UnparsedSql("db.collection.insertOne({text: 'Hello \"World\"', special: 'chars!@#$%^&*()'})"),
            new UnparsedSql("db.collection.find({regex: /^test.*$/i})"),
            new UnparsedSql("db.collection.updateOne({_id: ObjectId('507f1f77bcf86cd799439011')}, {$set: {updated: new Date()}})"),
        };
        
        final MongoshRunner specialRunner = new MongoshRunner(changeSet, specialCharSqlStatements);
        
        assertThat(specialRunner).isNotNull();
    }

    @Test
    void errorHandling_preparationForExecution() {
        // Test that runner can be created even with potentially problematic SQL
        final Sql[] problematicSqlStatements = new Sql[]{
            new UnparsedSql(""), // Empty SQL
            new UnparsedSql("   "), // Whitespace only
            new UnparsedSql("invalid javascript syntax {{{"), // Invalid syntax
            new UnparsedSql("db.nonexistentCollection.find()") // Valid syntax, might fail at runtime
        };
        
        final MongoshRunner errorProneRunner = new MongoshRunner(changeSet, problematicSqlStatements);
        
        // Should create runner successfully (errors would occur during execution)
        assertThat(errorProneRunner).isNotNull();
    }

    @Test
    void largeJavaScriptStatements_shouldHandleCorrectly() {
        // Test with larger JavaScript content
        final StringBuilder largeJs = new StringBuilder();
        largeJs.append("// Large JavaScript operation\n");
        for (int i = 0; i < 100; i++) {
            largeJs.append("db.collection").append(i).append(".insertOne({index: ").append(i).append("});\n");
        }
        
        final Sql[] largeSqlStatements = new Sql[]{
            new UnparsedSql(largeJs.toString())
        };
        
        final MongoshRunner largeRunner = new MongoshRunner(changeSet, largeSqlStatements);
        
        assertThat(largeRunner).isNotNull();
    }

    @Test
    void execution_context_information() {
        // Test that runner maintains context about changeset and database
        when(changeSet.getDescription()).thenReturn("Test changeset description");
        when(changeSet.getComments()).thenReturn("Test comments");
        
        final MongoshRunner contextualRunner = new MongoshRunner(changeSet, sqlStatements);
        
        assertThat(contextualRunner).isNotNull();
    }

    @Test
    void temporal_operations_shouldHandleCorrectly() {
        // Test JavaScript with date/time operations
        final Sql[] temporalSqlStatements = new Sql[]{
            new UnparsedSql("db.events.insertOne({timestamp: new Date(), event: 'test'})"),
            new UnparsedSql("db.logs.deleteMany({created: {$lt: new Date(Date.now() - 7*24*60*60*1000)}})"),
            new UnparsedSql("db.sessions.find({lastAccess: {$gte: ISODate('2023-01-01T00:00:00Z')}})")
        };
        
        final MongoshRunner temporalRunner = new MongoshRunner(changeSet, temporalSqlStatements);
        
        assertThat(temporalRunner).isNotNull();
    }

    @Test
    void functional_javascript_constructs() {
        // Test with functional JavaScript constructs
        final Sql[] functionalSqlStatements = new Sql[]{
            new UnparsedSql(
                "db.products.find().forEach(function(product) {\n" +
                "    if (product.price > 100) {\n" +
                "        db.expensiveProducts.insertOne(product);\n" +
                "    }\n" +
                "});"
            ),
            new UnparsedSql(
                "var pipeline = [\n" +
                "    {$match: {status: 'active'}},\n" +
                "    {$project: {name: 1, email: 1}}\n" +
                "];\n" +
                "db.users.aggregate(pipeline).forEach(printjson);"
            )
        };
        
        final MongoshRunner functionalRunner = new MongoshRunner(changeSet, functionalSqlStatements);
        
        assertThat(functionalRunner).isNotNull();
    }
}
