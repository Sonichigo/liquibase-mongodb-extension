package liquibase.ext.mongodb.statement;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MongoshStatementTest {

    @Test
    void constructor_withJavaScriptOnly_shouldUseDefaultDelimiter() {
        final String javascript = "db.users.find();";
        final MongoshStatement statement = new MongoshStatement(javascript);

        assertThat(statement.getJavaScript()).isEqualTo(javascript);
        assertThat(statement.getEndDelimiter()).isEqualTo(";");
    }

    @Test
    void constructor_withJavaScriptAndDelimiter_shouldUseProvidedDelimiter() {
        final String javascript = "db.products.insertOne({name: 'test'})";
        final String delimiter = "//";
        final MongoshStatement statement = new MongoshStatement(javascript, delimiter);

        assertThat(statement.getJavaScript()).isEqualTo(javascript);
        assertThat(statement.getEndDelimiter()).isEqualTo(delimiter);
    }

    @Test
    void constructor_withNullDelimiter_shouldUseDefaultDelimiter() {
        final String javascript = "db.orders.count()";
        final MongoshStatement statement = new MongoshStatement(javascript, null);

        assertThat(statement.getJavaScript()).isEqualTo(javascript);
        assertThat(statement.getEndDelimiter()).isEqualTo(";");
    }

    @Test
    void getJavaScript_shouldReturnOriginalJavaScript() {
        final String javascript = "db.inventory.find({quantity: {$gt: 50}})";
        final MongoshStatement statement = new MongoshStatement(javascript);

        assertThat(statement.getJavaScript()).isEqualTo(javascript);
    }

    @Test
    void getEndDelimiter_shouldHandleEscapeSequences() {
        final String javascript = "db.test.find()";
        final String delimiterWithEscapes = "\\r\\n";
        final MongoshStatement statement = new MongoshStatement(javascript, delimiterWithEscapes);

        assertThat(statement.getEndDelimiter()).isEqualTo("\r\n");
    }

    @Test
    void getEndDelimiter_withoutEscapeSequences_shouldReturnAsIs() {
        final String javascript = "db.test.find()";
        final String normalDelimiter = ";;";
        final MongoshStatement statement = new MongoshStatement(javascript, normalDelimiter);

        assertThat(statement.getEndDelimiter()).isEqualTo(";;");
    }

    @Test
    void getEndDelimiter_withMixedEscapeSequences_shouldHandleAll() {
        final String javascript = "db.test.find()";
        final String complexDelimiter = "\\r\\n;\\r";
        final MongoshStatement statement = new MongoshStatement(javascript, complexDelimiter);

        assertThat(statement.getEndDelimiter()).isEqualTo("\r\n;\r");
    }

    @Test
    void toString_shouldReturnJavaScript() {
        final String javascript = "db.analytics.aggregate([{$match: {date: new Date()}}])";
        final MongoshStatement statement = new MongoshStatement(javascript);

        assertThat(statement.toString()).isEqualTo(javascript);
    }

    @Test
    void toJs_shouldReturnJavaScriptWithDelimiter() {
        final String javascript = "db.users.createIndex({email: 1})";
        final String delimiter = ";";
        final MongoshStatement statement = new MongoshStatement(javascript, delimiter);

        assertThat(statement.toJs()).isEqualTo(javascript + delimiter);
    }

    @Test
    void toJs_withCustomDelimiter_shouldUseCustomDelimiter() {
        final String javascript = "db.logs.deleteMany({timestamp: {$lt: new Date('2023-01-01')}})";
        final String delimiter = " GO";
        final MongoshStatement statement = new MongoshStatement(javascript, delimiter);

        assertThat(statement.toJs()).isEqualTo(javascript + delimiter);
    }

    @Test
    void toJs_withEscapedDelimiter_shouldUseProcessedDelimiter() {
        final String javascript = "db.sessions.drop()";
        final String escapedDelimiter = "\\n";
        final MongoshStatement statement = new MongoshStatement(javascript, escapedDelimiter);

        assertThat(statement.toJs()).isEqualTo(javascript + "\n");
    }

    @Test
    void constructor_withEmptyJavaScript_shouldAllowEmptyString() {
        final String emptyJavaScript = "";
        final MongoshStatement statement = new MongoshStatement(emptyJavaScript);

        assertThat(statement.getJavaScript()).isEmpty();
        assertThat(statement.getEndDelimiter()).isEqualTo(";");
        assertThat(statement.toString()).isEmpty();
        assertThat(statement.toJs()).isEqualTo(";");
    }

    @Test
    void constructor_withNullJavaScript_shouldAllowNull() {
        final MongoshStatement statement = new MongoshStatement(null);

        assertThat(statement.getJavaScript()).isNull();
        assertThat(statement.getEndDelimiter()).isEqualTo(";");
        assertThat(statement.toString()).isNull();
        assertThat(statement.toJs()).isEqualTo("null;");
    }

    @Test
    void complexMongoScript_shouldBeHandledCorrectly() {
        final String javascript = 
            "db.users.aggregate([\n" +
            "    { $match: { status: \"active\" } },\n" +
            "    { $group: { _id: \"$department\", count: { $sum: 1 } } }\n" +
            "])";
        final MongoshStatement statement = new MongoshStatement(javascript, "//");

        assertThat(statement.getJavaScript()).isEqualTo(javascript);
        assertThat(statement.getEndDelimiter()).isEqualTo("//");
        assertThat(statement.toString()).isEqualTo(javascript);
        assertThat(statement.toJs()).isEqualTo(javascript + "//");
    }

    @Test
    void multipleStatements_shouldMaintainIndependence() {
        final String js1 = "db.collection1.find()";
        final String js2 = "db.collection2.insertOne({test: true})";
        final String delimiter1 = ";";
        final String delimiter2 = "//";

        final MongoshStatement statement1 = new MongoshStatement(js1, delimiter1);
        final MongoshStatement statement2 = new MongoshStatement(js2, delimiter2);

        assertThat(statement1.getJavaScript()).isEqualTo(js1);
        assertThat(statement1.getEndDelimiter()).isEqualTo(delimiter1);
        assertThat(statement2.getJavaScript()).isEqualTo(js2);
        assertThat(statement2.getEndDelimiter()).isEqualTo(delimiter2);
    }
}
