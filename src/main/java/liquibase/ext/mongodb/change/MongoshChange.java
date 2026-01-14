package liquibase.ext.mongodb.change;

import liquibase.change.AbstractSQLChange;
import liquibase.change.DatabaseChange;
import liquibase.database.Database;
import liquibase.exception.ValidationErrors;
import liquibase.ext.mongodb.statement.MongoshStatement;
import liquibase.statement.SqlStatement;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@DatabaseChange(
    name = "mongo", 
    description = "Execute JS/MongoDB shell commands via mongosh",
    priority = 1
)
@NoArgsConstructor
@Getter
public class MongoshChange extends AbstractSQLChange {
    private String MONGO_PROPERTY_NAME = "mongo";
    private String DBMS_PROPERTY_NAME = "dbms";

    private String mongo;

    public void setMongo(String mongo) {
        this.mongo = mongo;
        this.setSql(mongo);
    }
    
    @Override
    public String getConfirmationMessage() {
        return "Mongosh command executed";
    }
    
    @Override
    public SqlStatement[] generateStatements(Database database) {
        List<MongoshStatement> statements = new ArrayList<>();

        String javascript = StringUtils.trimToNull(this.getMongo());
        
        if (javascript == null) {
            return SqlStatement.EMPTY_SQL_STATEMENT;
        }
        
        // Normalize line endings for cross-platform compatibility
        String processedJavaScript = normalizeLineEndings(javascript);
        statements.add(new MongoshStatement(processedJavaScript, this.getEndDelimiter()));
        
        return statements.toArray(new SqlStatement[0]);
    }
    
    @Override
    public ValidationErrors validate(Database database) {
        ValidationErrors validationErrors = new ValidationErrors();

        // Require either mongo property or sql property to have content
        if (StringUtils.trimToNull(this.getMongo()) == null) {
            validationErrors.addError("'mongo' property is required for mongosh changes. ");
        }
        
        return validationErrors;
    }

    public Set<String> getSerializableFields() {
        return new HashSet<>(Arrays.asList(MONGO_PROPERTY_NAME, DBMS_PROPERTY_NAME));
    }
}
