package liquibase.ext.mongodb.tools;

import liquibase.Scope;
import liquibase.changelog.ChangeSet;
import liquibase.ext.mongodb.configuration.MongoConfiguration;
import liquibase.logging.Logger;
import liquibase.util.FilenameUtil;
import liquibase.util.StringUtil;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Utility class for creating temporary mongosh script files.
 * Handles secure file creation, naming conventions, and cleanup options.
 */
public class MongoshFileCreator {
    
    private final ChangeSet changeSet;
    private final String filenameOverride;
    private final String pathOverride;
    private final boolean overwriteFile;
    private final boolean keepFile;
    
    /**
     * Create a mongosh file creator for the given changeset
     * 
     * @param changeSet the changeset being executed
     * @param filenameOverride optional custom filename pattern
     * @param pathOverride optional custom directory path
     * @param overwriteFile whether to overwrite existing files
     * @param keepFile whether to keep the file after execution
     */
    public MongoshFileCreator(ChangeSet changeSet, String filenameOverride, String pathOverride, 
                              boolean overwriteFile, boolean keepFile) {
        this.changeSet = changeSet;
        this.filenameOverride = filenameOverride;
        this.pathOverride = pathOverride;
        this.overwriteFile = overwriteFile;
        this.keepFile = keepFile;
    }
    
    /**
     * Create a mongosh file creator using configuration defaults
     * 
     * @param changeSet the changeset being executed
     */
    public MongoshFileCreator(ChangeSet changeSet) {
        this(changeSet,
             MongoConfiguration.MONGOSH_TEMP_FILENAME.getCurrentValue(),
             MongoConfiguration.MONGOSH_TEMP_DIRECTORY.getCurrentValue(),
             true,
             MongoConfiguration.MONGOSH_KEEP_TEMP_FILES.getCurrentValue()
        );
    }
    
    /**
     * Generate a temporary file for mongosh script execution
     * 
     * @param fileExtension the file extension (e.g., ".js", ".txt")
     * @return File object for the temporary script file
     * @throws IOException if file creation fails
     */
    public File generateTemporaryFile(String fileExtension) throws IOException {
        Logger log = Scope.getCurrentScope().getLog(getClass());
        
        // Build filename pattern
        String tempFileName = buildFilename(fileExtension);
        log.info("Creating temporary " + fileExtension + " file for changeset '" + 
                 changeSet.getId() + ":" + changeSet.getAuthor() + "'");
        
        File tempFile;
        
        if (StringUtil.isEmpty(pathOverride)) {
            // Use system temp directory
            if (StringUtil.isEmpty(filenameOverride)) {
                // Use Java's createTempFile for secure creation
                tempFile = File.createTempFile(tempFileName, fileExtension);
            } else {
                // Use custom filename in temp directory
                String tempDir = System.getProperty("java.io.tmpdir");
                tempFile = new File(tempDir, tempFileName + fileExtension);
                if (!overwriteFile && tempFile.exists()) {
                    throw new IOException("File already exists and overwrite is disabled: " + tempFile.getAbsolutePath());
                }
                if (!tempFile.createNewFile() && !overwriteFile) {
                    throw new IOException("Failed to create new file: " + tempFile.getAbsolutePath());
                }
            }
        } else {
            // Use custom directory
            Path customPath = Paths.get(pathOverride);
            if (!customPath.toFile().exists()) {
                if (!customPath.toFile().mkdirs()) {
                    throw new IOException("Failed to create directory: " + pathOverride);
                }
            }
            
            tempFile = new File(customPath.toFile(), tempFileName + fileExtension);
            if (!overwriteFile && tempFile.exists()) {
                throw new IOException("File already exists and overwrite is disabled: " + tempFile.getAbsolutePath());
            }
            if (!tempFile.createNewFile() && !overwriteFile) {
                throw new IOException("Failed to create new file: " + tempFile.getAbsolutePath());
            }
        }
        
        // Configure file cleanup
        configureFileCleanup(tempFile, log);
        
        log.info("Created temporary file: " + tempFile.getAbsolutePath());
        return tempFile;
    }
    
    /**
     * Build filename pattern based on changeset and configuration
     * 
     * @param fileExtension the file extension
     * @return sanitized filename
     */
    private String buildFilename(String fileExtension) {
        String filename;
        
        if (StringUtil.isNotEmpty(filenameOverride)) {
            filename = filenameOverride;
        } else {
            // Build default filename pattern: liquibase-{ext}-{id}-{author}-{timestamp}
            filename = "liquibase" + fileExtension + "-" + 
                      changeSet.getId() + "-" + 
                      changeSet.getAuthor() + "-" +
                      System.currentTimeMillis();
        }
        
        // Sanitize filename for filesystem compatibility
        return FilenameUtil.sanitizeFileName(filename);
    }
    
    /**
     * Configure file cleanup behavior based on keepFile setting
     * 
     * @param tempFile the temporary file
     * @param log logger instance
     */
    private void configureFileCleanup(File tempFile, Logger log) {
        if (keepFile) {
            log.info("Temporary file will be retained for debugging: " + tempFile.getAbsolutePath());
        } else {
            // Schedule file for deletion on JVM exit
            tempFile.deleteOnExit();
            log.fine("Temporary file scheduled for cleanup on exit: " + tempFile.getAbsolutePath());
        }
    }
}
