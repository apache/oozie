/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.tools;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.gson.Gson;

import com.google.gson.JsonSyntaxException;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.oozie.BundleActionBean;
import org.apache.oozie.BundleJobBean;
import org.apache.oozie.CoordinatorActionBean;
import org.apache.oozie.CoordinatorJobBean;
import org.apache.oozie.WorkflowActionBean;
import org.apache.oozie.WorkflowJobBean;
import org.apache.oozie.cli.CLIParser;
import org.apache.oozie.executor.jpa.JPAExecutorException;
import org.apache.oozie.service.ConfigurationService;
import org.apache.oozie.service.JPAService;
import org.apache.oozie.service.Services;
import org.apache.oozie.sla.SLARegistrationBean;
import org.apache.oozie.sla.SLASummaryBean;
import org.apache.oozie.store.StoreException;
import org.apache.openjpa.persistence.OpenJPAEntityManagerSPI;
import org.apache.openjpa.persistence.RollbackException;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;
import javax.persistence.FlushModeType;
import javax.persistence.Table;
import javax.persistence.metamodel.EntityType;
import javax.persistence.metamodel.Metamodel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_AC_JSON;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_BNA_JSON;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_BNJ_JSON;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_CA_JSON;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_CJ_JSON;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_SLAREG_JSON;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_SLASUM_JSON;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_SYS_INFO_JSON;
import static org.apache.oozie.tools.OozieDBExportCLI.OOZIEDB_WF_JSON;

/**
 * This class provides the following functionality:
 *  <ul>
 *      <li>imports the data from json files created by {@link OozieDBExportCLI} the specified target zip file</li>
 *      <li>this class uses the current Oozie configuration in {oozie-site.xml}</li>
 *      <li></li>
 *  </ul>
 */
public class OozieDBImportCLI {
    private static final String[] HELP_INFO = {
            "",
            "OozieDBImportCLI reads Oozie database from a zip file."
    };
    private static final String IMPORT_CMD = "import";
    private static final String HELP_CMD = "help";
    public static final String OOZIE_DB_IMPORT_BATCH_SIZE_KEY = "oozie.db.import.batch.size";
    static final int DEFAULT_BATCH_SIZE = 1000;
    private static int IMPORT_BATCH_SIZE;
    private static final String OPTION_VERBOSE_SHORT = "v";
    private static final String OPTION_VERBOSE_LONG = "verbose";

    private final EntityManager entityManager;
    private final ZipFile mainZipFile;
    private final boolean verbose;
    private boolean cleanupNecessary = false;
    private final Set<Class<?>> entityClasses = Sets.newLinkedHashSet();

    private OozieDBImportCLI(final EntityManager entityManager, final ZipFile mainZipFile, final boolean verbose) {
        this.entityManager = entityManager;
        this.mainZipFile = mainZipFile;
        this.verbose = verbose;
    }

    public static void main(final String[] args) throws ParseException {
        final CLIParser parser = new CLIParser("oozie-setup.sh", HELP_INFO);
        parser.addCommand(HELP_CMD, "", "display usage for all commands or specified command", new Options(), false);
        parser.addCommand(IMPORT_CMD, "",
                "imports the contents of the Oozie database from the specified file",
                new Options().addOption(OPTION_VERBOSE_SHORT, OPTION_VERBOSE_LONG, false, "Enables verbose logging."), true);
        boolean verbose = false;

        try {
            final CLIParser.Command command = parser.parse(args);
            if (command.getName().equals(IMPORT_CMD)) {
                final Services services = new Services();
                services.getConf().set(Services.CONF_SERVICE_CLASSES, JPAService.class.getName());
                services.getConf().set(Services.CONF_SERVICE_EXT_CLASSES, "");
                services.init();
                setImportBatchSize();
                System.out.println("==========================================================");
                System.out.println(Arrays.toString(command.getCommandLine().getArgs()));
                System.out.println(String.format("Import batch length is %d", IMPORT_BATCH_SIZE));

                verbose = command.getCommandLine().hasOption(OPTION_VERBOSE_SHORT)
                        || command.getCommandLine().hasOption(OPTION_VERBOSE_LONG);

                importAllDBTables(command.getCommandLine().getArgs()[0], verbose);
            } else if (command.getName().equals(HELP_CMD)) {
                parser.showHelp(command.getCommandLine());
            }
        } catch (final ParseException pex) {
            System.err.println("Invalid sub-command: " + pex.getMessage());
            System.err.println();
            System.err.println(parser.shortHelp());
            System.exit(1);
        } catch (final Throwable e) {
            System.err.println();
            System.err.println("Error: " + e.getMessage());
            System.err.println();

            if (verbose) {
                System.err.println("Stack trace for the error was (for debug purposes):");
                System.err.println("--------------------------------------");
                e.printStackTrace(System.err);
                System.err.println("--------------------------------------");
                System.err.println();
            }

            System.exit(1);
        } finally {
            if (Services.get() != null) {
                Services.get().destroy();
            }
        }
    }

    private static void setImportBatchSize() {
        if (!Strings.isNullOrEmpty(System.getProperty(OOZIE_DB_IMPORT_BATCH_SIZE_KEY))) {
            try {
                IMPORT_BATCH_SIZE = Integer.parseInt(System.getProperty(OOZIE_DB_IMPORT_BATCH_SIZE_KEY));
            }
            catch (final NumberFormatException e) {
                IMPORT_BATCH_SIZE = ConfigurationService.getInt(OOZIE_DB_IMPORT_BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE);
            }
        }
        else {
            IMPORT_BATCH_SIZE = ConfigurationService.getInt(OOZIE_DB_IMPORT_BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE);
        }
    }

    private static void importAllDBTables(final String zipFileName, final boolean verbose) throws StoreException, IOException,
            JPAExecutorException, SQLException {

        EntityManager entityManager = null;

        try (ZipFile mainZipFile = new ZipFile(zipFileName)) {
            entityManager = Services.get().get(JPAService.class).getEntityManager();
            entityManager.setFlushMode(FlushModeType.COMMIT);

            final OozieDBImportCLI importer = new OozieDBImportCLI(entityManager, mainZipFile, verbose);

            importer.checkDBVersion();

            importer.checkTablesArePresentAndEmpty();

            importer.importOneInputFileToOneEntityTable(WorkflowJobBean.class, OOZIEDB_WF_JSON);
            importer.importOneInputFileToOneEntityTable(WorkflowActionBean.class, OOZIEDB_AC_JSON);
            importer.importOneInputFileToOneEntityTable(CoordinatorJobBean.class, OOZIEDB_CJ_JSON);
            importer.importOneInputFileToOneEntityTable(CoordinatorActionBean.class, OOZIEDB_CA_JSON);
            importer.importOneInputFileToOneEntityTable(BundleJobBean.class, OOZIEDB_BNJ_JSON);
            importer.importOneInputFileToOneEntityTable(BundleActionBean.class, OOZIEDB_BNA_JSON);
            importer.importOneInputFileToOneEntityTable(SLARegistrationBean.class, OOZIEDB_SLAREG_JSON);
            importer.importOneInputFileToOneEntityTable(SLASummaryBean.class, OOZIEDB_SLASUM_JSON);

            final boolean cleanupPerformed = importer.cleanupIfNecessary();

            checkState(!cleanupPerformed, "DB cleanup happened due to skipped rows. " +
                    "See previous log entries about what rows were skipped and why.");
        } finally {
            if (entityManager != null) {
                entityManager.close();
            }
        }

    }

    private void checkDBVersion() throws IOException {
        try {
            final String currentDBVersion = (String) entityManager
                    .createNativeQuery("select data from OOZIE_SYS where name='db.version'")
                    .getSingleResult();
            String dumpDBVersion = null;
            final ZipEntry sysInfoEntry = mainZipFile.getEntry(OOZIEDB_SYS_INFO_JSON);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(mainZipFile.getInputStream(sysInfoEntry), Charsets.UTF_8))) {
                String line;
                final Gson gson = new Gson();
                while ((line = reader.readLine()) != null) {
                    final List<String> info = gson.fromJson(line, List.class);
                    if (info.size() > 1 && "db.version".equals(info.get(0))) {
                        dumpDBVersion = info.get(1);
                    }
                }
            }

            if (currentDBVersion.equals(dumpDBVersion)) {
                System.out.println("Loading to Oozie database version " + currentDBVersion);
            }
            else {
                System.err.println("ERROR Oozie database version mismatch.");
                System.err.println("Oozie DB version:\t" + currentDBVersion);
                System.err.println("Dump DB version:\t" + dumpDBVersion);
                System.exit(1);
            }
        } catch (final Exception e) {
            System.err.println();
            System.err.println("Error during DB version check: " + e.getMessage());
            System.err.println();

            if (verbose) {
                System.err.println("Stack trace for the error was (for debug purposes):");
                System.err.println("--------------------------------------");
                e.printStackTrace(System.err);
                System.err.println("--------------------------------------");
                System.err.println();
            }
        }
    }

    private void checkTablesArePresentAndEmpty() throws SQLException {
        checkTableIsPresentAndEmpty(WorkflowJobBean.class);
        checkTableIsPresentAndEmpty(WorkflowActionBean.class);
        checkTableIsPresentAndEmpty(CoordinatorJobBean.class);
        checkTableIsPresentAndEmpty(CoordinatorActionBean.class);
        checkTableIsPresentAndEmpty(BundleJobBean.class);
        checkTableIsPresentAndEmpty(BundleActionBean.class);
        checkTableIsPresentAndEmpty(SLARegistrationBean.class);
        checkTableIsPresentAndEmpty(SLASummaryBean.class);
    }

    private <E> void checkTableIsPresentAndEmpty(final Class<E> entityClass) throws SQLException {
        final OpenJPAEntityManagerSPI entityManagerDelegate = (OpenJPAEntityManagerSPI) entityManager.getDelegate();
        final Connection connection = (Connection) entityManagerDelegate.getConnection();
        final DatabaseMetaData metaData = connection.getMetaData();
        final String tableName = findTableName(entityManager, entityClass);

        try (final ResultSet rs = metaData.getTables(null, null, tableName, null)) {
            checkState(rs.next(),
                    String.format("Table [%s] does not exist for class [%s].", tableName, entityClass.getSimpleName()));
        }

        final long entityCount = getEntityCount(entityClass);

        checkState(entityCount == 0,
                String.format("There are already [%d] entries in table [%s] for class [%s], should be empty.",
                        entityCount,
                        tableName,
                        entityClass.getSimpleName()));
    }

    private <E> long getEntityCount(final Class<E> entityClass) {
        return entityManager.createQuery(
                    String.format("SELECT COUNT(e) FROM %s e", entityClass.getSimpleName()), Long.class)
                    .getSingleResult();
    }

    /**
     * Import all the contents of the input JSON file to one database table where the {@link javax.persistence.Entity} instances are
     * stored. This call hides batch {@link EntityTransaction} handling details, as well as trying to commit pending entities
     * one-by-one, if needed.
     * @param entityClass the class to persist
     * @param importFileName the JSON file name
     * @param <E> {@link javax.persistence.Entity} type
     * @throws JPAExecutorException
     */
    private <E> void importOneInputFileToOneEntityTable(final Class<E> entityClass, final String importFileName) {
        final BatchTransactionHandler<E> batchTransactionHandler = new BatchTransactionHandler<>();
        final BatchEntityPersister<E> batchEntityPersister = new BatchEntityPersister<>(entityClass,
                importFileName, batchTransactionHandler);

        final List<E> batch = Lists.newArrayList();
        final Gson gson = new Gson();
        final ZipEntry importEntry = mainZipFile.getEntry(importFileName);

        if (importEntry != null) {
            long lineIndex = 1L;
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(mainZipFile.getInputStream(importEntry), Charsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    final E newEntity = gson.fromJson(line, entityClass);
                    batch.add(newEntity);

                    if (lineIndex % IMPORT_BATCH_SIZE == 0) {
                        System.out.println(String.format("Batch is full, persisting. [lineIndex=%s;batch.size=%s]",
                                lineIndex, batch.size()));
                        batchEntityPersister.persist(batch);
                    }

                    lineIndex++;
                }
            } catch (final IOException e) {
                rollbackAndThrow(importFileName, batchTransactionHandler, e);
            } catch (final JsonSyntaxException e) {
                if (verbose) {
                    System.err.println(String.format("JSON error. [lineIndex=%s;e.message=%s]", lineIndex, e.getMessage()));
                }
                rollbackAndThrow(importFileName, batchTransactionHandler, e);
            }
        }

        if (!batch.isEmpty()) {
            System.out.println(String.format("Persisting last batch. [batch.size=%s]", batch.size()));
            batchEntityPersister.persist(batch);
        }

        final String tableName = findTableName(entityManager, entityClass);
        System.out.println(String.format("%s row(s) imported to table %s.",
                batchTransactionHandler.getTotalPersistedCount(),
                tableName));

        if (batchTransactionHandler.getTotalSkippedCount() > 0) {
            System.err.println(
                    String.format("[%s] row(s) skipped while importing to table [%s]. " +
                                    "Will remove all the rows of all the tables to get clean data.",
                    batchTransactionHandler.getTotalSkippedCount(),
                    tableName));

            cleanupNecessary = true;
        }
    }

    private boolean cleanupIfNecessary() {
        if (!cleanupNecessary) {
            System.out.println("Cleanup not necessary, no entities skipped.");

            return false;
        }

        System.out.println(String.format("Performing cleanup of [%d] tables due to skipped entities.", entityClasses.size()));

        for (final Class<?> entityClass : entityClasses) {
            final String tableName = findTableName(entityManager, entityClass);
            System.out.println(String.format("Cleaning up table [%s].", tableName));

            final BatchTransactionHandler<?> batchTransactionHandler = new BatchTransactionHandler<>();

            batchTransactionHandler.begin();

            entityManager.createQuery(String.format("DELETE FROM %s e", entityClass.getSimpleName())).executeUpdate();

            batchTransactionHandler.commit();

            System.out.println(String.format("Table [%s] cleaned up.", tableName));
        }

        System.out.println(String.format("Cleanup of [%d] tables due to skipped entities performed.", entityClasses.size()));

        return true;
    }

    private <E> void rollbackAndThrow(final String importFileName,
                                      final BatchTransactionHandler<E> batchTransactionHandler,
                                      final Exception cause) {
        batchTransactionHandler.rollbackIfActive();

        throw new RuntimeException(String.format("Import failed from json [zippedFileName=%s;e.message=%s].", importFileName,
                cause.getMessage()), cause);
    }

    static <E> String findTableName(final EntityManager entityManager, final Class<E> entityClass) {
        final Metamodel meta = entityManager.getMetamodel();
        final EntityType<E> entityType = meta.entity(entityClass);

        final Table t = entityClass.getAnnotation(Table.class);

        final String tableName = (t == null)
                ? entityType.getName().toUpperCase()
                : t.name();

        return tableName;
    }

    /**
     * Handles batch transactions, that is, the actual commit will be done when the number of persistable entities reach the batch
     * limit. This was needed because importing everything in a huge {@link EntityTransaction} (and holding in
     * {@link EntityManager} heap) has resulted in {@link OutOfMemoryError} for large input JSON files.
     * <p/>
     * When there is some problem while persisting one of them, usually it's an issue that
     * gets revealed when we're about to call {@link EntityTransaction#commit()}. If this call fails, the caller can get all the
     * pending {@link javax.persistence.Entity} instances that have not been persisted successfully, and make some kind of retry
     * using a new {@link BatchTransactionHandler} instance.
     *
     * @param <E> the {@link javax.persistence.Entity} class
     */
    private class BatchTransactionHandler<E> {

        private EntityTransaction currentTransaction;
        private int totalPersistedCount = 0;
        private int totalSkippedCount = 0;
        private List<E> pendingEntities = Lists.newArrayList();

        /**
         * Begin recording the {@link EntityTransaction}
         */
        void begin() {
            currentTransaction = entityManager.getTransaction();
            currentTransaction.begin();

            pendingEntities.clear();
        }

        /**
         * Commit the {@link EntityTransaction}
         */
        void commit() {
            checkNotNull(currentTransaction, "TX should be open.");

            currentTransaction.commit();

            totalPersistedCount += pendingEntities.size();
            pendingEntities.clear();
        }

        /**
         * Rollback if {@link EntityTransaction} is active
         */
        void rollbackIfActive() {
            if (currentTransaction == null) {
                return;
            }

            if (currentTransaction.isActive()) {
                currentTransaction.rollback();
            }

            pendingEntities.clear();
        }

        /**
         * Persist a new {@link javax.persistence.Entity} instance
         * @param newEntity the new {@link javax.persistence.Entity} instance
         */
        void persist(final E newEntity) {
            checkNotNull(currentTransaction, "TX should be open.");

            entityManager.persist(newEntity);
            pendingEntities.add(newEntity);

            if (pendingEntities.size() == IMPORT_BATCH_SIZE) {
                commit();
                begin();
            }
        }

        /**
         * Persist and try to commit a pending {@link javax.persistence.Entity} instance, that is, one that was part of a failing
         * {@link EntityTransaction#commit()} call
         * @param pendingEntity the pending {@link javax.persistence.Entity} instance
         */
        void persistAndTryCommit(final E pendingEntity) {
            try {
                currentTransaction.begin();
                entityManager.persist(pendingEntity);
                currentTransaction.commit();

                totalPersistedCount++;
            } catch (final RollbackException re) {
                if (verbose) {
                    System.err.println(String.format("Cannot persist entity, skipping. [re.failedObject=%s]",
                            re.getFailedObject()));
                }

                totalSkippedCount++;
            }
        }

        /**
         * Number of persisted {@link javax.persistence.Entity} instances
         * @return how many {@link javax.persistence.Entity}es have already been persisted by this {@link BatchTransactionHandler}
         */
        int getTotalPersistedCount() {
            return totalPersistedCount;
        }

        /**
         * Number of skipped {@link javax.persistence.Entity} instances
         * @return how many {@link javax.persistence.Entity}es have already been skipped by this {@link BatchTransactionHandler}
         */
        int getTotalSkippedCount() {
            return totalSkippedCount;
        }

        /**
         * All the pending {@link javax.persistence.Entity} instance that have been tried to {@link EntityManager#persist(Object)}
         * but have not yet been {@link EntityTransaction#commit()}ed.
         * @return how many {@link javax.persistence.Entity}es have not been committed by this {@link BatchTransactionHandler}
         */
        List<E> getPendingEntities() {
            return pendingEntities;
        }
    }

    /**
     * Persists entities in batches. Delegates to {@link BatchTransactionHandler} methods.
     * <p/>
     * As entities are being persisted, the backing {@code List} is also being emptied. This is to relieve GC cycles when batch
     * size is such that entities fitting into one batch would cause {@link OutOfMemoryError}.
     * @param <E>
     */
    private class BatchEntityPersister<E> {

        private final Class<E> entityClass;
        private final String importFileName;
        private final BatchTransactionHandler<E> batchTransactionHandler;

        private BatchEntityPersister(final Class<E> entityClass,
                                     final String importFileName,
                                     final BatchTransactionHandler<E> batchTransactionHandler) {
            this.entityClass = entityClass;
            this.importFileName = importFileName;
            this.batchTransactionHandler = batchTransactionHandler;
        }

        void persist(final List<E> batch) {
            if (batch.isEmpty()) {
                System.out.println("No entities to import.");
                return;
            }

            while (!batch.isEmpty()) {
                try {
                    entityClasses.add(entityClass);

                    batchTransactionHandler.begin();

                    final ListIterator<E> iterator = batch.listIterator();

                    while (iterator.hasNext()) {
                        final E entityToPersist = iterator.next();
                        iterator.remove();
                        batchTransactionHandler.persist(entityToPersist);
                    }

                    batchTransactionHandler.commit();
                } catch (final RollbackException re) {
                    for (final E pendingEntity : batchTransactionHandler.getPendingEntities()) {
                        batchTransactionHandler.persistAndTryCommit(pendingEntity);
                    }
                } catch (final Exception e) {
                    rollbackAndThrow(importFileName, batchTransactionHandler, e);
                }
            }
        }
    }
}
