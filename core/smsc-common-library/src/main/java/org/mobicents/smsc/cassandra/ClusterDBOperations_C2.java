/*
 * TeleStax, Open Source Cloud Communications  Copyright 2012.
 * and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.mobicents.smsc.cassandra;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import com.datastax.driver.core.utils.Bytes;
import org.apache.log4j.Logger;
import org.mobicents.smsc.library.Alert;
import org.mobicents.smsc.library.CorrelationIdValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * @author sergey vetyutnev
 */
public class ClusterDBOperations_C2 {
    private static final Logger logger = Logger.getLogger(ClusterDBOperations_C2.class);

    private static final ClusterDBOperations_C2 instance = new ClusterDBOperations_C2();
    private final Alert[] noAlerts = new Alert[0];
    private final String[] noSmscNames = new String[0];
    protected Session session;
    // cassandra access
    private Cluster cluster;
    private volatile boolean started = false;
    private boolean databaseAvailable = false;
    private PreparedStatement removeAlertByMsisdn;

    //each smsc has its own table for alerts, PreparedStatement will be initialized lazily
    private Map<String, PreparedStatement> insertAlertTable = new HashMap<>();
    private PreparedStatement selectNotDeletedAlerts;
    private PreparedStatement selectCorrelationId;
    private PreparedStatement insertCorrelationId;
    private PreparedStatement insertExpectedAlert;
    private PreparedStatement selectExpectedAlert;
    private PreparedStatement selectCurrentSmscExpectedAlert;
    private PreparedStatement removeExpectedAlertByMsisdn;
    private String smscName;
    private int expectedAlertTtl;

    private ClusterDBOperations_C2() {
        super();
    }

    public static ClusterDBOperations_C2 getInstance() {
        return instance;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isDatabaseAvailable() {
        return databaseAvailable;
    }

    protected Session getSession() {
        return this.session;
    }

    public void start(String hosts, int port, String keyspace, String smscName, int expectedAlertTtl) throws Exception {
        logger.debug("Starting ClusterDBOperations_C2");
        if (this.started) {
            throw new Exception("DBOperations already started");
        }
        Cluster.Builder builder = Cluster.builder();
        this.smscName = smscName;
        this.expectedAlertTtl = expectedAlertTtl;

        try {
            String[] cassHostsArray = hosts.split(",");
            builder.addContactPoints(cassHostsArray);
            builder.withPort(port);
            builder.withLoadBalancingPolicy(LatencyAwarePolicy
                    .builder(new DCAwareRoundRobinPolicy())
                    .build()
            );
            this.cluster = builder.build().init();
        } catch (Exception e) {
            logger.error(
                    String.format(
                            "Failure of connting to shared cassandra database. : host=%s, port=%d. SMSC GW will work cluster support\n",
                            hosts, port), e);
            this.started = true;
            return;
        }

        this.databaseAvailable = true;

        Metadata metadata = cluster.getMetadata();
        logger.info(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
        for (Host host : metadata.getAllHosts()) {
            logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
                    host.getRack()));
        }
        session = cluster.connect();

        session.execute("USE \"" + keyspace + "\"");

        createTablesIfNotExists();
        prepareCQLStatements();

        this.started = true;
    }

    private void prepareCQLStatements() {
        String sharedAlertsTAbleName = getSharedAlertsTAbleName();
        PreparedStatementCollection_C3.Builder builder = new PreparedStatementCollection_C3.Builder(session);

        removeAlertByMsisdn = builder.delete()
                .from(sharedAlertsTAbleName)
                .where(
                        PreparedStatementCollection_C3.Builder.and(
                                PreparedStatementCollection_C3.Builder.eq(Schema.COLUMN_MSISDN)
                        )
                ).build();

        selectNotDeletedAlerts = builder.selectAll().from(sharedAlertsTAbleName).build();

        selectCorrelationId = builder.selectAll()
                .from(Schema.FAMILY_SHARED_CORRELATION_ID)
                .where(
                        PreparedStatementCollection_C3.Builder.eq(Schema.COLUMN_ID)
                ).build();

        insertCorrelationId = builder.insertInto(Schema.FAMILY_SHARED_CORRELATION_ID)
                .values(Schema.COLUMN_ID, Schema.COLUMN_VALUE)
                .usingTtl()
                .build();

        selectExpectedAlert = builder.selectAll()
                .from(Schema.FAMILY_SHARED_EXPECTED_ALERTS)
                .where(
                        PreparedStatementCollection_C3.Builder.eq(Schema.COLUMN_MSISDN)
                ).build();

        String msisdnAndSmscName = PreparedStatementCollection_C3.Builder.and(
                PreparedStatementCollection_C3.Builder.eq(Schema.COLUMN_MSISDN),
                PreparedStatementCollection_C3.Builder.eq(Schema.COLUMN_SMSC)
        );

        selectCurrentSmscExpectedAlert = builder.selectAll()
                .from(Schema.FAMILY_SHARED_EXPECTED_ALERTS)
                .where(msisdnAndSmscName).build();

        removeExpectedAlertByMsisdn = builder.delete()
                .from(Schema.FAMILY_SHARED_EXPECTED_ALERTS)
                .where(msisdnAndSmscName).build();

        insertExpectedAlert = builder.insertInto(Schema.FAMILY_SHARED_EXPECTED_ALERTS)
                .values(Schema.COLUMN_MSISDN, Schema.COLUMN_PARTIAL_TARGET_ID, Schema.COLUMN_SMSC, Schema.COLUMN_CREATED)
                .usingTtl()
                .build();
    }

    private String getSharedAlertsTAbleName() {
        return Schema.FAMILY_SHARED_ALERTS + "_" + this.smscName;
    }

    private void createTablesIfNotExists() {
        createAlertTable();
        createTableSharedCorelationId();
        createTableSharedExpectedAlerts();
    }

    private void createTableSharedExpectedAlerts() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"");
        sb.append(Schema.FAMILY_SHARED_EXPECTED_ALERTS);
        sb.append("\" (");

        appendField(sb, Schema.COLUMN_MSISDN, "ascii");
        appendField(sb, Schema.COLUMN_PARTIAL_TARGET_ID, "ascii");
        appendField(sb, Schema.COLUMN_SMSC, "ascii");
        appendField(sb, Schema.COLUMN_CREATED, "timestamp");

        sb.append(
                String.format("PRIMARY KEY (\"%s\", \"%s\", \"%s\")",
                        Schema.COLUMN_MSISDN,
                        Schema.COLUMN_SMSC,
                        Schema.COLUMN_PARTIAL_TARGET_ID)
        );
        sb.append(");");

        String createTableQuery = sb.toString();
        PreparedStatement ps = session.prepare(createTableQuery);
        session.execute(ps.bind());
    }

    /**
     * each smsc in cluster will have it's own alert table == SHARED_ALERTS_${CLUSTERING_SMSC_NAME}
     */
    private void createAlertTable() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"");
        sb.append(getSharedAlertsTAbleName());
        sb.append("\" (");

        appendField(sb, Schema.COLUMN_MSISDN, "ascii");
        appendField(sb, Schema.COLUMN_PARTIAL_TARGET_ID, "ascii");
        appendField(sb, Schema.COLUMN_CREATED, "timestamp");
        sb.append(
                String.format("PRIMARY KEY (\"%s\", \"%s\")", Schema.COLUMN_MSISDN, Schema.COLUMN_PARTIAL_TARGET_ID)
        );

        sb.append(");");

        String createTableQuery = sb.toString();
        PreparedStatement ps = session.prepare(createTableQuery);
        session.execute(ps.bind());
    }

    private void createTableSharedCorelationId() {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS \"");
        sb.append(Schema.FAMILY_SHARED_CORRELATION_ID);
        sb.append("\" (");

        appendField(sb, Schema.COLUMN_ID, "ascii");
        appendField(sb, Schema.COLUMN_VALUE, "blob");

        sb.append(
                String.format("PRIMARY KEY (\"%s\")", Schema.COLUMN_ID)
        );
        sb.append(");");

        String createTableQuery = sb.toString();
        PreparedStatement ps = session.prepare(createTableQuery);
        session.execute(ps.bind());
    }

    private void appendField(StringBuilder sb, String name, String type) {
        sb.append("\"");
        sb.append(name);
        sb.append("\" ");
        sb.append(type);
        sb.append(", ");
    }

    public void stop() throws Exception {
        if (!this.started)
            return;

        if (cluster != null && !cluster.isClosed()) {
            Metadata metadata = cluster.getMetadata();
            cluster.close();
            logger.info(String.format("Disconnected from cluster: %s\n", metadata.getClusterName()));
        }

        this.started = false;
        this.databaseAvailable = false;
    }

    /**
     * if alert exist in shared DB, this function will remove it from SHARED_ALERTS
     *
     * @param msisdn
     */
    public void c2_removeAlert(String msisdn) {
        if (!started || !databaseAvailable) {
            logger.warn(String.format("Cluster DB not available, can't remove alert with msisdn = %s", msisdn));
            return;
        }

        logger.debug(String.format("Trying to remove alert with msisdn = %s", msisdn));

        try {
            BoundStatement boundStatement = removeAlertByMsisdn.bind(msisdn);
            session.executeAsync(boundStatement);
        } catch (Exception e) {
            logger.error(String.format("Can't remove alert for msisdn = %s", msisdn));
        }
    }

    /**
     * Insert alerts to SHARED_ALERTS
     *
     * @param alerts
     */
    public void c2_insertAlerts(Alert[] alerts) {
        if (!started || !databaseAvailable) {
            logger.warn("DB not available, failed to insert alert");
            return;
        }
        BatchStatement batch = new BatchStatement();

        for (Alert alert : alerts) {
            if (alert.getSmscName() == null) {
                logger.warn("Can't get PreparedStatement for inserting one of alert, smsc name not set. Alert: " + alert.toString());
                continue;
            }

            PreparedStatement insertAlert = getPreparedStatementForSmsc(alert.getSmscName());
            BoundStatement boundStatement = insertAlert.bind(
                    alert.getMsisdn(),
                    alert.getPartialTargetId(),
                    alert.getCreatedDate()
            );
            batch.add(boundStatement);
            // TODO: Remove the records from Main Shared table.
        }

        try {
            session.executeAsync(batch);
        } catch (Exception e) {
            logger.error("Can't insert alert", e);
        }
    }

    private PreparedStatement getPreparedStatementForSmsc(String smscName) {
        PreparedStatement preparedStatement = insertAlertTable.get(smscName);
        if (preparedStatement == null) {
            PreparedStatementCollection_C3.Builder builder = new PreparedStatementCollection_C3.Builder(this.session);
            preparedStatement = builder.insertInto(Schema.FAMILY_SHARED_ALERTS + "_" + smscName)
                    .values(
                            Schema.COLUMN_MSISDN,
                            Schema.COLUMN_PARTIAL_TARGET_ID,
                            Schema.COLUMN_CREATED
                    ).build();
            insertAlertTable.put(smscName, preparedStatement);
        }
        return preparedStatement;
    }

    /**
     * Select all alerts for current smsc
     */
    public Alert[] c2_getAlerts() {
        if (!started || !databaseAvailable)
            return noAlerts;

        try {
            ResultSet result = session.execute(selectNotDeletedAlerts.bind());
            int availableWithoutFetching = result.getAvailableWithoutFetching();
            if (availableWithoutFetching < 1)
                return noAlerts;

            Alert[] alertResList = new Alert[availableWithoutFetching];

            int currentIndex = 0;
            for (Row row : result.all()) {
                alertResList[currentIndex++] = new Alert(row.getString(Schema.COLUMN_MSISDN), row.getString(Schema.COLUMN_PARTIAL_TARGET_ID), this.smscName, row.getDate(Schema.COLUMN_CREATED));
            }

            logger.debug("Retrieved " + currentIndex+1 + " pending alerts");

            return alertResList;
        } catch (Exception e) {
            logger.error("Can't fetch alerts", e);
            return noAlerts;
        }
    }

    public CorrelationIdValue c2_getCorrelationID(String correlationID) {
        if (!started || !databaseAvailable) {
            logger.warn("DB not available, can't fetch CorrelationIdValue");
            return null;
        }

        try {
            BoundStatement boundStatement = selectCorrelationId.bind(correlationID);
            ResultSet result = session.execute(boundStatement);
            CorrelationIdValue res = null;
            Row row = result.one();
            if (row != null) {
                res = deserializeFromCassandraBlob(row.getBytes(Schema.COLUMN_VALUE));
            }
            return res;
        } catch (Exception e) {
            logger.error("Can't get CorrelationIdValue", e);
            return null;
        }
    }

    public void c2_insertCorrelationId(CorrelationIdValue elem, int ttl) {
        if (!started || !databaseAvailable) {
            logger.warn("DB not available, can't insert CorrelationIdValue");
            return;
        }

        try {
            BoundStatement boundStatement = new BoundStatement(insertCorrelationId);
            boundStatement.setString(0, elem.getCorrelationID());
            boundStatement.setBytes(1, serializeToCassandraBlob(elem));
            boundStatement.setInt(2, ttl);
            session.executeAsync(boundStatement);
        } catch (Exception e) {
            logger.error("Can't insert CorrelationIdValue", e);
        }
    }

    private ByteBuffer serializeToCassandraBlob(CorrelationIdValue correlationIdValue) {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(bytes)) {
            oos.writeObject(correlationIdValue);
            String hexString = Bytes.toHexString(bytes.toByteArray());
            return Bytes.fromHexString(hexString);
        } catch (IOException e) {
            logger.error("Serializing CorrelationIdValue object error", e);
            return null;
        }
    }

    private CorrelationIdValue deserializeFromCassandraBlob(ByteBuffer bytes) {
        String hx = Bytes.toHexString(bytes);
        ByteBuffer ex = Bytes.fromHexString(hx);
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(ex.array()))) {
            return (CorrelationIdValue) ois.readObject();
        } catch (ClassNotFoundException | IOException e) {
            logger.error("Deserializing CorrelationIdValue object error", e);
            return null;
        }
    }

    /**
     * checks if current smsc expect alerts for current tartgetId
     * @param msisdn
     */
    public boolean c2_checkSmscExpectAlerts(String msisdn) {
        if (!started || !databaseAvailable) {
            logger.warn("Cluster DB not available, can't check sms data;");
            return false;
        }

        try {
            BoundStatement boundStatement = selectCurrentSmscExpectedAlert.bind(msisdn, this.smscName);
            ResultSet res = session.execute(boundStatement);
            boolean alertExpected = res.getAvailableWithoutFetching() > 0;
            logger.debug(String.format("Is alert for msisdn '%s' expected == %b", msisdn, alertExpected));
            return alertExpected;
        } catch (Exception e) {
            logger.error("Failed to check if smsc expect alerts for msisdn = " + msisdn, e);
            return false;
        }
    }

    public void c2_removeExpectedAlert(String msisdn) {
        if (!started || !databaseAvailable) {
            logger.warn("Cluster DB not available, can't check remove expected alert;");
            return;
        }

        logger.debug("Removing expected alert for msisdn = " + msisdn);

        try {
            BoundStatement boundStatement = removeExpectedAlertByMsisdn.bind(msisdn, this.smscName);
            session.execute(boundStatement);
        }catch (Exception e) {
            logger.error("Failed to remove expected alert for msisdn = " + msisdn, e);
        }
    }

    /**
     * find all smsc names, except current smsc name
     */
    public String[] c2_findSmscNamesExcludingCurrentSmsc(String msisdn) {
        if (!started || !databaseAvailable) {
            logger.warn("Cluster DB not available, can't find sms name;");
            return noSmscNames;
        }

        try {
            BoundStatement boundStatement = selectExpectedAlert.bind(msisdn);
            ResultSet res = session.execute(boundStatement);
            int resultLength = res.getAvailableWithoutFetching();
            if (resultLength == 0) {
                return noSmscNames;
            }

            String[] names = new String[resultLength];
            int index = 0;
            for (Row row : res.all()) {
                String smscName = row.getString(Schema.COLUMN_SMSC);
                if (!smscName.equals(this.smscName)) {
                    names[index++] = smscName;
                }
            }

            return names;
        } catch (Exception e) {
            logger.error("Failed to check if smsc expect alerts for msisdn = " + msisdn, e);
            return noSmscNames;
        }
    }

    public List<Alert> c2_findExpectedAlertsForMsisdn(String msisdn) {
        if (!started || !databaseAvailable) {
            logger.warn("Cluster DB not available, can't find sms name;");
            return Collections.emptyList();
        }

        try {
            BoundStatement boundStatement = selectExpectedAlert.bind(msisdn);
            ResultSet res = session.execute(boundStatement);

            List<Alert> resultSet = new ArrayList<>();
            for (Row row : res.all()) {
                Alert alert = new Alert(row.getString(Schema.COLUMN_MSISDN),
                        row.getString(Schema.COLUMN_PARTIAL_TARGET_ID),
                        row.getString(Schema.COLUMN_SMSC),
                        row.getDate(Schema.COLUMN_CREATED));
                resultSet.add(alert);
            }

            return resultSet;
        } catch (Exception e) {
            logger.error("Failed to check if smsc expect alerts for msisdn = " + msisdn, e);
            return Collections.emptyList();
        }
    }


    public void c2_insertExpectedAlert(String msisdn, String partialTargetId) {
        if (!started || !databaseAvailable) {
            logger.warn("Cluster DB not available, can't insert expected alert;");
            return;
        }

        try {
            BoundStatement boundStatement = insertExpectedAlert.bind(msisdn, partialTargetId, this.smscName, new Date(), this.expectedAlertTtl);
            session.executeAsync(boundStatement);
        } catch (Exception e) {
            logger.error("Can't insert CorrelationIdValue", e);
        }
    }
}
