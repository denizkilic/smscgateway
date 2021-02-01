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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.LatencyAwarePolicy;
import javolution.util.FastList;
import javolution.util.FastMap;
import javolution.xml.XMLObjectReader;
import javolution.xml.XMLObjectWriter;
import javolution.xml.stream.XMLStreamException;

import org.apache.log4j.Logger;
import org.mobicents.protocols.ss7.map.api.smstpdu.CharacterSet;
import org.mobicents.protocols.ss7.map.api.smstpdu.DataCodingScheme;
import org.mobicents.protocols.ss7.map.smstpdu.DataCodingSchemeImpl;
import org.mobicents.smsc.library.DbSmsRoutingRule;
import org.mobicents.smsc.library.SmType;
import org.mobicents.smsc.library.Sms;
import org.mobicents.smsc.library.SmsSet;
import org.mobicents.smsc.library.SmsSetCache;
import org.mobicents.smsc.library.TargetAddress;
import org.mobicents.smsc.smpp.TlvSet;

import com.cloudhopper.smpp.SmppConstants;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.InvalidQueryException;

/**
 *
 * @author sergey vetyutnev
 *
 */
public class DBOperations_C2 {
    private static final Logger logger = Logger.getLogger(DBOperations_C2.class);

    public static final String TLV_SET = "tlvSet";
    public static final UUID emptyUuid = UUID.nameUUIDFromBytes(new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            0, 0 });
    public static final int IN_SYSTEM_UNSENT = 0;
    public static final int IN_SYSTEM_INPROCESS = 1;
    public static final int IN_SYSTEM_SENT = 2;

    public static final int CURRENT_DUE_SLOT = 0;
    public static final int NEXT_MESSAGE_ID = 1;
    public static final int NEXT_CORRELATION_ID = 2;
    public static final long MAX_MESSAGE_ID = 10000000000L;
    public static final long MESSAGE_ID_LAG = 1000;
    public static final long DUE_SLOT_WRITING_POSSIBILITY_DELAY = 10;

    private static final DBOperations_C2 instance = new DBOperations_C2();

    // cassandra access
    private Cluster cluster;
    protected Session session;

    // hardcored configuring data - for table structures

    // multiTableModel: splitting database tables depending on dates
    private boolean multiTableModel = true;
    // length of the due_slot in milliseconds
    private int slotMSecondsTimeArea = 1000;
    // how many days one table carries (if value is <1 or >30 this means one
    // month)
    private int dataTableDaysTimeArea = 1;
    // the date from which due_slots are calculated (01.01.2000)
    private Date slotOrigDate = new Date(100, 0, 1);

    // configurable configuring data - for table structures

    // TTL for DST_SLOT_TABLE and SLOT_MESSAGES_TABLE tables (0 - no TTL)
    private int ttlCurrent = 0;
    // TTL for MESSAGES table (0 - no TTL)
    private int ttlArchive = 0;
    // timeout of finishing of writing on new income messages (in
    // dueSlotWritingArray) (5 sec)
    private int millisecDueSlotWritingTimeout = 5000;
    // due_slot count for forward storing after current processing due_slot
    private int dueSlotForwardStoring;
    // due_slot count for which previous loaded records were revised
    private int dueSlotReviseOnSmscStart;
    // Timeout of life cycle of SmsSet in SmsSetCashe.ProcessingSmsSet in
    // seconds
    private int processingSmsSetTimeout;

    // data for processing

    private long currentDueSlot = 0;
    private long messageId = 0;
    private UUID currentSessionUUID;

    private int msgIdSuffix = 0; // default is 0

    private FastMap<String, PreparedStatementCollection_C3> dataTableRead = new FastMap<String, PreparedStatementCollection_C3>();
    private FastMap<Long, DueSlotWritingElement> dueSlotWritingArray = new FastMap<Long, DueSlotWritingElement>();

    // prepared general statements
    private PreparedStatement selectCurrentSlotTable;
    private PreparedStatement updateCurrentSlotTable;
    private PreparedStatement getSmppSmsRoutingRule;
    private PreparedStatement getSipSmsRoutingRule;

    private PreparedStatement updateSmppSmsRoutingRule;
    private PreparedStatement updateSipSmsRoutingRule;

    private PreparedStatement deleteSmppSmsRoutingRule;
    private PreparedStatement deleteSipSmsRoutingRule;

    private PreparedStatement getSmppSmsRoutingRulesRange;
    private PreparedStatement getSmppSmsRoutingRulesRange2;

    private PreparedStatement getSipSmsRoutingRulesRange;
    private PreparedStatement getSipSmsRoutingRulesRange2;

    private PreparedStatement getTableList;

    private Date pcsDate;
    private PreparedStatementCollection_C3[] savedPsc;

    private volatile boolean started = false;
    private boolean databaseAvailable = false;

    protected DBOperations_C2() {
        super();
    }

    public static DBOperations_C2 getInstance() {
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

    public void start(String hosts, int port, String keyspace, int secondsForwardStoring, int reviseSecondsOnSmscStart,
                      int processingSmsSetTimeout, int smscMsgIdPrefix) throws Exception {
        if (this.started) {
            throw new Exception("DBOperations already started");
        }


        this.msgIdSuffix = smscMsgIdPrefix;

        if (secondsForwardStoring < 3)
            secondsForwardStoring = 3;
        this.dueSlotForwardStoring = secondsForwardStoring * 1000 / slotMSecondsTimeArea;
        this.dueSlotReviseOnSmscStart = reviseSecondsOnSmscStart * 1000 / slotMSecondsTimeArea;
        this.processingSmsSetTimeout = processingSmsSetTimeout;

        this.pcsDate = null;
        currentSessionUUID = UUID.randomUUID();

        Builder builder = Cluster.builder();

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
                            "Failure of connting to cassandra database. : host=%s, port=%d. SMSC GW will work without database support\n",
                            hosts, port), e);
            this.started = true;
            return;
        }
        databaseAvailable = true;

        Metadata metadata = cluster.getMetadata();
        logger.info(String.format("Connected to cluster: %s\n", metadata.getClusterName()));
        for (Host host : metadata.getAllHosts()) {
            logger.info(String.format("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(),
                    host.getRack()));
        }
        session = cluster.connect();

        session.execute("USE \"" + keyspace + "\"");

        this.checkCurrentSlotTableExists();

        String sa = "SELECT \"" + Schema.COLUMN_NEXT_SLOT + "\" FROM \"" + Schema.FAMILY_CURRENT_SLOT_TABLE
                + "\" where \"" + Schema.COLUMN_ID + "\"=?;";
        selectCurrentSlotTable = session.prepare(sa);
        sa = "INSERT INTO \"" + Schema.FAMILY_CURRENT_SLOT_TABLE + "\" (\"" + Schema.COLUMN_ID + "\", \""
                + Schema.COLUMN_NEXT_SLOT + "\") VALUES (?, ?);";
        updateCurrentSlotTable = session.prepare(sa);

        getSmppSmsRoutingRule = session.prepare("select * from \"" + Schema.FAMILY_SMPP_SMS_ROUTING_RULE + "\" where \"" + Schema.COLUMN_ADDRESS
                + "\"=? and \"" + Schema.COLUMN_NETWORK_ID + "\"=?;");
        getSipSmsRoutingRule = session.prepare("select * from \"" + Schema.FAMILY_SIP_SMS_ROUTING_RULE + "\" where \"" + Schema.COLUMN_ADDRESS + "\"=? and \""
                + Schema.COLUMN_NETWORK_ID + "\"=?;");

        updateSmppSmsRoutingRule = session.prepare("INSERT INTO \"" + Schema.FAMILY_SMPP_SMS_ROUTING_RULE + "\" (\"" + Schema.COLUMN_ADDRESS + "\", \""
                + Schema.COLUMN_NETWORK_ID + "\", \"" + Schema.COLUMN_CLUSTER_NAME + "\") VALUES (?, ?, ?);");
        updateSipSmsRoutingRule = session.prepare("INSERT INTO \"" + Schema.FAMILY_SIP_SMS_ROUTING_RULE + "\" (\"" + Schema.COLUMN_ADDRESS + "\", \""
                + Schema.COLUMN_NETWORK_ID + "\", \"" + Schema.COLUMN_CLUSTER_NAME + "\") VALUES (?, ?, ?);");

        deleteSmppSmsRoutingRule = session.prepare("delete from \"" + Schema.FAMILY_SMPP_SMS_ROUTING_RULE + "\" where \"" + Schema.COLUMN_ADDRESS
                + "\"=? and \"" + Schema.COLUMN_NETWORK_ID + "\"=?;");
        deleteSipSmsRoutingRule = session.prepare("delete from \"" + Schema.FAMILY_SIP_SMS_ROUTING_RULE + "\" where \"" + Schema.COLUMN_ADDRESS + "\"=? and \""
                + Schema.COLUMN_NETWORK_ID + "\"=?;");

        int row_count = 100;

        getSmppSmsRoutingRulesRange = session.prepare("select * from \"" + Schema.FAMILY_SMPP_SMS_ROUTING_RULE
                + "\" where token(\"" + Schema.COLUMN_ADDRESS + "\") >= token(?) LIMIT " + row_count + ";");
        getSmppSmsRoutingRulesRange2 = session.prepare("select * from \"" + Schema.FAMILY_SMPP_SMS_ROUTING_RULE
                + "\"  LIMIT " + row_count + ";");

        getSipSmsRoutingRulesRange = session.prepare("select * from \"" + Schema.FAMILY_SIP_SMS_ROUTING_RULE
                + "\" where token(\"" + Schema.COLUMN_ADDRESS + "\") >= token(?) LIMIT " + row_count + ";");
        getSipSmsRoutingRulesRange2 = session.prepare("select * from \"" + Schema.FAMILY_SIP_SMS_ROUTING_RULE
                + "\"  LIMIT " + row_count + ";");

        getTableList = session.prepare("select * from system.schema_columnfamilies;");

        try {
            currentDueSlot = c2_getCurrentSlotTable(CURRENT_DUE_SLOT);
            if (currentDueSlot == 0) {
                // not yet set
                long l1 = this.c2_getDueSlotForTime(new Date());
                this.c2_setCurrentDueSlot(l1);
            } else {
                this.c2_setCurrentDueSlot(currentDueSlot - dueSlotReviseOnSmscStart);
            }

            messageId = c2_getCurrentSlotTable(NEXT_MESSAGE_ID);
            messageId += MESSAGE_ID_LAG;
            c2_setCurrentSlotTable(NEXT_MESSAGE_ID, messageId);
        } catch (Exception e1) {
            String msg = "Failed reading a currentDueSlot !";
            throw new PersistenceException(msg, e1);
        }

        this.started = true;
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
    }

    /**
     * Returns length of the due_slot in milliseconds
     *
     */
    public long getSlotMSecondsTimeArea() {
        return slotMSecondsTimeArea;
    }

    /**
     * Returns due_slot for the given time
     */
    public long c2_getDueSlotForTime(Date time) {
        long a2 = time.getTime();
        long a1 = this.slotOrigDate.getTime();
        long diff = a2 - a1;
        long res = diff / this.slotMSecondsTimeArea;
        return res;
    }

    /**
     * Returns time for the given due_slot
     */
    public Date c2_getTimeForDueSlot(long dueSlot) {
        long a1 = this.slotOrigDate.getTime() + dueSlot * this.slotMSecondsTimeArea;
        Date date = new Date(a1);
        return date;
    }

    /**
     * Returns due_slop that SMSC is processing now
     */
    public long c2_getCurrentDueSlot() {
        return currentDueSlot;
    }

    /**
     * Set a new due_slop that SMSC "is processing now" and store it into the
     * database
     */
    public void c2_setCurrentDueSlot(long newDueSlot) throws PersistenceException {
        currentDueSlot = newDueSlot;

        c2_setCurrentSlotTable(CURRENT_DUE_SLOT, newDueSlot);
    }

    /**
     * Returns a next messageId
     * Every MESSAGE_ID_LAG messageId will be stored at cassandra database
     */
    public synchronized long c2_getNextMessageId() {
        messageId++;

        // Tili
        // add MsgId prefix
//		messageId = (messageId * 100) + this.msgIdSuffix;
        if (messageId >= MAX_MESSAGE_ID) {
            messageId = 1;
        }
        if (messageId %  MESSAGE_ID_LAG == 0 && databaseAvailable) {
            try {
                c2_setCurrentSlotTable(NEXT_MESSAGE_ID, messageId);
            } catch (PersistenceException e) {
                logger.error("Exception when storing next messageId to the database: " + e.getMessage(), e);
            }
        }

        return (messageId * 100) + this.msgIdSuffix;
    }

    /**
     * Initial reading CURRENT_DUE_SLOT and NEXT_MESSAGE_ID when cassandra
     * database access starting
     *
     * @param CURRENT_DUE_SLOT
     *            or NEXT_MESSAGE_ID
     * @return dead value
     * @throws PersistenceException
     */
    protected long c2_getCurrentSlotTable(int key) throws PersistenceException {
        PreparedStatement ps = selectCurrentSlotTable;
        BoundStatement boundStatement = new BoundStatement(ps);
        boundStatement.bind(key);
        ResultSet res = session.execute(boundStatement);

        long res2 = 0;
        for (Row row : res) {
            res2 = row.getLong(0);
            break;
        }
        return res2;
    }

    private void c2_setCurrentSlotTable(int key, long newVal) throws PersistenceException {
        try {
            PreparedStatement ps = updateCurrentSlotTable;
            BoundStatement boundStatement = new BoundStatement(ps);
            boundStatement.bind(key, newVal);
            ResultSet res = session.execute(boundStatement);
        } catch (Exception e1) {
            String msg = "Failed writing a c2_setCurrenrSlotTable !";
            throw new PersistenceException(msg, e1);
        }
    }

    /**
     * Return due_slop for current time
     */
    public long c2_getIntimeDueSlot() {
        return this.c2_getDueSlotForTime(new Date());
    }

    /**
     * Return due_slop for storing next incoming to SMSC message
     */
    public long c2_getDueSlotForNewSms() {
        long res = c2_getIntimeDueSlot() + dueSlotForwardStoring;
        return res;

        // TODO: we can add here code incrementing of due_slot if current
        // due_slot is overloaded
    }

    /**
     * Checking if dueSlot is possible to use for writing (it is not too soon).
     * if possible - returns dueSlot
     * if not - returns next possible dueSlot
     */
    public long c2_checkDueSlotWritingPossibility(long dueSlot) {
        long first = this.c2_getCurrentDueSlot() + DUE_SLOT_WRITING_POSSIBILITY_DELAY;
        if (dueSlot < first)
            return first;
        else
            return dueSlot;
    }

    /**
     * Registering that thread starts writing to this due_slot
     */
    public void c2_registerDueSlotWriting(long dueSlot) {
        synchronized (dueSlotWritingArray) {
            Long ll = dueSlot;
            DueSlotWritingElement el = dueSlotWritingArray.get(ll);
            if (el == null) {
                el = new DueSlotWritingElement(ll);
                dueSlotWritingArray.put(ll, el);
                el.writingCount = 1;
            } else {
                el.writingCount++;
            }
            el.lastStartDate = new Date();
        }
    }

    /**
     * Registering that thread finishes writing to this due_slot
     */
    public void c2_unregisterDueSlotWriting(long dueSlot) {
        synchronized (dueSlotWritingArray) {
            Long ll = dueSlot;
            DueSlotWritingElement el = dueSlotWritingArray.get(ll);
            if (el != null) {
                el.writingCount--;
                if (el.writingCount == 0) {
                    dueSlotWritingArray.remove(ll);
                }
            }
        }
    }

    /**
     * Checking if due_slot is not in writing state now Returns true if due_slot
     * is not in writing now
     */
    public boolean c2_checkDueSlotNotWriting(long dueSlot) {
        synchronized (dueSlotWritingArray) {
            Long ll = dueSlot;
            DueSlotWritingElement el = dueSlotWritingArray.get(ll);
            if (el != null) {
                Date d1 = el.lastStartDate;
                Date d2 = new Date();
                long diff = d2.getTime() - d1.getTime();
                if (diff > millisecDueSlotWritingTimeout) {
                    logger.warn("Timeout in millisecDueSlotWritingTimeout element");
                    dueSlotWritingArray.remove(ll);
                    return true;
                } else
                    return false;
            } else {
                return true;
            }
        }
    }

    /**
     * Generate a table name depending on long dueSlot
     */
    protected String getTableName(long dueSlot) {
        Date dt = this.c2_getTimeForDueSlot(dueSlot);
        return getTableName(dt);
    }

    /**
     * Generate a table name depending on date
     */
    protected String getTableName(Date dt) {
        if (multiTableModel) {
            StringBuilder sb = new StringBuilder();
            sb.append("_");
            sb.append(dt.getYear() + 1900);
            sb.append("_");
            int mn = dt.getMonth() + 1;
            if (mn >= 10)
                sb.append(mn);
            else {
                sb.append("0");
                sb.append(mn);
            }
            if (this.dataTableDaysTimeArea < 1 || this.dataTableDaysTimeArea >= 30) {
            } else {
                int dy = dt.getDate();
                int fNum;
                if (this.dataTableDaysTimeArea == 1)
                    fNum = dy / this.dataTableDaysTimeArea;
                else
                    fNum = dy / this.dataTableDaysTimeArea + 1;
                sb.append("_");
                if (fNum >= 10)
                    sb.append(fNum);
                else {
                    sb.append("0");
                    sb.append(fNum);
                }
            }
            return sb.toString();
        } else
            return "";
    }

    public PreparedStatementCollection_C3[] c2_getPscList() throws PersistenceException {
        Date dt = new Date();
        if (!this.isStarted())
            return new PreparedStatementCollection_C3[0];
        if (pcsDate != null && dt.getDate() == pcsDate.getDate()) {
            return savedPsc;
        } else {
            createPscList();
            return savedPsc;
        }
    }

    private void createPscList() throws PersistenceException {
        Date dt = new Date();
        Date dtt = new Date(dt.getTime() + 1000 * 60 * 60 * 24);
        String s1 = this.getTableName(dt);
        String s2 = this.getTableName(dtt);
        PreparedStatementCollection_C3[] res;
        if (!s2.equals(s1)) {
            res = new PreparedStatementCollection_C3[2];
            res[0] = this.getStatementCollection(dtt);
            res[1] = this.getStatementCollection(dt);
        } else {
            res = new PreparedStatementCollection_C3[1];
            res[0] = this.getStatementCollection(dt);
        }
        savedPsc = res;

        pcsDate = dt;
    }

    public long c2_getDueSlotForTargetId(String targetId) throws PersistenceException {
        PreparedStatementCollection_C3[] lstPsc = this.c2_getPscList();

        for (PreparedStatementCollection_C3 psc : lstPsc) {
            long dueSlot = this.c2_getDueSlotForTargetId(psc, targetId);
            if (dueSlot != 0)
                return dueSlot;
        }
        return 0;
    }

    public long c2_getDueSlotForTargetId(PreparedStatementCollection_C3 psc, String targetId)
            throws PersistenceException {
        try {
            PreparedStatement ps = psc.getDueSlotForTargetId;
            BoundStatement boundStatement = new BoundStatement(ps);
            boundStatement.bind(targetId);
            ResultSet res = session.execute(boundStatement);

            long l = 0;
            for (Row row : res) {
                l = row.getLong(0);
                break;
            }
            return l;
        } catch (Exception e1) {
            // TODO: remove it
            e1.printStackTrace();

            String msg = "Failed to execute getDueSlotForTargetId() !";
            throw new PersistenceException(msg, e1);
        }
    }

    public void c2_updateDueSlotForTargetId(String targetId, long newDueSlot) throws PersistenceException {
        PreparedStatementCollection_C3 psc = this.getStatementCollection(newDueSlot);

        try {
            PreparedStatement ps = psc.createDueSlotForTargetId;
            BoundStatement boundStatement = new BoundStatement(ps);
            boundStatement.bind(targetId, newDueSlot);
            ResultSet res = session.execute(boundStatement);
        } catch (Exception e1) {
            String msg = "Failed to execute createDueSlotForTargetId() !";
            throw new PersistenceException(msg, e1);
        }
    }

    private void c2_clearDueSlotForTargetId(String targetId, long newDueSlot) throws PersistenceException {
        PreparedStatementCollection_C3 psc = this.getStatementCollection(newDueSlot);

        try {
            PreparedStatement ps = psc.createDueSlotForTargetId;
            BoundStatement boundStatement = new BoundStatement(ps);
            boundStatement.bind(targetId, 0L);
            ResultSet res = session.execute(boundStatement);
        } catch (Exception e1) {
            String msg = "Failed to execute clearDueSlotForTargetId() !";
            throw new PersistenceException(msg, e1);
        }
    }

    public void c2_updateDueSlotForTargetId_WithTableCleaning(String targetId, long newDueSlot)
            throws PersistenceException {
        // removing dueSlot for other time tables is any
        PreparedStatementCollection_C3[] lstPsc = this.c2_getPscList();
        if (lstPsc.length >= 2) {
            String s1 = this.getTableName(newDueSlot);
            for (int i1 = 0; i1 < lstPsc.length; i1++) {
                PreparedStatementCollection_C3 psc = lstPsc[i1];
                if (!psc.getTName().equals(s1)) {
                    long dueSlot = this.c2_getDueSlotForTargetId(psc, targetId);
                    if (dueSlot != 0) {
                        c2_clearDueSlotForTargetId(targetId, dueSlot);
                    }
                }
            }
        }

        c2_updateDueSlotForTargetId(targetId, newDueSlot);
    }

    /**
     * scheduler a message into a dueSlot that is already scheduled for all
     * messages for this targetId or for a next available if no dueSlot has been
     * scheduled for targetId
     */
    public void c2_scheduleMessage_ReschedDueSlot(Sms sms, boolean fastStoreAndForwordMode, boolean removeExpiredValidityPeriod) throws PersistenceException {
        c2_scheduleMessage_ReschedDueSlot(sms, fastStoreAndForwordMode, 0, null, removeExpiredValidityPeriod);
    }

    private void c2_scheduleMessage_ReschedDueSlot(Sms sms, boolean fastStoreAndForwordMode, long preferredDueSlot, ArrayList<Sms> lstFailured,
                                                   boolean removeExpiredValidityPeriod) throws PersistenceException {
        if (sms.getStored()) {
            long dueSlot = 0;
            PreparedStatementCollection_C3[] lstPsc = this.c2_getPscList();
            boolean done = false;
            int cnt = 0;
            while (!done && cnt < 5) {
                cnt++;

                SmType destType = sms.getSmsSet().getType();
                if (destType == null || destType == SmType.SMS_FOR_SS7) {
                    for (PreparedStatementCollection_C3 psc : lstPsc) {
                        dueSlot = this.c2_getDueSlotForTargetId(psc, sms.getSmsSet().getTargetId());
                        if (dueSlot != 0)
                            break;
                    }

                    if ((dueSlot == 0 || dueSlot <= this.c2_getCurrentDueSlot()) && preferredDueSlot > 0) {
                        dueSlot = preferredDueSlot;
                        this.c2_updateDueSlotForTargetId_WithTableCleaning(sms.getSmsSet().getTargetId(), dueSlot);
                    }

                    if (fastStoreAndForwordMode && dueSlot != 0) {
                        long dueSlot2 = c2_checkDueSlotWritingPossibility(dueSlot);
                        if (dueSlot2 != dueSlot) {
                            dueSlot = dueSlot2;
                            this.c2_updateDueSlotForTargetId_WithTableCleaning(sms.getSmsSet().getTargetId(), dueSlot);
                        }
                    }
                }

                if (dueSlot == 0 || dueSlot <= this.c2_getCurrentDueSlot()) {
                    dueSlot = this.c2_getDueSlotForNewSms();
                    this.c2_updateDueSlotForTargetId_WithTableCleaning(sms.getSmsSet().getTargetId(), dueSlot);
                }

                sms.setDueSlot(dueSlot);

                done = this.do_scheduleMessage(sms, dueSlot, lstFailured, fastStoreAndForwordMode, removeExpiredValidityPeriod);
            }

            if (!done) {
                logger.warn("5 retries of c2_scheduleMessage fails for targetId=" + sms.getSmsSet().getTargetId());
            }
        }
    }

    /**
     * scheduler a message for a predefined dueSlot (for a next schedule time)
     */
    public void c2_scheduleMessage_NewDueSlot(Sms sms, long dueSlot, ArrayList<Sms> lstFailured, boolean fastStoreAndForwordMode) throws PersistenceException {
        if (sms.getStoringAfterFailure()) {
            // the first store in ForwardAndStore mode - we store with other
            // messages to this
            sms.setStored(true);
            c2_scheduleMessage_ReschedDueSlot(sms, fastStoreAndForwordMode, dueSlot, lstFailured, true);
        } else {
            if (sms.getStored()) {
                if (fastStoreAndForwordMode) {
                    dueSlot = c2_checkDueSlotWritingPossibility(dueSlot);
                }

                this.c2_updateInSystem(sms, DBOperations_C2.IN_SYSTEM_SENT, fastStoreAndForwordMode);
                this.c2_updateDueSlotForTargetId_WithTableCleaning(sms.getSmsSet().getTargetId(), dueSlot);
                this.do_scheduleMessage(sms, dueSlot, lstFailured, fastStoreAndForwordMode, true);
            } else {
                lstFailured.add(sms);
            }
        }
    }

    protected boolean do_scheduleMessage(Sms sms, long dueSlot, ArrayList<Sms> lstFailured, boolean fastStoreAndForwordMode, boolean removeExpiredValidityPeriod)
            throws PersistenceException {

        sms.setDueSlot(dueSlot);

        Date dt = this.c2_getTimeForDueSlot(dueSlot);

        // special case for ScheduleDeliveryTime
        Date schedTime = sms.getScheduleDeliveryTime();
        if (schedTime != null && schedTime.after(dt)) {
            dueSlot = this.c2_getDueSlotForTime(schedTime);
            sms.setDueSlot(dueSlot);
        }

        // checking validity date
        if (sms.getValidityPeriod() != null && sms.getValidityPeriod().before(dt)) {
            if (sms.getValidityPeriod().after(new Date())){
                // we need dueSlot to be at validityPeriod
                long validDueSlot = this.c2_getDueSlotForTime(sms.getValidityPeriod()) + 10; // 10 seconds after validity so that it expires
/*				long nextDueSlotInSystem = this.c2_getCurrentDueSlot();
				if(validDueSlot < nextDueSlotInSystem ) {
					// our due slot is before next DueSlot of system. We need to update system due slot to be ours atleast or else system will skip our sms try
					this.c2_setCurrentDueSlot(validDueSlot);
				}*/
                sms.setDueSlot(validDueSlot);
                this.c2_updateDueSlotForTargetId_WithTableCleaning(sms.getSmsSet().getTargetId(), validDueSlot);
            } else if (lstFailured != null) {
                lstFailured.add(sms);
                return true;
            }
        }

        if (fastStoreAndForwordMode) {
            return doCreateRecordCurrent(sms, dueSlot);
        } else {
            this.c2_registerDueSlotWriting(dueSlot);
            try {
                return doCreateRecordCurrent(sms, dueSlot);
            } finally {
                this.c2_unregisterDueSlotWriting(dueSlot);
            }
        }
    }

    private boolean doCreateRecordCurrent(Sms sms, long dueSlot) throws PersistenceException {
        if (dueSlot <= this.c2_getCurrentDueSlot()) {
            return false;
        } else {
            this.c2_createRecordCurrent(sms);
            return true;
        }
    }

    public void c2_createRecordCurrent(Sms sms) throws PersistenceException {
        long dueSlot = sms.getDueSlot();
        PreparedStatementCollection_C3 psc = getStatementCollection(dueSlot);

        try {
            PreparedStatement ps = psc.createRecordCurrent;
            BoundStatement boundStatement = new BoundStatement(ps);

            setSmsFields(sms, dueSlot, boundStatement, false, psc.getShortMessageNewStringFormat(), psc.getAddedCorrId(), psc.getAddedNetworkId(),
                    psc.getAddedOrigNetworkId());

            ResultSet res = session.execute(boundStatement);
        } catch (Exception e1) {
            String msg = "Failed createRecordCurrent !" + e1.getMessage();

            throw new PersistenceException(msg, e1);
        }
    }

    public void c2_createRecordArchive(Sms sms) throws PersistenceException {
        if (!this.databaseAvailable)
            return;

        Date deliveryDate = sms.getDeliverDate();
        if (deliveryDate == null)
            deliveryDate = new Date();
        long dueSlot = this.c2_getDueSlotForTime(deliveryDate);
        PreparedStatementCollection_C3 psc = getStatementCollection(deliveryDate);

        try {
            PreparedStatement ps = psc.createRecordArchive;
            BoundStatement boundStatement = new BoundStatement(ps);

            setSmsFields(sms, dueSlot, boundStatement, true, psc.getShortMessageNewStringFormat(), psc.getAddedCorrId(), psc.getAddedNetworkId(),
                    psc.getAddedOrigNetworkId());

            ResultSet res = session.execute(boundStatement);
        } catch (Exception e1) {
            String msg = "Failed createRecordArchive !" + e1.getMessage();

            throw new PersistenceException(msg, e1);
        }
    }

    private void setSmsFields(Sms sms, long dueSlot, BoundStatement boundStatement, boolean archive, boolean shortMessageNewStringFormat, boolean addedCorrId,
                              boolean addedNetworkId, boolean addedOrigNetworkId) throws PersistenceException {
        boundStatement.setUUID(Schema.COLUMN_ID, sms.getDbId());
        boundStatement.setString(Schema.COLUMN_TARGET_ID, sms.getSmsSet().getTargetId());
        if (addedNetworkId) {
            boundStatement.setInt(Schema.COLUMN_NETWORK_ID, sms.getSmsSet().getNetworkId());
        }
        boundStatement.setLong(Schema.COLUMN_DUE_SLOT, dueSlot);
        boundStatement.setInt(Schema.COLUMN_IN_SYSTEM, IN_SYSTEM_UNSENT);
        boundStatement.setUUID(Schema.COLUMN_SMSC_UUID, emptyUuid);

        boundStatement.setString(Schema.COLUMN_ADDR_DST_DIGITS, sms.getSmsSet().getDestAddr());
        boundStatement.setInt(Schema.COLUMN_ADDR_DST_TON, sms.getSmsSet().getDestAddrTon());
        boundStatement.setInt(Schema.COLUMN_ADDR_DST_NPI, sms.getSmsSet().getDestAddrNpi());


        if (sms.getSourceAddr() != null) {
            boundStatement.setString(Schema.COLUMN_ADDR_SRC_DIGITS, sms.getSourceAddr());
        } else {
            boundStatement.setToNull(Schema.COLUMN_ADDR_SRC_DIGITS);
        }
        boundStatement.setInt(Schema.COLUMN_ADDR_SRC_TON, sms.getSourceAddrTon());
        boundStatement.setInt(Schema.COLUMN_ADDR_SRC_NPI, sms.getSourceAddrNpi());
        if (addedOrigNetworkId) {
            boundStatement.setInt(Schema.COLUMN_ORIG_NETWORK_ID, sms.getOrigNetworkId());
        }

        boundStatement.setInt(Schema.COLUMN_DUE_DELAY, sms.getSmsSet().getDueDelay());
        if (sms.getSmsSet().getStatus() != null)
            boundStatement.setInt(Schema.COLUMN_SM_STATUS, sms.getSmsSet().getStatus().getCode());
        else
            boundStatement.setToNull(Schema.COLUMN_SM_STATUS);
        boundStatement.setBool(Schema.COLUMN_ALERTING_SUPPORTED, sms.getSmsSet().isAlertingSupported());

        boundStatement.setLong(Schema.COLUMN_MESSAGE_ID, sms.getMessageId());
        boundStatement.setInt(Schema.COLUMN_MO_MESSAGE_REF, sms.getMoMessageRef());
        if (sms.getOrigEsmeName() != null) {
            boundStatement.setString(Schema.COLUMN_ORIG_ESME_NAME, sms.getOrigEsmeName());
        } else
            boundStatement.setToNull(Schema.COLUMN_ORIG_ESME_NAME);
        if (sms.getOrigSystemId() != null) {
            boundStatement.setString(Schema.COLUMN_ORIG_SYSTEM_ID, sms.getOrigSystemId());
        } else
            boundStatement.setToNull(Schema.COLUMN_ORIG_SYSTEM_ID);
        if (sms.getSubmitDate() != null) {
            boundStatement.setDate(Schema.COLUMN_SUBMIT_DATE, sms.getSubmitDate());
        } else
            boundStatement.setToNull(Schema.COLUMN_SUBMIT_DATE);
        if (sms.getDeliverDate() != null) {
            boundStatement.setDate(Schema.COLUMN_DELIVERY_DATE, sms.getDeliverDate());
        } else
            boundStatement.setToNull(Schema.COLUMN_DELIVERY_DATE);
        if (sms.getServiceType() != null) {
            boundStatement.setString(Schema.COLUMN_SERVICE_TYPE, sms.getServiceType());
        } else
            boundStatement.setToNull(Schema.COLUMN_SERVICE_TYPE);
        boundStatement.setInt(Schema.COLUMN_ESM_CLASS, sms.getEsmClass());
        boundStatement.setInt(Schema.COLUMN_PROTOCOL_ID, sms.getProtocolId());
        boundStatement.setInt(Schema.COLUMN_PRIORITY, sms.getPriority());

        boundStatement.setInt(Schema.COLUMN_REGISTERED_DELIVERY, sms.getRegisteredDelivery());
        boundStatement.setInt(Schema.COLUMN_REPLACE, sms.getReplaceIfPresent());
        boundStatement.setInt(Schema.COLUMN_DATA_CODING, sms.getDataCodingForDatabase());
        boundStatement.setInt(Schema.COLUMN_DEFAULT_MSG_ID, sms.getDefaultMsgId());

        if (shortMessageNewStringFormat) {
            if (sms.getShortMessageText() != null) {
                boundStatement.setString(Schema.COLUMN_MESSAGE_TEXT, sms.getShortMessageText());
            } else
                boundStatement.setToNull(Schema.COLUMN_MESSAGE_TEXT);
            if (sms.getShortMessageBin() != null) {
                boundStatement.setBytes(Schema.COLUMN_MESSAGE_BIN, ByteBuffer.wrap(sms.getShortMessageBin()));
            } else
                boundStatement.setToNull(Schema.COLUMN_MESSAGE_BIN);
            boundStatement.setToNull(Schema.COLUMN_MESSAGE);
        } else {
            // convert to an old format
            String msg = sms.getShortMessageText();
            byte[] udhData = sms.getShortMessageBin();
            byte[] textPart = null;
            DataCodingScheme dcs = new DataCodingSchemeImpl(sms.getDataCoding());
            switch (dcs.getCharacterSet()) {
                case GSM7:
                    textPart = msg.getBytes();
                    break;
                case UCS2:
                    Charset ucs2Charset = Charset.forName("UTF-16BE");
                    ByteBuffer bb = ucs2Charset.encode(msg);
                    textPart = new byte[bb.limit()];
                    bb.get(textPart);
                    break;
                default:
                    // we do not support this yet
                    break;
            }

            byte[] data;
            if (textPart != null) {
                if (udhData != null) {
                    data = new byte[textPart.length + udhData.length];
                    System.arraycopy(udhData, 0, data, 0, udhData.length);
                    System.arraycopy(textPart, 0, data, udhData.length, textPart.length);
                } else {
                    data = textPart;
                }
            } else {
                if (udhData != null) {
                    data = udhData;
                } else {
                    data = new byte[0];
                }
            }
            boundStatement.setBytes(Schema.COLUMN_MESSAGE, ByteBuffer.wrap(data));
        }

        if (sms.getScheduleDeliveryTime() != null) {
            boundStatement.setDate(Schema.COLUMN_SCHEDULE_DELIVERY_TIME, sms.getScheduleDeliveryTime());
        } else
            boundStatement.setToNull(Schema.COLUMN_SCHEDULE_DELIVERY_TIME);
        if (sms.getValidityPeriod() != null) {
            boundStatement.setDate(Schema.COLUMN_VALIDITY_PERIOD, sms.getValidityPeriod());
        } else
            boundStatement.setToNull(Schema.COLUMN_VALIDITY_PERIOD);

        boundStatement.setInt(Schema.COLUMN_DELIVERY_COUNT, sms.getDeliveryCount());

        if (sms.getTlvSet().getOptionalParameterCount() > 0) {
            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                XMLObjectWriter writer = XMLObjectWriter.newInstance(baos);
                writer.setIndentation("\t");
                writer.write(sms.getTlvSet(), TLV_SET, TlvSet.class);
                writer.close();
                byte[] rawData = baos.toByteArray();
                String serializedEvent = new String(rawData);

                boundStatement.setString(Schema.COLUMN_OPTIONAL_PARAMETERS, serializedEvent);
            } catch (XMLStreamException e) {
                String msg = "XMLStreamException when serializing optional parameters for '" + sms.getDbId() + "'!";

                throw new PersistenceException(msg, e);
            }
        } else {
            boundStatement.setToNull(Schema.COLUMN_OPTIONAL_PARAMETERS);
        }

        if (!archive) {
            if (addedCorrId) {
                boundStatement.setToNull(Schema.COLUMN_CORR_ID);
            }
            boundStatement.setToNull(Schema.COLUMN_IMSI);
            if (sms.getSmsSet().getCorrelationId() != null) {
                if (addedCorrId) {
                    boundStatement.setString(Schema.COLUMN_CORR_ID, sms.getSmsSet().getCorrelationId());
                } else {
                    boundStatement.setString(Schema.COLUMN_IMSI, sms.getSmsSet().getCorrelationId());
                }
            }
        } else {
            if (sms.getSmsSet().getLocationInfoWithLMSI() != null
                    && sms.getSmsSet().getLocationInfoWithLMSI().getNetworkNodeNumber() != null) {
                boundStatement.setString(Schema.COLUMN_NNN_DIGITS, sms.getSmsSet().getLocationInfoWithLMSI()
                        .getNetworkNodeNumber().getAddress());
                boundStatement.setInt(Schema.COLUMN_NNN_AN, sms.getSmsSet().getLocationInfoWithLMSI().getNetworkNodeNumber()
                        .getAddressNature().getIndicator());
                boundStatement.setInt(Schema.COLUMN_NNN_NP, sms.getSmsSet().getLocationInfoWithLMSI().getNetworkNodeNumber()
                        .getNumberingPlan().getIndicator());
            } else {
                boundStatement.setToNull(Schema.COLUMN_NNN_DIGITS);
                boundStatement.setToNull(Schema.COLUMN_NNN_AN);
                boundStatement.setToNull(Schema.COLUMN_NNN_NP);
            }
            if (sms.getSmsSet().getType() != null) {
                boundStatement.setInt(Schema.COLUMN_SM_TYPE, sms.getSmsSet().getType().getCode());
            } else {
                boundStatement.setToNull(Schema.COLUMN_SM_TYPE);
            }
            if (addedCorrId) {
                boundStatement.setString(Schema.COLUMN_CORR_ID, sms.getSmsSet().getCorrelationId());
            }
            boundStatement.setString(Schema.COLUMN_IMSI, sms.getSmsSet().getImsi());
        }
    }

    public ArrayList<SmsSet> c2_getRecordList(long dueSlot) throws PersistenceException {
        PreparedStatementCollection_C3 psc = getStatementCollection(dueSlot);

        ArrayList<SmsSet> result = new ArrayList<SmsSet>();
        try {
            PreparedStatement ps = psc.getRecordData;
            BoundStatement boundStatement = new BoundStatement(ps);
            boundStatement.bind(dueSlot);
            ResultSet res = session.execute(boundStatement);

            for (Row row : res) {
                SmsSet smsSet = this.createSms(row, null, psc.getShortMessageNewStringFormat(), psc.getAddedCorrId(), psc.getAddedNetworkId(),
                        psc.getAddedOrigNetworkId());
                if (smsSet != null)
                    result.add(smsSet);
            }
        } catch (Exception e1) {
            String msg = "Failed getRecordList()";

            throw new PersistenceException(msg, e1);
        }

        return result;
    }

    public SmsSet c2_getRecordListForTargeId(long dueSlot, String targetId) throws PersistenceException {
        PreparedStatementCollection_C3 psc = getStatementCollection(dueSlot);

        SmsSet result = null;
        try {
            PreparedStatement ps = psc.getRecordData2;
            BoundStatement boundStatement = new BoundStatement(ps);
            boundStatement.bind(dueSlot, targetId);
            ResultSet res = session.execute(boundStatement);

            for (Row row : res) {
                result = this.createSms(row, result, psc.getShortMessageNewStringFormat(), psc.getAddedCorrId(), psc.getAddedNetworkId(),
                        psc.getAddedOrigNetworkId());
            }
        } catch (Exception e1) {
            String msg = "Failed getRecordListForTargeId()";

            throw new PersistenceException(msg, e1);
        }

        return result;
    }

    protected SmsSet createSms(final Row row, SmsSet smsSet, boolean shortMessageNewStringFormat, boolean addedCorrId, boolean addedNetworkId,
                               boolean addedOrigNetworkId) throws PersistenceException {
        if (row == null) {
            return smsSet;
        }

        int inSystem = row.getInt(Schema.COLUMN_IN_SYSTEM);
        UUID smscUuid = row.getUUID(Schema.COLUMN_SMSC_UUID);
        if (inSystem == IN_SYSTEM_SENT || inSystem == IN_SYSTEM_INPROCESS && smscUuid.equals(currentSessionUUID)) {
            // inSystem it is in processing or processed - skip this
            return smsSet;
        }

        Sms sms = new Sms();
        sms.setStored(true);
        sms.setDbId(row.getUUID(Schema.COLUMN_ID));
        sms.setDueSlot(row.getLong(Schema.COLUMN_DUE_SLOT));

        sms.setSourceAddr(row.getString(Schema.COLUMN_ADDR_SRC_DIGITS));
        sms.setSourceAddrTon(row.getInt(Schema.COLUMN_ADDR_SRC_TON));
        sms.setSourceAddrNpi(row.getInt(Schema.COLUMN_ADDR_SRC_NPI));
        if (addedOrigNetworkId) {
            sms.setOrigNetworkId(row.getInt(Schema.COLUMN_ORIG_NETWORK_ID));
        }

        sms.setMessageId(row.getLong(Schema.COLUMN_MESSAGE_ID));
        sms.setMoMessageRef(row.getInt(Schema.COLUMN_MO_MESSAGE_REF));
        sms.setOrigEsmeName(row.getString(Schema.COLUMN_ORIG_ESME_NAME));
        sms.setOrigSystemId(row.getString(Schema.COLUMN_ORIG_SYSTEM_ID));
        sms.setSubmitDate(row.getDate(Schema.COLUMN_SUBMIT_DATE));

        sms.setServiceType(row.getString(Schema.COLUMN_SERVICE_TYPE));
        sms.setEsmClass(row.getInt(Schema.COLUMN_ESM_CLASS));
        sms.setProtocolId(row.getInt(Schema.COLUMN_PROTOCOL_ID));
        sms.setPriority(row.getInt(Schema.COLUMN_PRIORITY));
        sms.setRegisteredDelivery(row.getInt(Schema.COLUMN_REGISTERED_DELIVERY));
        sms.setReplaceIfPresent(row.getInt(Schema.COLUMN_REPLACE));
        sms.setDataCodingForDatabase(row.getInt(Schema.COLUMN_DATA_CODING));
        sms.setDefaultMsgId(row.getInt(Schema.COLUMN_DEFAULT_MSG_ID));

        if (shortMessageNewStringFormat) {
            sms.setShortMessageText(row.getString(Schema.COLUMN_MESSAGE_TEXT));
            ByteBuffer bb = row.getBytes(Schema.COLUMN_MESSAGE_BIN);
            if (bb != null) {
                byte[] buf = new byte[bb.limit() - bb.position()];
                bb.get(buf);
                sms.setShortMessageBin(buf);
            }
        } else {
            ByteBuffer bb = row.getBytes(Schema.COLUMN_MESSAGE);
            if (bb != null) {
                byte[] buf = new byte[bb.limit() - bb.position()];
                bb.get(buf);
                sms.setShortMessage(buf);

                // convert to a new format
                byte[] shortMessage = sms.getShortMessage();
                byte[] textPart = shortMessage;
                byte[] udhData = null;
                boolean udhExists = false;
                DataCodingScheme dcs = new DataCodingSchemeImpl(sms.getDataCoding());
                String msg = null;
                if (dcs.getCharacterSet() == CharacterSet.GSM8) {
                    udhData = shortMessage;
                } else {
                    if ((sms.getEsmClass() & SmppConstants.ESM_CLASS_UDHI_MASK) != 0) {
                        udhExists = true;
                    }
                    if (udhExists && shortMessage != null && shortMessage.length > 2) {
                        // UDH exists
                        int udhLen = (shortMessage[0] & 0xFF) + 1;
                        if (udhLen <= shortMessage.length) {
                            textPart = new byte[shortMessage.length - udhLen];
                            udhData = new byte[udhLen];
                            System.arraycopy(shortMessage, udhLen, textPart, 0, textPart.length);
                            System.arraycopy(shortMessage, 0, udhData, 0, udhLen);
                        }
                    }

                    switch (dcs.getCharacterSet()) {
                        case GSM7:
                            msg = new String(textPart);
                            break;
                        case UCS2:
                            Charset ucs2Charset = Charset.forName("UTF-16BE");
                            bb = ByteBuffer.wrap(textPart);
                            CharBuffer bf = ucs2Charset.decode(bb);
                            msg = bf.toString();
                            break;
                        default:
                            udhData = sms.getShortMessage();
                            break;
                    }
                }

                sms.setShortMessageText(msg);
                sms.setShortMessageBin(udhData);
            }
        }

        sms.setScheduleDeliveryTime(row.getDate(Schema.COLUMN_SCHEDULE_DELIVERY_TIME));
        sms.setValidityPeriod(row.getDate(Schema.COLUMN_VALIDITY_PERIOD));
        sms.setDeliveryCount(row.getInt(Schema.COLUMN_DELIVERY_COUNT));

        String s = row.getString(Schema.COLUMN_OPTIONAL_PARAMETERS);
        if (s != null) {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(s.getBytes());
                XMLObjectReader reader = XMLObjectReader.newInstance(bais);
                TlvSet copy = reader.read(TLV_SET, TlvSet.class);
                sms.getTlvSet().clearAllOptionalParameter();
                sms.getTlvSet().addAllOptionalParameter(copy.getOptionalParameters());
            } catch (XMLStreamException e) {
                String msg = "XMLStreamException when deserializing optional parameters for '" + sms.getDbId() + "'!";

                throw new PersistenceException(msg, e);
            }
        }

        if (smsSet == null) {
            smsSet = new SmsSet();

            String destAddr = null;
            int destAddrTon = -1;
            int destAddrNpi = -1;

            destAddr = row.getString(Schema.COLUMN_ADDR_DST_DIGITS);
            destAddrTon = row.getInt(Schema.COLUMN_ADDR_DST_TON);
            destAddrNpi = row.getInt(Schema.COLUMN_ADDR_DST_NPI);

            if (destAddr == null || destAddrTon == -1 || destAddrNpi == -1) {
                throw new PersistenceException("destAddr or destAddrTon or destAddrNpi is absent for ID='"
                        + sms.getDbId() + "'");
            }
            smsSet.setDestAddr(destAddr);
            smsSet.setDestAddrTon(destAddrTon);
            smsSet.setDestAddrNpi(destAddrNpi);

            if (addedNetworkId) {
                smsSet.setNetworkId(row.getInt(Schema.COLUMN_NETWORK_ID));
            } else {
                String tagId = row.getString(Schema.COLUMN_TARGET_ID);
                if (tagId != null) {
                    String[] ss = tagId.split("_");
                    if (ss.length == 4) {
                        String s1 = ss[3];
                        try {
                            int networkId = Integer.parseInt(s1);
                            smsSet.setNetworkId(networkId);
                        } catch (Exception e) {
                        }
                    }
                }
            }

            if (addedCorrId)
                smsSet.setCorrelationId(row.getString(Schema.COLUMN_CORR_ID));
            else
                smsSet.setCorrelationId(row.getString(Schema.COLUMN_IMSI));
        }
        int dueDelay = row.getInt(Schema.COLUMN_DUE_DELAY);
        if (dueDelay > smsSet.getDueDelay())
            smsSet.setDueDelay(dueDelay);

        boolean alertingSupported = row.getBool(Schema.COLUMN_ALERTING_SUPPORTED);
        if (alertingSupported)
            smsSet.setAlertingSupported(true);

        smsSet.addSms(sms);

        return smsSet;
    }

    public ArrayList<SmsSet> c2_sortRecordList(ArrayList<SmsSet> sourceLst) {
        FastMap<String, SmsSet> res = new FastMap<String, SmsSet>();

        // aggregating messages for one targetId
        for (SmsSet smsSet : sourceLst) {
            SmsSet smsSet2 = null;
            try {
                smsSet2 = res.get(smsSet.getTargetId());
            } catch (Throwable e) {
                int dd = 0;
            }
            if (smsSet2 != null) {
                smsSet2.addSms(smsSet.getSms(0));
                if (smsSet2.getCorrelationId() == null) {
                    // filling correcationId if not all SmsSet are filled
                    smsSet2.setCorrelationId(smsSet.getCorrelationId());
                }
            } else {
                res.put(smsSet.getTargetId(), smsSet);
            }
        }

        // adding into SmsSetCashe
        ArrayList<SmsSet> res2 = new ArrayList<SmsSet>();
        // 60 min timeout
        Date timeOutDate = new Date(new Date().getTime() - 1000 * 60 * 30);
        for (SmsSet smsSet : res.values()) {
            smsSet.resortSms();

            TargetAddress lock = SmsSetCache.getInstance().addSmsSet(new TargetAddress(smsSet));
            try {
                SmsSet smsSet2;
                synchronized (lock) {
                    smsSet2 = SmsSetCache.getInstance().getProcessingSmsSet(smsSet.getTargetId());
                    if (smsSet2 != null) {
                        if (smsSet2.getLastUpdateTime().after(timeOutDate)) {
                            smsSet2.addSmsSet(smsSet);
//							for (int i1 = 0; i1 < smsSet.getSmsCount(); i1++) {
//                                Sms smsx = smsSet.getSms(i1);
//                                if (!smsSet2.checkSmsPresent(smsx)) {
//                                    smsSet2.addSmsSet(smsx);
//                                }
//							}
                        } else {
                            logger.warn("Timeout of SmsSet in ProcessingSmsSet: targetId=" + smsSet2.getTargetId()
                                    + ", messageCount=" + smsSet2.getSmsCount());
                            smsSet2 = smsSet;
                            SmsSetCache.getInstance().addProcessingSmsSet(smsSet2.getTargetId(), smsSet2,
                                    processingSmsSetTimeout);
                        }
                    } else {
                        smsSet2 = smsSet;
                        SmsSetCache.getInstance().addProcessingSmsSet(smsSet2.getTargetId(), smsSet2,
                                processingSmsSetTimeout);
                    }
                }
                res2.add(smsSet2);
            } finally {
                SmsSetCache.getInstance().removeSmsSet(lock);
            }
        }

        return res2;
    }

    public boolean c2_checkProcessingSmsSet(SmsSet smsSet) {
        Date timeOutDate = new Date(new Date().getTime() - 1000 * 60 * 30);

        TargetAddress lock = SmsSetCache.getInstance().addSmsSet(new TargetAddress(smsSet));
        try {
            synchronized (lock) {
                SmsSet processingSmsSet = SmsSetCache.getInstance().getProcessingSmsSet(smsSet.getTargetId());
                if (processingSmsSet != null) {
                    if (processingSmsSet.getLastUpdateTime().after(timeOutDate)) {
                        processingSmsSet.addSmsSet(smsSet);
//                        for (int i1 = 0; i1 < smsSet.getSmsCount(); i1++) {
//                            Sms smsx = smsSet.getSms(i1);
//                            if (!processingSmsSet.checkSmsPresent(smsx)) {
//                                processingSmsSet.addSmsSet(smsx);
//                            }
//                        }
                        return false;
                    } else {
                        logger.warn("Timeout of SmsSet in ProcessingSmsSet: targetId=" + processingSmsSet.getTargetId() + ", messageCount=" + processingSmsSet.getSmsCount());
                        processingSmsSet = smsSet;
                        SmsSetCache.getInstance().addProcessingSmsSet(processingSmsSet.getTargetId(), processingSmsSet, processingSmsSetTimeout);
                    }
                } else {
                    processingSmsSet = smsSet;
                    SmsSetCache.getInstance().addProcessingSmsSet(processingSmsSet.getTargetId(), processingSmsSet, processingSmsSetTimeout);
                }
            }
        } finally {
            SmsSetCache.getInstance().removeSmsSet(lock);
        }

        return true;
    }

    public void c2_updateInSystem(Sms sms, int isSystemStatus, boolean fastStoreAndForwordMode) throws PersistenceException {
        // if (sms.getStored() && (!fastStoreAndForwordMode ||
        // sms.getInvokedByAlert() || <new method parameter -
        // "updateInSystemForFastMode">)) {

        if (sms.getStored()) {
            PreparedStatementCollection_C3 psc = this.getStatementCollection(sms.getDueSlot());

            try {
                PreparedStatement ps = psc.updateInSystem;
                BoundStatement boundStatement = new BoundStatement(ps);
                boundStatement.bind(isSystemStatus, currentSessionUUID, sms.getDueSlot(), sms.getSmsSet().getTargetId(), sms.getDbId());
                ResultSet res = session.execute(boundStatement);
            } catch (Exception e1) {
                String msg = "Failed to execute updateInSystem() !";
                throw new PersistenceException(msg, e1);
            }
        }
    }

    public void c2_updateAlertingSupport(long dueSlot, String targetId, UUID dbId) throws PersistenceException {
        PreparedStatementCollection_C3 psc = this.getStatementCollection(dueSlot);

        try {
            PreparedStatement ps = psc.updateAlertingSupport;
            BoundStatement boundStatement = new BoundStatement(ps);
            boundStatement.bind(true, dueSlot, targetId, dbId);
            ResultSet res = session.execute(boundStatement);
        } catch (Exception e1) {
            String msg = "Failed to execute c2_updateAlertingSupport() !";
            throw new PersistenceException(msg, e1);
        }
    }

    public void createArchiveMessage(Sms sms) throws PersistenceException {
        // TODO: ......................
        // .....................................
    }

    protected PreparedStatementCollection_C3 getStatementCollection(Date dt) throws PersistenceException {
        String tName = this.getTableName(dt);
        PreparedStatementCollection_C3 psc = dataTableRead.get(tName);
        if (psc != null)
            return psc;

        return doGetStatementCollection(tName);
    }

    protected PreparedStatementCollection_C3 getStatementCollection(long deuSlot) throws PersistenceException {
        String tName = this.getTableName(deuSlot);
        PreparedStatementCollection_C3 psc = dataTableRead.get(tName);
        if (psc != null)
            return psc;

        return doGetStatementCollection(tName);
    }

    private synchronized PreparedStatementCollection_C3 doGetStatementCollection(String tName)
            throws PersistenceException {
        PreparedStatementCollection_C3 psc = dataTableRead.get(tName);
        if (psc != null)
            return psc;

        try {
            try {
                // checking if a datatable exists
                String s1 = "SELECT * FROM \"" + Schema.FAMILY_DST_SLOT_TABLE + tName + "\";";
                PreparedStatement ps = session.prepare(s1);
            } catch (InvalidQueryException e) {
                // datatable does not exist

                // DST_SLOT_TABLE
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE \"" + Schema.FAMILY_DST_SLOT_TABLE);
                sb.append(tName);
                sb.append("\" (");

                appendField(sb, Schema.COLUMN_TARGET_ID, "ascii");
                appendField(sb, Schema.COLUMN_DUE_SLOT, "bigint");

                sb.append("PRIMARY KEY (\"");
                sb.append(Schema.COLUMN_TARGET_ID);
                sb.append("\"");
                sb.append("));");

                String s2 = sb.toString();
                PreparedStatement ps = session.prepare(s2);
                BoundStatement boundStatement = new BoundStatement(ps);
                ResultSet res = session.execute(boundStatement);

                // SLOT_MESSAGES_TABLE
                sb = new StringBuilder();
                sb.append("CREATE TABLE \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE);
                sb.append(tName);
                sb.append("\" (");

                addSmsFields(sb);

                sb.append("PRIMARY KEY ((\"");
                sb.append(Schema.COLUMN_DUE_SLOT);
                sb.append("\"), \"");
                sb.append(Schema.COLUMN_TARGET_ID);
                sb.append("\", \"");
                sb.append(Schema.COLUMN_ID);
                sb.append("\"");
                sb.append("));");

                s2 = sb.toString();
                ps = session.prepare(s2);
                boundStatement = new BoundStatement(ps);
                res = session.execute(boundStatement);

                // MESSAGES
                sb = new StringBuilder();
                sb.append("CREATE TABLE \"" + Schema.FAMILY_MESSAGES);
                sb.append(tName);
                sb.append("\" (");

                addSmsFields(sb);

                sb.append("PRIMARY KEY ((\"");
                sb.append(Schema.COLUMN_ADDR_DST_DIGITS);
                sb.append("\"), \"");
                sb.append(Schema.COLUMN_ID);
                sb.append("\"");
                sb.append("));");

                s2 = sb.toString();
                ps = session.prepare(s2);
                boundStatement = new BoundStatement(ps);
                res = session.execute(boundStatement);
            }
        } catch (Exception e1) {
            String msg = "Failed to access or create table " + tName + "!";
            throw new PersistenceException(msg, e1);
        }

        psc = new PreparedStatementCollection_C3(this, tName, ttlCurrent, ttlArchive);
        dataTableRead.putEntry(tName, psc);
        return psc;
    }

    protected void addSmsFields(StringBuilder sb) {
        appendField(sb, Schema.COLUMN_ID, "uuid");
        appendField(sb, Schema.COLUMN_TARGET_ID, "ascii");
        appendField(sb, Schema.COLUMN_NETWORK_ID, "int");
        appendField(sb, Schema.COLUMN_DUE_SLOT, "bigint");
        appendField(sb, Schema.COLUMN_IN_SYSTEM, "int");
        appendField(sb, Schema.COLUMN_SMSC_UUID, "uuid");

        appendField(sb, Schema.COLUMN_ADDR_DST_DIGITS, "ascii");
        appendField(sb, Schema.COLUMN_ADDR_DST_TON, "int");
        appendField(sb, Schema.COLUMN_ADDR_DST_NPI, "int");

        appendField(sb, Schema.COLUMN_ADDR_SRC_DIGITS, "ascii");
        appendField(sb, Schema.COLUMN_ADDR_SRC_TON, "int");
        appendField(sb, Schema.COLUMN_ADDR_SRC_NPI, "int");
        appendField(sb, Schema.COLUMN_ORIG_NETWORK_ID, "int");

        appendField(sb, Schema.COLUMN_DUE_DELAY, "int");
        appendField(sb, Schema.COLUMN_ALERTING_SUPPORTED, "boolean");

        appendField(sb, Schema.COLUMN_MESSAGE_ID, "bigint");
        appendField(sb, Schema.COLUMN_MO_MESSAGE_REF, "int");
        appendField(sb, Schema.COLUMN_ORIG_ESME_NAME, "text");
        appendField(sb, Schema.COLUMN_ORIG_SYSTEM_ID, "text");
        appendField(sb, Schema.COLUMN_DEST_CLUSTER_NAME, "text");
        appendField(sb, Schema.COLUMN_DEST_ESME_NAME, "text");
        appendField(sb, Schema.COLUMN_DEST_SYSTEM_ID, "text");
        appendField(sb, Schema.COLUMN_SUBMIT_DATE, "timestamp");
        appendField(sb, Schema.COLUMN_DELIVERY_DATE, "timestamp");

        appendField(sb, Schema.COLUMN_SERVICE_TYPE, "text");
        appendField(sb, Schema.COLUMN_ESM_CLASS, "int");
        appendField(sb, Schema.COLUMN_PROTOCOL_ID, "int");
        appendField(sb, Schema.COLUMN_PRIORITY, "int");
        appendField(sb, Schema.COLUMN_REGISTERED_DELIVERY, "int");
        appendField(sb, Schema.COLUMN_REPLACE, "int");
        appendField(sb, Schema.COLUMN_DATA_CODING, "int");
        appendField(sb, Schema.COLUMN_DEFAULT_MSG_ID, "int");

        appendField(sb, Schema.COLUMN_MESSAGE, "blob");
        appendField(sb, Schema.COLUMN_MESSAGE_TEXT, "text");
        appendField(sb, Schema.COLUMN_MESSAGE_BIN, "blob");
        appendField(sb, Schema.COLUMN_OPTIONAL_PARAMETERS, "text");
        appendField(sb, Schema.COLUMN_SCHEDULE_DELIVERY_TIME, "timestamp");
        appendField(sb, Schema.COLUMN_VALIDITY_PERIOD, "timestamp");

        appendField(sb, Schema.COLUMN_IMSI, "ascii");
        appendField(sb, Schema.COLUMN_CORR_ID, "ascii");
        appendField(sb, Schema.COLUMN_NNN_DIGITS, "ascii");
        appendField(sb, Schema.COLUMN_NNN_AN, "int");
        appendField(sb, Schema.COLUMN_NNN_NP, "int");
        appendField(sb, Schema.COLUMN_SM_STATUS, "int");
        appendField(sb, Schema.COLUMN_SM_TYPE, "int");
        appendField(sb, Schema.COLUMN_DELIVERY_COUNT, "int");
    }

    private synchronized void checkCurrentSlotTableExists() throws PersistenceException {
        try {
            try {
                // checking of CURRENT_SLOT_TABLE existence
                String sa = "SELECT \"" + Schema.COLUMN_NEXT_SLOT + "\" FROM \"" + Schema.FAMILY_CURRENT_SLOT_TABLE
                        + "\" where \"" + Schema.COLUMN_ID + "\"=0;";
                PreparedStatement ps = session.prepare(sa);
            } catch (InvalidQueryException e) {
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE \"");
                sb.append(Schema.FAMILY_CURRENT_SLOT_TABLE);
                sb.append("\" (");

                appendField(sb, Schema.COLUMN_ID, "int");
                appendField(sb, Schema.COLUMN_NEXT_SLOT, "bigint");

                sb.append("PRIMARY KEY (\"");
                sb.append(Schema.COLUMN_ID);
                sb.append("\"");
                sb.append("));");

                String s2 = sb.toString();
                PreparedStatement ps = session.prepare(s2);
                BoundStatement boundStatement = new BoundStatement(ps);
                ResultSet res = session.execute(boundStatement);
            }
        } catch (Exception e1) {
            String msg = "Failed to access or create table " + Schema.FAMILY_CURRENT_SLOT_TABLE + "!";
            throw new PersistenceException(msg, e1);
        }

        try {
            try {
                // checking of SMPP_SMS_ROUTING_RULE_2 existence
                String sa = "SELECT \"" + Schema.COLUMN_ADDRESS + "\" FROM \"" + Schema.FAMILY_SMPP_SMS_ROUTING_RULE + "\" where \"" + Schema.COLUMN_ADDRESS
                        + "\"='' and \"" + Schema.COLUMN_NETWORK_ID + "\"=0;";
                PreparedStatement ps = session.prepare(sa);
            } catch (InvalidQueryException e) {
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE \"");
                sb.append(Schema.FAMILY_SMPP_SMS_ROUTING_RULE);
                sb.append("\" (");

                appendField(sb, Schema.COLUMN_ADDRESS, "text");
                appendField(sb, Schema.COLUMN_NETWORK_ID, "int");
                appendField(sb, Schema.COLUMN_CLUSTER_NAME, "text");

                sb.append("PRIMARY KEY (\"");
                sb.append(Schema.COLUMN_ADDRESS);
                sb.append("\", \"");
                sb.append(Schema.COLUMN_NETWORK_ID);
                sb.append("\"");
                sb.append("));");

                String s2 = sb.toString();
                PreparedStatement ps = session.prepare(s2);
                BoundStatement boundStatement = new BoundStatement(ps);
                ResultSet res = session.execute(boundStatement);
            }
        } catch (Exception e1) {
            String msg = "Failed to access or create table " + Schema.FAMILY_SMPP_SMS_ROUTING_RULE + "!";
            throw new PersistenceException(msg, e1);
        }

        try {
            try {
                // checking of SIP_SMS_ROUTING_RULE_2 existence
                String sa = "SELECT \"" + Schema.COLUMN_ADDRESS + "\" FROM \"" + Schema.FAMILY_SIP_SMS_ROUTING_RULE + "\" where \"" + Schema.COLUMN_ADDRESS
                        + "\"='' and \"" + Schema.COLUMN_NETWORK_ID + "\"=0;";
                PreparedStatement ps = session.prepare(sa);
            } catch (InvalidQueryException e) {
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE \"");
                sb.append(Schema.FAMILY_SIP_SMS_ROUTING_RULE);
                sb.append("\" (");

                appendField(sb, Schema.COLUMN_ADDRESS, "text");
                appendField(sb, Schema.COLUMN_NETWORK_ID, "int");
                appendField(sb, Schema.COLUMN_CLUSTER_NAME, "text");

                sb.append("PRIMARY KEY (\"");
                sb.append(Schema.COLUMN_ADDRESS);
                sb.append("\", \"");
                sb.append(Schema.COLUMN_NETWORK_ID);
                sb.append("\"");
                sb.append("));");

                String s2 = sb.toString();
                PreparedStatement ps = session.prepare(s2);
                BoundStatement boundStatement = new BoundStatement(ps);
                ResultSet res = session.execute(boundStatement);
            }
        } catch (Exception e1) {
            String msg = "Failed to access or create table " + Schema.FAMILY_SIP_SMS_ROUTING_RULE + "!";
            throw new PersistenceException(msg, e1);
        }
    }

    protected void appendField(StringBuilder sb, String name, String type) {
        sb.append("\"");
        sb.append(name);
        sb.append("\" ");
        sb.append(type);
        sb.append(", ");
    }

    private void appendIndex(String tName, String fieldName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ON \"");
        sb.append(tName);
        sb.append("\" (\"");
        sb.append(fieldName);
        sb.append("\");");

        String s2 = sb.toString();
        PreparedStatement ps = session.prepare(s2);
        BoundStatement boundStatement = new BoundStatement(ps);
        ResultSet res = session.execute(boundStatement);
    }

    /**
     * Returns the SMPP Cluster name for passed address. If not found returns
     * null
     *
     * @param address
     * @return
     * @throws PersistenceException
     */
    public DbSmsRoutingRule c2_getSmppSmsRoutingRule(final String address, int networkId) throws PersistenceException {

        try {
            BoundStatement boundStatement = new BoundStatement(getSmppSmsRoutingRule);
            boundStatement.bind(address, networkId);
            ResultSet result = session.execute(boundStatement);

            Row row = result.one();
            if (row == null) {
                return null;
            } else {
                String name = row.getString(Schema.COLUMN_CLUSTER_NAME);
                DbSmsRoutingRule res = new DbSmsRoutingRule(SmsRoutingRuleType.SMPP, address, networkId, name);
                return res;
            }
        } catch (Exception e) {
            String msg = "Failed to getSmppSmsRoutingRule for address=" + address + " networkId=" + networkId + " !";
            throw new PersistenceException(msg, e);
        }
    }

    /**
     * Returns SIP Cluster name for passed address. If not found returns null
     *
     * @param address
     * @return
     * @throws PersistenceException
     */
    public DbSmsRoutingRule c2_getSipSmsRoutingRule(final String address, int networkId) throws PersistenceException {

        try {
            BoundStatement boundStatement = new BoundStatement(getSipSmsRoutingRule);
            boundStatement.bind(address, networkId);
            ResultSet result = session.execute(boundStatement);

            Row row = result.one();
            if (row == null) {
                return null;
            } else {
                String name = row.getString(Schema.COLUMN_CLUSTER_NAME);
                DbSmsRoutingRule res = new DbSmsRoutingRule(SmsRoutingRuleType.SIP, address, networkId, name);
                return res;
            }
        } catch (Exception e) {
            String msg = "Failed to getSipSmsRoutingRule for address=" + address + " networkId=" + networkId + " !";
            throw new PersistenceException(msg, e);
        }
    }

    /**
     * Updates the SMPP_SMS_ROUTING_RULE table, sets the CLUSTER_NAME for passed
     * ADDRESS
     *
     * @param dbSmsRoutingRule
     * @throws PersistenceException
     */
    public void c2_updateSmppSmsRoutingRule(DbSmsRoutingRule dbSmsRoutingRule) throws PersistenceException {
        try {
            BoundStatement boundStatement = new BoundStatement(updateSmppSmsRoutingRule);
            boundStatement.bind(dbSmsRoutingRule.getAddress(), dbSmsRoutingRule.getNetworkId(), dbSmsRoutingRule.getClusterName());
            session.execute(boundStatement);
        } catch (Exception e) {
            String msg = "Failed to updateSmppSmsRoutingRule for address=" + dbSmsRoutingRule.getAddress() + " networkId=" + dbSmsRoutingRule.getNetworkId() + " !";
            throw new PersistenceException(msg, e);
        }
    }

    /**
     * Updates the SIP_SMS_ROUTING_RULE table, sets the CLUSTER_NAME for passed
     * ADDRESS
     *
     * @param dbSmsRoutingRule
     * @throws PersistenceException
     */
    public void c2_updateSipSmsRoutingRule(DbSmsRoutingRule dbSmsRoutingRule) throws PersistenceException {
        try {
            BoundStatement boundStatement = new BoundStatement(updateSipSmsRoutingRule);
            boundStatement.bind(dbSmsRoutingRule.getAddress(), dbSmsRoutingRule.getNetworkId(), dbSmsRoutingRule.getClusterName());
            session.execute(boundStatement);
        } catch (Exception e) {
            String msg = "Failed to updateSipSmsRoutingRule for address=" + dbSmsRoutingRule.getAddress() + " networkId=" + dbSmsRoutingRule.getNetworkId() + " !";
            throw new PersistenceException(msg, e);
        }
    }

    /**
     * Deletes row corresponding to passed address from SMPP_SMS_ROUTING_RULE
     *
     * @param address
     * @throws PersistenceException
     */
    public void c2_deleteSmppSmsRoutingRule(final String address, int networkId) throws PersistenceException {
        try {
            BoundStatement boundStatement = new BoundStatement(deleteSmppSmsRoutingRule);
            boundStatement.bind(address, networkId);
            session.execute(boundStatement);
        } catch (Exception e) {
            String msg = "Failed to deleteSmppSmsRoutingRule for '" + address + "-" + networkId + "'!";
            throw new PersistenceException(msg, e);
        }
    }

    /**
     * Deletes row corresponding to passed address from SIP_SMS_ROUTING_RULE
     *
     * @param address
     * @throws PersistenceException
     */
    public void c2_deleteSipSmsRoutingRule(final String address, int networkId) throws PersistenceException {
        try {
            BoundStatement boundStatement = new BoundStatement(deleteSipSmsRoutingRule);
            boundStatement.bind(address, networkId);
            session.execute(boundStatement);
        } catch (Exception e) {
            String msg = "Failed to deleteSipSmsRoutingRule for '" + address + "-" + networkId + "'!";
            throw new PersistenceException(msg, e);
        }
    }

    /**
     * Returns first 100 rows of SMPP_SMS_ROUTING_RULE
     *
     * @return
     * @throws PersistenceException
     */
    public List<DbSmsRoutingRule> c2_getSmppSmsRoutingRulesRange() throws PersistenceException {
        return c2_getSmppSmsRoutingRulesRange(null);
    }

    /**
     * Returns 100 rows of SMPP_SMS_ROUTING_RULE starting from passed
     * lastAdress. If lastAdress, first 100 rows are returned
     *
     * @param lastAdress
     * @return
     * @throws PersistenceException
     */
    public List<DbSmsRoutingRule> c2_getSmppSmsRoutingRulesRange(String lastAdress) throws PersistenceException {

        List<DbSmsRoutingRule> ress = new FastList<DbSmsRoutingRule>();
        try {
            PreparedStatement ps = lastAdress != null ? getSmppSmsRoutingRulesRange : getSmppSmsRoutingRulesRange2;
            BoundStatement boundStatement = new BoundStatement(ps);
            if (lastAdress != null) {
                boundStatement.bind(lastAdress);
            }
            ResultSet result = session.execute(boundStatement);

            int i1 = 0;
            for (Row row : result) {
                String address = row.getString(Schema.COLUMN_ADDRESS);
                String name = row.getString(Schema.COLUMN_CLUSTER_NAME);
                int networkId = row.getInt(Schema.COLUMN_NETWORK_ID);

                DbSmsRoutingRule res = new DbSmsRoutingRule(SmsRoutingRuleType.SMPP, address, networkId, name);

                if (i1 == 0) {
                    i1 = 1;
                    if (lastAdress == null)
                        ress.add(res);
                } else {
                    ress.add(res);
                }
            }

            return ress;
        } catch (Exception e) {
            String msg = "Failed to getSmsRoutingRule DbSmsRoutingRule for all records: " + e;

            throw new PersistenceException(msg, e);
        }
    }

    /**
     * Returns first 100 rows of SIP_SMS_ROUTING_RULE
     *
     * @return
     * @throws PersistenceException
     */
    public List<DbSmsRoutingRule> c2_getSipSmsRoutingRulesRange() throws PersistenceException {
        return c2_getSipSmsRoutingRulesRange(null);
    }

    /**
     * Returns 100 rows of SIP_SMS_ROUTING_RULE starting from passed lastAdress.
     * If lastAdress, first 100 rows are returned
     *
     * @param lastAdress
     * @return
     * @throws PersistenceException
     */
    public List<DbSmsRoutingRule> c2_getSipSmsRoutingRulesRange(String lastAdress) throws PersistenceException {

        List<DbSmsRoutingRule> ress = new FastList<DbSmsRoutingRule>();
        try {
            PreparedStatement ps = lastAdress != null ? getSipSmsRoutingRulesRange : getSipSmsRoutingRulesRange2;
            BoundStatement boundStatement = new BoundStatement(ps);
            if (lastAdress != null) {
                boundStatement.bind(lastAdress);
            }
            ResultSet result = session.execute(boundStatement);

            int i1 = 0;
            for (Row row : result) {
                String address = row.getString(Schema.COLUMN_ADDRESS);
                String name = row.getString(Schema.COLUMN_CLUSTER_NAME);
                int networkId = row.getInt(Schema.COLUMN_NETWORK_ID);

                DbSmsRoutingRule res = new DbSmsRoutingRule(SmsRoutingRuleType.SIP, address, networkId, name);

                if (i1 == 0) {
                    i1 = 1;
                    if (lastAdress == null)
                        ress.add(res);
                } else {
                    ress.add(res);
                }
            }

            return ress;
        } catch (Exception e) {
            String msg = "Failed to getSmsRoutingRule DbSmsRoutingRule for all records: " + e;

            throw new PersistenceException(msg, e);
        }
    }

    /**
     * Deleting live tables (DST_SLOT_TABLE_YYYY_MM_DD and
     * SLOT_MESSAGES_TABLE_YYYY_MM_DD) for a defined date. Before deleting
     * checking is made for we can not delay table for future, today and 2 days
     * before
     *
     * @param dt
     *            Date for a table
     * @return true: success or perm failure, false: temporary failure, we need
     *         to retry
     */
    public boolean c2_deleteLiveTablesForDate(Date dt) {
        // auto_snapshot option !!!

        CheckDeletingDateResult res = checkDeletingDate(dt, "live cassandra tables");
        switch (res) {
            case permFailure:
                return true;
            case tempFailure:
                return false;
        }

        String tName = "DST_SLOT_TABLE" + getTableName(dt);
        this.doDeleteTable(tName);

        tName = "SLOT_MESSAGES_TABLE" + getTableName(dt);
        this.doDeleteTable(tName);

        String tName2 = this.getTableName(dt);
        dataTableRead.remove(tName2);

        return true;
    }

    /**
     * Deleting live table (MESSAGES) for a defined date. Before deleting
     * checking is made for we can not delay table for future, today and 2 days
     * before
     *
     * @param dt
     *            Date for a table
     * @return true: success or perm failure, false: temporary failure, we need
     *         to retry
     */
    public boolean c2_deleteArchiveTablesForDate(Date dt) {
        // auto_snapshot option !!!

        CheckDeletingDateResult res = checkDeletingDate(dt, "archive cassandra tables");
        switch (res) {
            case permFailure:
                return true;
            case tempFailure:
                return false;
        }

        String tName = "MESSAGES" + getTableName(dt);
        this.doDeleteTable(tName);

        String tName2 = this.getTableName(dt);
        dataTableRead.remove(tName2);

        return true;
    }

    private CheckDeletingDateResult checkDeletingDate(Date dt, String taskName) {
        // checking date
        int maxBackupsDays = 2;
        Date curTime = new Date();
        Date curDate = new Date(curTime.getYear(), curTime.getMonth(), curTime.getDate());
        Date maxDate = new Date(curDate.getTime() - maxBackupsDays * 24 * 3600 * 1000);
        if (!dt.before(maxDate)) {
            logger.warn("Rejected an attempt of dropping of " + taskName + " for too late date: " + dt);
            return CheckDeletingDateResult.permFailure;
        }

        // checking if this date is still in processing
        Date procDate = this.c2_getTimeForDueSlot(this.c2_getCurrentDueSlot());
        if (procDate.getYear() != curDate.getYear() || procDate.getMonth() != curDate.getMonth() || procDate.getDate() != curDate.getDate()) {
            logger.warn("Rejected an attempt of dropping of " + taskName + " for old message data are still in processing: " + dt);
            return CheckDeletingDateResult.tempFailure;
        }

        return CheckDeletingDateResult.success;
    }

    enum CheckDeletingDateResult {
        success, permFailure, tempFailure;
    }

    private void doDeleteTable(String tName) {
        try {
            String sa = "select * from \"" + tName + "\" limit 1";
            PreparedStatement ps = session.prepare(sa);
            BoundStatement boundStatement = new BoundStatement(ps);
            session.execute(boundStatement);
        } catch (Exception e) {
            logger.info("Can not drop cassandra table because it is absent: " + tName);
            return;
        }

        try {
            String sb = "DROP TABLE \"" + tName + "\"";
            PreparedStatement ps = session.prepare(sb);
            BoundStatement boundStatement = new BoundStatement(ps);
            session.execute(boundStatement);

            logger.warn("Successfully dropped cassandra table: " + tName);
        } catch (Exception e) {
            logger.warn("Exception when dropping cassandra table: " + tName, e);
        }
    }

    public Date[] c2_getLiveTableList(String keyspace) {
        String[] ss = this.c2_getTableList(keyspace);

        FastMap<Date, Date> res = new FastMap<Date, Date>();
        for (String s : ss) {
            Date dt = null;
            if (s.startsWith("DST_SLOT_TABLE_") && s.length() == 25) {
                String sYear = s.substring(15, 19);
                String sMon = s.substring(20, 22);
                String sDay = s.substring(23, 25);
                try {
                    int year = Integer.parseInt(sYear);
                    int mon = Integer.parseInt(sMon);
                    int day = Integer.parseInt(sDay);
                    dt = new Date(year - 1900, mon - 1, day);
                } catch (Exception e) {
                }
            }
            if (s.startsWith("SLOT_MESSAGES_TABLE_") && s.length() == 30) {
                String sYear = s.substring(20, 24);
                String sMon = s.substring(25, 27);
                String sDay = s.substring(28, 30);
                try {
                    int year = Integer.parseInt(sYear);
                    int mon = Integer.parseInt(sMon);
                    int day = Integer.parseInt(sDay);
                    dt = new Date(year - 1900, mon - 1, day);
                } catch (Exception e) {
                }
            }
            if (dt != null) {
                res.put(dt, dt);
            }
        }

        Date[] dd = new Date[res.size()];
        int i1 = 0;
        for (Date dt : res.keySet()) {
            dd[i1++] = dt;
        }
        Arrays.sort(dd);

        return dd;
    }

    public Date[] c2_getArchiveTableList(String keyspace) {
        String[] ss = this.c2_getTableList(keyspace);

        FastMap<Date, Date> res = new FastMap<Date, Date>();
        for (String s : ss) {
            Date dt = null;
            if (s.startsWith("MESSAGES_") && s.length() == 19) {
                String sYear = s.substring(9, 13);
                String sMon = s.substring(14, 16);
                String sDay = s.substring(17, 19);
                try {
                    int year = Integer.parseInt(sYear);
                    int mon = Integer.parseInt(sMon);
                    int day = Integer.parseInt(sDay);
                    dt = new Date(year - 1900, mon - 1, day);
                } catch (Exception e) {
                }
            }
            if (dt != null) {
                res.put(dt, dt);
            }
        }

        Date[] dd = new Date[res.size()];
        int i1 = 0;
        for (Date dt : res.keySet()) {
            dd[i1++] = dt;
        }
        Arrays.sort(dd);

        return dd;
    }

    public String[] c2_getTableList(String keyspace) {
        ArrayList<String> res = new ArrayList<String>();
        try {

            BoundStatement boundStatement = new BoundStatement(this.getTableList);

            ResultSet result = session.execute(boundStatement);

            for (Row row : result) {
                String keyspace1 = row.getString(Schema.COLUMN_SYSTEM_KEYSPACE_NAME);
                String tableName = row.getString(Schema.COLUMN_SYSTEM_COLUMNFAMILY_NAME);

                if (keyspace.equals(keyspace1)) {
                    res.add(tableName);
                }
            }
        } catch (Exception e) {
            logger.info("Can not get a cassandra table list");
            return new String[0];
        }

        String[] ss = new String[res.size()];
        res.toArray(ss);
        return ss;
    }

    private class DueSlotWritingElement {
        public long dueSlot;
        public int writingCount;
        public Date lastStartDate;

        public DueSlotWritingElement(long dueSlot) {
            this.dueSlot = dueSlot;
        }
    }
}
