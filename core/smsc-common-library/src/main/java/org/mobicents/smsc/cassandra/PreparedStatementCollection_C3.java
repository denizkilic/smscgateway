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

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class PreparedStatementCollection_C3 {

    private DBOperations_C2 dbOperation;
    private String tName;
    private boolean shortMessageNewStringFormat;
    private boolean addedCorrId;
    private boolean addedNetworkId;
    private boolean addedOrigNetworkId;

    protected PreparedStatement createDueSlotForTargetId;
    protected PreparedStatement getDueSlotForTargetId;
    protected PreparedStatement createRecordCurrent;
    protected PreparedStatement getRecordData;
    protected PreparedStatement getRecordData2;
    protected PreparedStatement updateInSystem;
    protected PreparedStatement updateAlertingSupport;
    protected PreparedStatement createRecordArchive;

    public PreparedStatementCollection_C3(DBOperations_C2 dbOperation, String tName, int ttlCurrent, int ttlArchive) {
        this.dbOperation = dbOperation;
        this.tName = tName;

        // check table version format
        try {
            shortMessageNewStringFormat = false;
            String s1 = "select \"" + Schema.COLUMN_MESSAGE_TEXT + "\" FROM \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" limit 1;";
            dbOperation.session.execute(s1);
            shortMessageNewStringFormat = true;
        } catch (Exception e) {
        }
        try {
            addedCorrId = false;
            String s1 = "select \"" + Schema.COLUMN_CORR_ID + "\" FROM \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" limit 1;";
            dbOperation.session.execute(s1);
            addedCorrId = true;
        } catch (Exception e) {
        }
        try {
            addedNetworkId = false;
            String s1 = "select \"" + Schema.COLUMN_NETWORK_ID + "\" FROM \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" limit 1;";
            dbOperation.session.execute(s1);
            addedNetworkId = true;
        } catch (Exception e) {
        }
        try {
            addedOrigNetworkId = false;
            String s1 = "select \"" + Schema.COLUMN_ORIG_NETWORK_ID + "\" FROM \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" limit 1;";
            dbOperation.session.execute(s1);
            addedOrigNetworkId = true;
        } catch (Exception e) {
        }

        try {
            String s1 = getFillUpdateFields();
            String s11 = getFillUpdateFields_Archive();
            String s2 = getFillUpdateFields2();
            String s22 = getFillUpdateFields2_Archive();
            String s3a, s3b;
            if (ttlCurrent > 0) {
                s3a = "USING TTL " + ttlCurrent;
            } else {
                s3a = "";
            }
            if (ttlArchive > 0) {
                s3b = "USING TTL " + ttlArchive;
            } else {
                s3b = "";
            }

            String sa = "INSERT INTO \"" + Schema.FAMILY_DST_SLOT_TABLE + tName + "\" (\"" + Schema.COLUMN_TARGET_ID + "\", \"" + Schema.COLUMN_DUE_SLOT
                    + "\") VALUES (?, ?) " + s3a + ";";
            createDueSlotForTargetId = dbOperation.session.prepare(sa);
            sa = "SELECT \"" + Schema.COLUMN_DUE_SLOT + "\" FROM \"" + Schema.FAMILY_DST_SLOT_TABLE + tName + "\" where \"" + Schema.COLUMN_TARGET_ID
                    + "\"=?;";
            getDueSlotForTargetId = dbOperation.session.prepare(sa);
            sa = "INSERT INTO \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" (" + s1 + ") VALUES (" + s2 + ") " + s3a + ";";
            createRecordCurrent = dbOperation.session.prepare(sa);
            sa = "SELECT * FROM \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" where \"" + Schema.COLUMN_DUE_SLOT
                    + "\"=?;";
            getRecordData = dbOperation.session.prepare(sa);
            sa = "SELECT * FROM \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" where \"" + Schema.COLUMN_DUE_SLOT + "\"=? and \""
                    + Schema.COLUMN_TARGET_ID + "\"=?;";
            getRecordData2 = dbOperation.session.prepare(sa);
            sa = "UPDATE \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" " + s3a + " SET \"" + Schema.COLUMN_IN_SYSTEM + "\"=?, \""
                    + Schema.COLUMN_SMSC_UUID + "\"=? where \"" + Schema.COLUMN_DUE_SLOT + "\"=? and \"" + Schema.COLUMN_TARGET_ID + "\"=? and \""
                    + Schema.COLUMN_ID + "\"=?;";
            updateInSystem = dbOperation.session.prepare(sa);
            sa = "UPDATE \"" + Schema.FAMILY_SLOT_MESSAGES_TABLE + tName + "\" " + s3a + " SET \"" + Schema.COLUMN_ALERTING_SUPPORTED + "\"=? where \""
                    + Schema.COLUMN_DUE_SLOT + "\"=? and \"" + Schema.COLUMN_TARGET_ID + "\"=? and \"" + Schema.COLUMN_ID + "\"=?;";
            updateAlertingSupport = dbOperation.session.prepare(sa);
            sa = "INSERT INTO \"" + Schema.FAMILY_MESSAGES + tName + "\" (" + s1 + s11 + ") VALUES (" + s2 + s22 + ") " + s3b + ";";
            createRecordArchive = dbOperation.session.prepare(sa);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    public String getTName() {
        return tName;
    }

    public boolean getShortMessageNewStringFormat() {
        return shortMessageNewStringFormat;
    }

    public boolean getAddedCorrId() {
        return this.addedCorrId;
    }

    public boolean getAddedNetworkId() {
        return this.addedNetworkId;
    }

    public boolean getAddedOrigNetworkId() {
        return this.addedOrigNetworkId;
    }

    private String getFillUpdateFields() {
        StringBuilder sb = new StringBuilder();

        sb.append("\"");
        sb.append(Schema.COLUMN_ID);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_TARGET_ID);
        if (addedNetworkId) {
            sb.append("\", \"");
            sb.append(Schema.COLUMN_NETWORK_ID);
        }
        if (addedOrigNetworkId) {
            sb.append("\", \"");
            sb.append(Schema.COLUMN_ORIG_NETWORK_ID);
        }
        sb.append("\", \"");
        sb.append(Schema.COLUMN_DUE_SLOT);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_IN_SYSTEM);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_SMSC_UUID);
        sb.append("\", \"");

        sb.append(Schema.COLUMN_ADDR_DST_DIGITS);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_ADDR_DST_TON);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_ADDR_DST_NPI);
        sb.append("\", \"");

        sb.append(Schema.COLUMN_ADDR_SRC_DIGITS);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_ADDR_SRC_TON);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_ADDR_SRC_NPI);
        sb.append("\", \"");

        sb.append(Schema.COLUMN_DUE_DELAY);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_SM_STATUS);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_ALERTING_SUPPORTED);
        sb.append("\", \"");

        sb.append(Schema.COLUMN_MESSAGE_ID);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_MO_MESSAGE_REF);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_ORIG_ESME_NAME);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_ORIG_SYSTEM_ID);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_SUBMIT_DATE);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_DELIVERY_DATE);
        sb.append("\", \"");

        sb.append(Schema.COLUMN_SERVICE_TYPE);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_ESM_CLASS);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_PROTOCOL_ID);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_PRIORITY);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_REGISTERED_DELIVERY);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_REPLACE);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_DATA_CODING);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_DEFAULT_MSG_ID);
        sb.append("\", \"");

        sb.append(Schema.COLUMN_MESSAGE);
        sb.append("\", \"");
        if (shortMessageNewStringFormat) {
            sb.append(Schema.COLUMN_MESSAGE_TEXT);
            sb.append("\", \"");
            sb.append(Schema.COLUMN_MESSAGE_BIN);
            sb.append("\", \"");
        }
        sb.append(Schema.COLUMN_SCHEDULE_DELIVERY_TIME);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_VALIDITY_PERIOD);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_DELIVERY_COUNT);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_OPTIONAL_PARAMETERS);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_IMSI);
        if (this.addedCorrId) {
            sb.append("\", \"");
            sb.append(Schema.COLUMN_CORR_ID);
        }
        sb.append("\"");

        return sb.toString();
    }

    private String getFillUpdateFields_Archive() {
        StringBuilder sb = new StringBuilder();

        sb.append(", \"");
        sb.append(Schema.COLUMN_NNN_DIGITS);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_NNN_AN);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_NNN_NP);
        sb.append("\", \"");
        sb.append(Schema.COLUMN_SM_TYPE);
        sb.append("\"");

        return sb.toString();
    }

    private String getFillUpdateFields2() {
        int cnt;
        if (shortMessageNewStringFormat) {
            cnt = 36;
        } else {
            cnt = 34;
        }
        if (this.addedCorrId)
            cnt++;
        if (this.addedNetworkId)
            cnt++;
        if (this.addedOrigNetworkId)
            cnt++;

        StringBuilder sb = new StringBuilder();
        int i2 = 0;
        for (int i1 = 0; i1 < cnt; i1++) {
            if (i2 == 0)
                i2 = 1;
            else
                sb.append(", ");
            sb.append("?");
        }
        return sb.toString();
    }

    private String getFillUpdateFields2_Archive() {
        int cnt;
        cnt = 4;

        StringBuilder sb = new StringBuilder();
        for (int i1 = 0; i1 < cnt; i1++) {
            sb.append(", ?");
        }
        return sb.toString();
    }

    /**
     * smsc specific builder, add double quotes to all external columns and family's
     * NOTE: class in not Thread safe
     * <p>
     * ex.:
     * calling PrepareStatementQueryBulder.from("SOME_NAME") will be wrapped with double quotes == "\"SOME_NAME\""
     */
    public static class Builder {
        private static final String SELECT = "SELECT ";
        private static final String SELECT_ALL = "SELECT * ";
        private static final String INSERT = "INSERT INTO ";
        private static final String DELETE = "DELETE ";
        private static final String LIMIT = "LIMIT ";
        private static final String VALUES = " VALUES ";
        private static final String AND = " AND ";
        private static final String WHERE = " WHERE ";
        private static final String EQUALS = " = ";
        private static final String NOT_EQUALS = " != ";
        private static final String FROM = "FROM ";
        private static final String INSERT_TTL = " USING TTL ?";

        private final Session session;
        private StringBuilder cache = new StringBuilder();

        public Builder(Session session) {
            this.session = session;
        }

        public Builder selectAll() {
            cache.append(SELECT_ALL);
            return this;
        }

        public Builder select(String... columns) {
            cache.append(SELECT);
            for (int i = 0; i < columns.length; i++) {
                wrapWithQuotes(columns[i]);
                if (i < columns.length - 1) {
                    cache.append(", ");
                }
            }
            return this;
        }

        public Builder insertInto(String family) {
            return addAction(family, INSERT);
        }

        /**
         * this method for insertInto()
         *
         * @param columns
         * @return this
         */
        public Builder values(String... columns) {
            cache.append("(");
            for (int i = 0; i < columns.length; i++) {
                wrapWithQuotes(columns[i]);
                if (i < columns.length - 1) {
                    cache.append(", ");
                }
            }
            cache.append(")");
            cache.append(VALUES);
            cache.append("(");

            for (int i = 0; i < columns.length; i++) {
                cache.append("?");
                if (i < columns.length-1) {
                    cache.append(", ");
                }
            }
            cache.append(")");
            return this;
        }

        public Builder where(String statement) {
            cache.append(WHERE);
            cache.append(statement);
            return this;
        }

        public static String and(String... statments) {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < statments.length; i++) {
                builder.append(statments[i]);
                if (i < statments.length-1) {
                    builder.append(AND);
                }
            }
            return builder.toString();
        }

        public static String eq(String column) {
            return "\"" + column + "\"" + EQUALS + "?";
        }

        public static String neq(String column) {
            return "\"" + column + "\"" + NOT_EQUALS + "?";
        }

        public Builder from(String family) {
            return addAction(family, FROM);
        }

        public Builder delete() {
            cache.append(DELETE);
            return this;
        }

        public PreparedStatement build() {
            if (session == null || cache.length() < 1)
                throw new RuntimeException("Builder not initialized properly. Session is not set or StringBuilder is empty.");

            String query = getQuery();
            cache = new StringBuilder();
            return session.prepare(query);
        }

        public Builder usingTtl() {
            cache.append(INSERT_TTL);
            return this;
        }

        public Builder limit(int i) {
            cache.append(LIMIT);
            cache.append(i);
            return this;
        }

        private String getQuery() {
            cache.append(";");
            return cache.toString();
        }

        private Builder addAction(String family, String action) {
            cache.append(action);
            wrapWithQuotes(family);
            return this;
        }

        private void wrapWithQuotes(String val) {
            cache.append("\"");
            cache.append(val);
            cache.append("\"");
        }
    }

}
