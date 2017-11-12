/*
 * Copyright 2004-2014 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.column;

import java.util.Iterator;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.index.Cursor;

/**
 * The cursor implementation for the columnar index.
 */
public class ColumnarCursor implements Cursor {
    private final ColumnarIndex theIndex;
    private Row row;
    private final Session session;
    private final boolean multiVersion;
    private Iterator<Row> delta;

    ColumnarCursor(Session session, ColumnarIndex theIndex, boolean multiVersion) {
        this.session = session;
        this.theIndex = theIndex;
        this.multiVersion = multiVersion;
        if (multiVersion) {
            delta = theIndex.getDelta();
        }
        row = null;
    }

    @Override
    public Row get() {
        return row;
    }

    @Override
    public SearchRow getSearchRow() {
        return row;
    }

    @Override
    public boolean next() {
        if (multiVersion) {
            while (true) {
                if (delta != null) {
                    if (!delta.hasNext()) {
                        delta = null;
                        row = null;
                        continue;
                    }
                    row = delta.next();
                    if (!row.isDeleted() || row.getSessionId() == session.getId()) {
                        continue;
                    }
                } else {
                    row = theIndex.getNextRow(row);
                    if (row != null && row.getSessionId() != 0 &&
			row.getSessionId() != session.getId()) {
                        continue;
                    }
                }
                break;
            }
            return row != null;
        }
        while ((row = theIndex.getNextRow(row)) == ColumnarIndex.TOMBSTONE) { };
        return row != null;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError(toString());
    }

}
