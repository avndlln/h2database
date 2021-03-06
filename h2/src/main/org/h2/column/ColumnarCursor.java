package org.h2.column;

import java.util.Iterator;
import org.h2.engine.Session;
import org.h2.message.DbException;
import org.h2.result.Row;
import org.h2.result.SearchRow;
import org.h2.index.Cursor;

import java.util.logging.Logger;

/**
 * Cursor implementation for the columnar index.
 *
 */
public class ColumnarCursor implements Cursor {
    Logger log = Logger.getLogger(ColumnarCursor.class.getName());
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

	// get the next non-tombstone row
	row = theIndex.getNextRow(row);
	while (row != null && row.getKey() == ColumnarIndex.TOMBSTONE.getKey()) {
	    row = theIndex.getNextRow(row);
	}

	log.info("next() - row: " + row);
	
        return row != null;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError(toString());
    }

}
