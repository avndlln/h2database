package org.h2.column.mvstore;

import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.index.IndexType;
import org.h2.table.IndexColumn;
import org.h2.result.Row;
import org.h2.index.Cursor;
import org.h2.result.SearchRow;

import org.h2.mvstore.db.MVTable;
import org.h2.mvstore.db.MVPrimaryIndex;

import java.util.logging.Logger;

public class ColumnarPrimaryIndex extends MVPrimaryIndex {

    private Logger log = Logger.getLogger(ColumnarPrimaryIndex.class.getName());
    
    public ColumnarPrimaryIndex(Database db, ColumnarTable table, int id,
				IndexColumn[] columns, IndexType indexType) {
	super(db, (MVTable) table, id, columns, indexType);
    }

    @Override
    public void add(Session session, Row row) {
	log.info("add() - row: " + row);
	super.add(session, row);
    }

    @Override
    public void remove(Session session, Row row) {
	log.info("remove() - row: " + row);
	super.remove(session, row);
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
	return super.find(session, first, last);
    }

    @Override
    public Row getRow(Session session, long key) {
	return super.getRow(session, key);
    }

}
