package org.h2.column;

import org.h2.index.ScanIndex;
import org.h2.table.IndexColumn;
import org.h2.index.IndexType;
import org.h2.result.Row;
import org.h2.engine.Session;

import java.util.logging.Logger;

public class ColumnarScanIndex extends ScanIndex {
    Logger log = Logger.getLogger(ColumnarTable.class.getName());

    public ColumnarScanIndex(ColumnarTable table, int id, IndexColumn[] columns,
			     IndexType indexType) {
	super(table, id, columns, indexType);
	//rows = new ColumnarList();
    }

    /*
    private class ColumnarList extends ArrayList {

	private Map<String,List> columnStore = new LinkedHashMap<String,List>();
	private String[] columnNames;
	
	public ColumnarList(String[] columnNames) {
	    super();
	    this.columnNames = columnNames;
	}

	@Override
	public boolean add(E e) {
	    Row r = (Row) e;
	    
	}
    }
    */    

    @Override
    public Row getRow(Session session, long key) {
	log.info("getRow() - " + key);
	return super.getRow(session, key);
	//return rows.get((int) key);
    }

    @Override
    public void add(Session session, Row row) {

	log.info("add() - " + row);
	super.add(session, row);

	/*
	// in-memory
	if (firstFree == -1) {
	    int key = rows.size();
	    row.setKey(key);
	    rows.add(row);
        } else {
	    long key = firstFree;
	    Row free = rows.get((int) key);
	    firstFree = free.getKey();
	    row.setKey(key);
	    rows.set((int) key, row);
	}
	row.setDeleted(false);
	if (database.isMultiVersion()) {
	    if (delta == null) {
		delta = New.hashSet();
	    }
	    boolean wasDeleted = delta.remove(row);
	    if (!wasDeleted) {
		delta.add(row);
	    }
	    incrementRowCount(session.getId(), 1);
	}
	rowCount++;
	*/
    }


    /*
    @Override
    public void remove(Session session, Row row) {
	// in-memory
	if (!database.isMultiVersion() && rowCount == 1) {
	    rows = New.arrayList();
	    firstFree = -1;
	} else {
	    Row free = session.createRow(null, 1);
	    free.setKey(firstFree);
	    long key = row.getKey();
	    if (rows.size() <= key) {
		throw DbException.get(ErrorCode.ROW_NOT_FOUND_WHEN_DELETING_1,
				      rows.size() + ": " + key);
	    }
	    rows.set((int) key, free);
	    firstFree = key;
	}
	if (database.isMultiVersion()) {
	    // if storage is null, the delete flag is not yet set
	    row.setDeleted(true);
	    if (delta == null) {
		delta = New.hashSet();
	    }
	    boolean wasAdded = delta.remove(row);
	    if (!wasAdded) {
		delta.add(row);
	    }
	    incrementRowCount(session.getId(), -1);
	}
	rowCount--;
    }

    @Override
    public Cursor find(Session session, SearchRow first, SearchRow last) {
	return new ScanCursor(session, this, database.isMultiVersion());
    }
    */
    
}
