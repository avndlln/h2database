package org.h2.column.mvstore;

import org.h2.engine.Database;
import org.h2.api.TableEngine;
import org.h2.table.TableBase;
//import org.h2.table.RegularTable;
import org.h2.mvstore.db.MVTableEngine;
import org.h2.mvstore.db.MVTableEngine.Store;
import org.h2.command.ddl.CreateTableData;

import java.util.logging.Logger;

public class ColumnarTableEngine extends MVTableEngine { //implements TableEngine {

    private Logger log = Logger.getLogger(ColumnarTableEngine.class.getName());

    @Override
    public TableBase createTable(CreateTableData data) {
	System.err.println("ColumnarTableEngine::createTable");
	log.info("createTable - " + data);
	return super.createTable(data);

	/*
	Database db = data.session.getDatabase();
	Store store = init(db);
	ColumnarTable table = new ColumnarTable(data, store);
	table.init(data.session);
	store.tableMap.put(table.getMapName(), table);  // tableMap is private?
	return table;
	*/
    }
}
    
