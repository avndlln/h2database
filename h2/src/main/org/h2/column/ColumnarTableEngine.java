package org.h2.column;

import org.h2.engine.Database;
import org.h2.api.TableEngine;
import org.h2.table.TableBase;
import org.h2.command.ddl.CreateTableData;

import java.util.logging.Logger;

/**
 *
 * @author csc560team1
 */
public class ColumnarTableEngine implements TableEngine {

    private Logger log = Logger.getLogger(ColumnarTableEngine.class.getName());

    private static ColumnarTable lastCreated = null;
    
    @Override
    public TableBase createTable(CreateTableData data) {
	log.info("createTable(" + data.tableName + ")");
	lastCreated = new ColumnarTable(data);
	return lastCreated;
    }

    // used for detailed unit testing
    public static ColumnarTable getLastCreated() {
	return lastCreated;
    }
    
}
