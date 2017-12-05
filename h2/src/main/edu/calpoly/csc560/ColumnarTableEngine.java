package edu.calpoly.csc560;

import org.h2.engine.Database;
import org.h2.api.TableEngine;
import org.h2.table.TableBase;
import org.h2.command.ddl.CreateTableData;

import java.util.logging.Logger;

/**
 * An H2 TableEngine which creates columnar tables upon request
 * Columnar mode can be activated at runtime using the JDBC URL parameter:
 *   ;DEFAULT_TABLE_ENGINE=org.h2.column.ColumnarTableEngine
 *
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
