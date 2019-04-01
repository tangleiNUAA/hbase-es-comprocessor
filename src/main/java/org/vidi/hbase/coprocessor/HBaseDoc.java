package org.vidi.hbase.coprocessor;

/**
 * 存储到 ES 的 doc 实体
 *
 * @author vidi
 */
public class HBaseDoc {
    private String rowkey;
    private String columnFamily;
    private String column;

    public String getRowkey() {
        return rowkey;
    }

    public void setRowkey(String rowkey) {
        this.rowkey = rowkey;
    }

    public String getColumnFamily() {
        return columnFamily;
    }

    public void setColumnFamily(String columnFamily) {
        this.columnFamily = columnFamily;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public HBaseDoc() {
    }

    public HBaseDoc(String rowkey, String columnFamily, String column) {
        this.rowkey = rowkey;
        this.columnFamily = columnFamily;
        this.column = column;
    }

    @Override
    public String toString() {
        return "HBaseDoc{" +
                "rowkey='" + rowkey + '\'' +
                ", columnFamily='" + columnFamily + '\'' +
                ", column='" + column + '\'' +
                '}';
    }
}
