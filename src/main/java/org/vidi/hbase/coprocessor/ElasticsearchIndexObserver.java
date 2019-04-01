package org.vidi.hbase.coprocessor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.NavigableMap;

/**
 * @author vidi
 */
public class ElasticsearchIndexObserver extends BaseRegionObserver {
    private static final Log LOG = LogFactory.getLog(ElasticsearchIndexObserver.class);

    /**
     * 是否处理
     */
    private boolean effective;

    /**
     * ES TransportClient 实例, 完成 ES 相关操作
     */
    private TransportClient client;

    private String host;
    private int port;
    private String clusterName;
    private String indexName;

    /**
     * 生命周期内, 只执行一次, 用来做一些初始化的操作
     */
    public void start(CoprocessorEnvironment e) throws IOException {
        super.start(e);
        init(e);
        Settings settings = Settings.builder().put("cluster.name", clusterName).build();
        try {
            client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
            this.effective = true;
            LOG.info("ElasticsearchIndexObserver is active.");
        } catch (Exception e1) {
            this.effective = false;
            LOG.error("ElasticsearchIndexObserver is inactive, please check Elasticsearch configuration params and repeat create this table..");
        }
    }

    public void stop(CoprocessorEnvironment e) throws IOException {
        this.client.close();
        super.stop(e);
    }

    /**
     * 获取 Observer 的参数
     */
    private void init(CoprocessorEnvironment e) {
        this.host = e.getConfiguration().get("host", null);
        this.port = e.getConfiguration().getInt("port", 9300);
        this.clusterName = e.getConfiguration().get("clusterName", null);
        this.indexName = e.getConfiguration().get("indexName", null);
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put,
                        WALEdit edit, Durability durability) throws IOException {
        super.postPut(e, put, edit, durability);
        if (!effective) {
            return;
        }
        String rowkey = Bytes.toString(put.getRow());

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        NavigableMap<byte[], List<Cell>> map = put.getFamilyCellMap();
        map.forEach((key, cells) -> {
            String columnFamily = Bytes.toString(key);
            cells.forEach(cell -> {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                HBaseDoc hBaseDoc = new HBaseDoc();
                hBaseDoc.setRowkey(rowkey);
                hBaseDoc.setColumnFamily(columnFamily);
                hBaseDoc.setColumn(column);
                bulkRequestBuilder.add(client.prepareUpdate(indexName, "hbase", rowkey + columnFamily).setDoc(XContentType.JSON, hBaseDoc));
            });
        });

        // Saving the HBase column and columnFamily information.
        bulkRequestBuilder.get();
    }

    public void postDelete(final ObserverContext<RegionCoprocessorEnvironment> e, final Delete delete,
                           final WALEdit edit, final Durability durability) {
        if (!effective) {
            return;
        }
        String rowkey = Bytes.toString(delete.getRow());
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        NavigableMap<byte[], List<Cell>> map = delete.getFamilyCellMap();
        map.forEach((key, cells) -> {
            String columnFamily = Bytes.toString(key);
            cells.forEach(cell -> {
                String column = Bytes.toString(CellUtil.cloneQualifier(cell));
                HBaseDoc hBaseDoc = new HBaseDoc();
                hBaseDoc.setRowkey(rowkey);
                hBaseDoc.setColumnFamily(columnFamily);
                hBaseDoc.setColumn(column);
                bulkRequestBuilder.add(client.prepareDelete(indexName, "hbase", rowkey + columnFamily));
            });
        });
        // Delete the HBase column and columnFamily information.
        bulkRequestBuilder.get();
    }
}
