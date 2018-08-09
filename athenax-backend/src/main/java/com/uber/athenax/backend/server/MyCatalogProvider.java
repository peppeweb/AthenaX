package com.uber.athenax.backend.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalog;
import com.uber.athenax.vm.api.tables.AthenaXTableCatalogProvider;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.CatalogNotExistException;
import org.apache.flink.table.api.TableNotExistException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import scala.Option;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.KAFKA_CONFIG_PREFIX;
import static com.uber.athenax.vm.connectors.kafka.KafkaConnectorDescriptorValidator.TOPIC_NAME_KEY;


public class MyCatalogProvider implements AthenaXTableCatalogProvider {
    static final String DEST_TOPIC = "bar";
    static final String SOURCE_TOPIC = "foo";

    private static final long STABILIZE_SLEEP_DELAYS = 3000;
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static String brokerAddress = "localhost";

    @Override
    public Map<String, AthenaXTableCatalog> getInputCatalog(String cluster) {
        Preconditions.checkNotNull(brokerAddress);
        return Collections.singletonMap(
                "input",
                new KafkaCatalog(brokerAddress, Collections.singletonList(SOURCE_TOPIC)));
    }

    @Override
    public AthenaXTableCatalog getOutputCatalog(String cluster, List<String> outputs) {
        Preconditions.checkNotNull(brokerAddress);
        return new KafkaCatalog(brokerAddress, Collections.singletonList(DEST_TOPIC));
    }

    public static class KafkaCatalog implements AthenaXTableCatalog {
        private static final long serialVersionUID = -1L;

        private final String broker;
        private final List<String> availableTables;

        KafkaCatalog(String broker, List<String> availableTables) {
            this.broker = broker;
            this.availableTables = availableTables;
        }

        @Override
        public ExternalCatalogTable getTable(String tableName) throws TableNotExistException {
            Map<String, String> sourceTableProp = new HashMap<>();
            sourceTableProp.put(KAFKA_CONFIG_PREFIX + "." + ConsumerConfig.GROUP_ID_CONFIG, tableName);
            sourceTableProp.put(KAFKA_CONFIG_PREFIX + "." + CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, broker);
            sourceTableProp.put(KAFKA_CONFIG_PREFIX + "." + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            sourceTableProp.put(TOPIC_NAME_KEY, tableName);
            return getKafkaExternalCatalogTable(sourceTableProp);
        }

        @Override
        public List<String> listTables() {
            return availableTables;
        }

        @Override
        public ExternalCatalog getSubCatalog(String dbName) throws CatalogNotExistException {
            throw new CatalogNotExistException(dbName);
        }

        @Override
        public List<String> listSubCatalogs() {
            return Collections.emptyList();
        }
    }

    static class KafkaInputExternalCatalogTable extends ExternalCatalogTable implements Serializable {
        static final TableSchema SCHEMA = new TableSchema(
                new String[] {"id", "proctime"},
                new TypeInformation<?>[] {BasicTypeInfo.INT_TYPE_INFO, SqlTimeTypeInfo.TIMESTAMP});

        KafkaInputExternalCatalogTable(ConnectorDescriptor descriptor) {
            super(descriptor, Option.empty(), Option.empty(), Option.empty(), Option.empty());
        }
    }

    static KafkaInputExternalCatalogTable getKafkaExternalCatalogTable(Map<String, String> props) {
        ConnectorDescriptor descriptor = new ConnectorDescriptor("kafka+json", 1, false) {
            @Override
            public void addConnectorProperties(DescriptorProperties properties) {
                properties.putTableSchema("athenax.kafka.topic.schema", KafkaInputExternalCatalogTable.SCHEMA);
                properties.putProperties(props);
            }
        };
        return new KafkaInputExternalCatalogTable(descriptor);
    }
}