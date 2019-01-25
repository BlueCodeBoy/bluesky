package org.pentaho.di.trans.consumer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import kafka.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.pentaho.di.core.CheckResult;
import org.pentaho.di.core.CheckResultInterface;
import org.pentaho.di.core.Const;
import org.pentaho.di.core.annotations.Step;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettlePluginException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.exception.KettleXMLException;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.core.row.value.ValueMetaFactory;
import org.pentaho.di.core.variables.VariableSpace;
import org.pentaho.di.core.xml.XMLHandler;
import org.pentaho.di.i18n.BaseMessages;
import org.pentaho.di.repository.ObjectId;
import org.pentaho.di.repository.Repository;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.*;
import org.pentaho.metastore.api.IMetaStore;
import org.w3c.dom.Node;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.*;

/**
 * Kafka Consumer step definitions and serializer to/from XML and to/from Kettle
 * repository.
 *
 * @author Michael Spector
 */
@Step(
        id = "KafkaConsumer",
        image = "org/pentaho/di/trans/consumer/resources/kafka_consumer.png",
        i18nPackageName = "org.pentaho.di.trans.consumer",
        name = "KafkaConsumerDialog.Shell.Title",
        description = "KafkaConsumerDialog.Shell.Tooltip",
        documentationUrl = "KafkaConsumerDialog.Shell.DocumentationURL",
        casesUrl = "KafkaConsumerDialog.Shell.CasesURL",
        categoryDescription = "i18n:org.pentaho.di.trans.step:BaseStep.Category.Input")
public class KafkaConsumerMeta extends BaseStepMeta implements StepMetaInterface {

    @SuppressWarnings("WeakerAccess")
    protected static final String[] KAFKA_PROPERTIES_NAMES = new String[]{"zookeeper.connect", "group.id", "consumer.id",
            "socket.timeout.ms", "socket.receive.buffer.bytes", "fetch.message.max.bytes", "auto.commit.interval.ms",
            "queued.max.message.chunks", "rebalance.max.retries", "fetch.min.bytes", "fetch.wait.max.ms",
            "rebalance.backoff.ms", "refresh.leader.backoff.ms", "auto.commit.enable", "auto.offset.reset",
            "consumer.timeout.ms", "client.id", "zookeeper.session.timeout.ms", "zookeeper.connection.timeout.ms",
            "zookeeper.sync.time.ms"};


    private static Class<?> PKG = KafkaConsumerMeta.class; // for i18n purposes, needed by Translator2!!
    @SuppressWarnings("WeakerAccess")
    protected static final Map<String, String> KAFKA_PROPERTIES_DEFAULTS = new HashMap<String, String>();

    public void setMap(Map<String, Integer> map) {
        this.map = map;
    }

    private  Map<String,Integer> map = null;

    private static final String ATTR_TOPIC = "TOPIC";
    private static final String ATTR_OFFSET = "TOPIC_OFSET";
    private static final String ATTR_PARTITON = "TOPIC_PARTITION";
    private static final String ATTR_POLL = "TOPIC_POLL";
    public static final String MIDDLE = "_BLUESKY_";
    public static final String CDC_FLAG = "bluesky_flag";
    public static final String CDC_TIMESTAMP = "bluesky_timestamp";
//    public static final String[] formateTye = { "NONE", "JSON","XML" };
    public static final String[] formateTye = { "NONE", "JSON"};
    private static final String ATTR_FIELD = "FIELD";
    private static final String ATTR_MAP = "MAPDATA";
    private static final String ATTR_KEY_FIELD = "KEY_FIELD";
    private static final String ATTR_LIMIT = "LIMIT";
    private static final String ATTR_DATAFORMATE = "DATAFORMATE";
    private static final String ATTR_LINEDATA = "LINEDATA";
    private static final String ATTR_TIMEOUT = "TIMEOUT";
    private static final String ATTR_STOP_ON_EMPTY_TOPIC = "STOP_ON_EMPTY_TOPIC";
    private static final String ATTR_KAFKA = "KAFKA";
    private int allloopTime = 0;
    static {
        KAFKA_PROPERTIES_DEFAULTS.put("zookeeper.connect", "localhost:2181");
        KAFKA_PROPERTIES_DEFAULTS.put("group.id", "group");
    }

    private Properties kafkaProperties = new Properties();
    private String topic;
    private String field;
    private String keyField;
    private String limit;
    private String offset;
    private String pollMS;
    private String partition;

    public String getPartition() {
        return partition;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public String getPollMS() {
        return pollMS;
    }

    public void setPollMS(String pollMS) {
        this.pollMS = pollMS;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
    }

    public String getDataformate() {
        return dataformate;
    }

    public void setDataformate(String dataformate) {
        this.dataformate = dataformate;
    }

    public boolean isIslineData() {
        return islineData;
    }

    public void setIslineData(boolean islineData) {
        this.islineData = islineData;
    }

    private String timeout;
    private boolean stopOnEmptyTopic;
    private String dataformate;
    private boolean islineData;
    public static String[] getKafkaPropertiesNames() {
        return KAFKA_PROPERTIES_NAMES;
    }

    public static Map<String, String> getKafkaPropertiesDefaults() {
        return KAFKA_PROPERTIES_DEFAULTS;
    }

    public KafkaConsumerMeta() {
        super();
    }

    public Properties getKafkaProperties() {
        return kafkaProperties;
    }

    @SuppressWarnings("unused")
    public Map getKafkaPropertiesMap() {
        return getKafkaProperties();
    }

    public void setKafkaProperties(Properties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @SuppressWarnings("unused")
    public void setKafkaPropertiesMap(Map<String, String> propertiesMap) {
        Properties props = new Properties();
        props.putAll(propertiesMap);
        setKafkaProperties(props);
    }

    public Map<String, Integer> getMap() {
        return map;
    }

    /**
     * @return Kafka topic name
     */
    public String getTopic() {
        return topic;
    }

    /**
     * @param topic Kafka topic name
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    /**
     * @return Target field name in Kettle stream
     */
    public String getField() {
        return field;
    }

    /**
     * @param field Target field name in Kettle stream
     */
    public void setField(String field) {
        this.field = field;
    }

    /**
     * @return Target key field name in Kettle stream
     */
    public String getKeyField() {
        return keyField;
    }

    /**
     * @param keyField Target key field name in Kettle stream
     */
    public void setKeyField(String keyField) {
        this.keyField = keyField;
    }

    /**
     * @return Limit number of entries to read from Kafka queue
     */
    public String getLimit() {
        return limit;
    }

    /**
     * @param limit Limit number of entries to read from Kafka queue
     */
    public void setLimit(String limit) {
        this.limit = limit;
    }

    /**
     * @return Time limit for reading entries from Kafka queue (in ms)
     */
    public String getTimeout() {
        return timeout;
    }

    /**
     * @param timeout Time limit for reading entries from Kafka queue (in ms)
     */
    public void setTimeout(String timeout) {
        this.timeout = timeout;
    }

    /**
     * @return 'true' if the consumer should stop when no more messages are
     * available
     */
    public boolean isStopOnEmptyTopic() {
        return stopOnEmptyTopic;
    }

    /**
     * @param stopOnEmptyTopic If 'true', stop the consumer when no more messages are
     *                         available on the topic
     */
    public void setStopOnEmptyTopic(boolean stopOnEmptyTopic) {
        this.stopOnEmptyTopic = stopOnEmptyTopic;
    }

    public void check(List<CheckResultInterface> remarks, TransMeta transMeta, StepMeta stepMeta, RowMetaInterface prev,
                      String[] input, String[] output, RowMetaInterface info, VariableSpace space, Repository repository,
                      IMetaStore metaStore) {

        if (topic == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    BaseMessages.getString( PKG,"KafkaConsumerMeta.Check.InvalidTopic"), stepMeta));
        }
        if (field == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    BaseMessages.getString( PKG,"KafkaConsumerMeta.Check.InvalidField"), stepMeta));
        }
        if (keyField == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    BaseMessages.getString( PKG,"KafkaConsumerMeta.Check.InvalidKeyField"), stepMeta));
        }
        if (pollMS == null) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR,
                    BaseMessages.getString( PKG,"KafkaConsumerMeta.Check.InvalidKeyField"), stepMeta));


        }

        if(partition !=null){
            try {
                Long.parseLong(partition);
            }catch (Exception e){
                throw new  RuntimeException (e);
            }
        }else{
            partition = "0";
        }



        if(offset !=null){
            try {
                Long.parseLong(offset);
            }catch (Exception e){
                throw new  RuntimeException (e);
            }
        }else{
            offset = "0";
        }
        if(pollMS !=null){
            try {
                Long.parseLong(pollMS);
            }catch (Exception e){
                throw new  RuntimeException (e);
            }
        }
        try {
            new ConsumerConfig(kafkaProperties);
        } catch (IllegalArgumentException e) {
            remarks.add(new CheckResult(CheckResultInterface.TYPE_RESULT_ERROR, e.getMessage(), stepMeta));
        }
    }

    public StepInterface getStep(StepMeta stepMeta, StepDataInterface stepDataInterface, int cnr, TransMeta transMeta,
                                 Trans trans) {
        return new KafkaConsumer(stepMeta, stepDataInterface, cnr, transMeta, trans);
    }

    public StepDataInterface getStepData() {
        return new KafkaConsumerData();
    }

    @Override
    public void loadXML(Node stepnode, List<DatabaseMeta> databases, IMetaStore metaStore)
            throws KettleXMLException {

        try {
            offset = XMLHandler.getTagValue(stepnode, ATTR_OFFSET);
            partition = XMLHandler.getTagValue(stepnode, ATTR_PARTITON);
            pollMS = XMLHandler.getTagValue(stepnode, ATTR_POLL);
            topic = XMLHandler.getTagValue(stepnode, ATTR_TOPIC);
            field = XMLHandler.getTagValue(stepnode, ATTR_FIELD);
            String mapstring = XMLHandler.getTagValue(stepnode, ATTR_MAP);
            try {
                map = (Map<String, Integer>) JSONObject.parse(mapstring);
            }catch (Exception e){
                e.printStackTrace();
            }
            keyField = XMLHandler.getTagValue(stepnode, ATTR_KEY_FIELD);
            limit = XMLHandler.getTagValue(stepnode, ATTR_LIMIT);
            timeout = XMLHandler.getTagValue(stepnode, ATTR_TIMEOUT);
            dataformate = XMLHandler.getTagValue(stepnode, ATTR_DATAFORMATE);
            islineData = XMLHandler.getTagValue(stepnode, ATTR_LINEDATA)!=null;
            // This tag only exists if the value is "true", so we can directly
            // populate the field
            stopOnEmptyTopic = XMLHandler.getTagValue(stepnode, ATTR_STOP_ON_EMPTY_TOPIC) != null;
            Node kafkaNode = XMLHandler.getSubNode(stepnode, ATTR_KAFKA);
            String[] kafkaElements = XMLHandler.getNodeElements(kafkaNode);
            if (kafkaElements != null) {
                for (String propName : kafkaElements) {
                    String value = XMLHandler.getTagValue(kafkaNode, propName);
                    if (value != null) {
                        kafkaProperties.put(propName, value);
                    }
                }
            }
        } catch (Exception e) {
            throw new KettleXMLException(BaseMessages.getString( PKG,"KafkaConsumerMeta.Exception.loadXml"), e);
        }
    }

    @Override
    public String getXML() throws KettleException {
        StringBuilder retval = new StringBuilder();
        if (topic != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_TOPIC, topic));
        }
        if (partition != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_PARTITON, partition));
        }
        if (field != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_FIELD, field));
        }
        if (map != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_MAP, JSONObject.toJSONString(map)));
        }
        if (keyField != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_KEY_FIELD, keyField));
        }
        if (limit != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_LIMIT, limit));
        }
        if (timeout != null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_TIMEOUT, timeout));
        }
        if (offset != null) {
            try {
                Long.parseLong(offset);
            }catch (Exception e){
                throw new  KettleException (e);
            }
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_OFFSET, offset));
        }

        if (pollMS != null) {
            try {
                Long.parseLong(pollMS);
            }catch (Exception e){
                throw new  KettleException (e);
            }
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_POLL, pollMS));
        }
        if (stopOnEmptyTopic) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_STOP_ON_EMPTY_TOPIC, "true"));


        }
        if (dataformate!=null) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_DATAFORMATE, dataformate));
        }
        if (islineData) {
            retval.append("    ").append(XMLHandler.addTagValue(ATTR_LINEDATA, "true"));
        }
        retval.append("    ").append(XMLHandler.openTag(ATTR_KAFKA)).append(Const.CR);
        for (String name : kafkaProperties.stringPropertyNames()) {
            String value = kafkaProperties.getProperty(name);
            if (value != null) {
                retval.append("      ").append(XMLHandler.addTagValue(name, value));
            }
        }
        retval.append("    ").append(XMLHandler.closeTag(ATTR_KAFKA)).append(Const.CR);
        return retval.toString();
    }

    @Override
    public void readRep(Repository rep, IMetaStore metaStore, ObjectId stepId, List<DatabaseMeta> databases)
            throws KettleException {
        try {
            partition = rep.getStepAttributeString(stepId, ATTR_PARTITON);
            topic = rep.getStepAttributeString(stepId, ATTR_TOPIC);
            pollMS = rep.getStepAttributeString(stepId, ATTR_POLL);
            offset = rep.getStepAttributeString(stepId, ATTR_OFFSET);
            field = rep.getStepAttributeString(stepId, ATTR_FIELD);
            String mapstring = rep.getStepAttributeString(stepId, ATTR_MAP);
            try {
                map = (Map<String, Integer>) JSONObject.parse(mapstring);
            }catch (Exception e){
                e.printStackTrace();
            }
            keyField = rep.getStepAttributeString(stepId, ATTR_KEY_FIELD);
            limit = rep.getStepAttributeString(stepId, ATTR_LIMIT);
            timeout = rep.getStepAttributeString(stepId, ATTR_TIMEOUT);
            stopOnEmptyTopic = rep.getStepAttributeBoolean(stepId, ATTR_STOP_ON_EMPTY_TOPIC);
            dataformate = rep.getStepAttributeString(stepId, ATTR_DATAFORMATE);
            islineData = rep.getStepAttributeBoolean(stepId, ATTR_LINEDATA);
            String kafkaPropsXML = rep.getStepAttributeString(stepId, ATTR_KAFKA);
            if (kafkaPropsXML != null) {
                kafkaProperties.loadFromXML(new ByteArrayInputStream(kafkaPropsXML.getBytes()));
            }
            // Support old versions:
            for (String name : KAFKA_PROPERTIES_NAMES) {
                String value = rep.getStepAttributeString(stepId, name);
                if (value != null) {
                    kafkaProperties.put(name, value);
                }
            }
        } catch (Exception e) {
            throw new KettleException("KafkaConsumerMeta.Exception.loadRep", e);
        }
    }

    @Override
    public void saveRep(Repository rep, IMetaStore metaStore, ObjectId transformationId, ObjectId stepId) throws KettleException {
        try {
            if (partition != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_PARTITON, partition);
            }
            if (topic != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_TOPIC, topic);
            }
            if (field != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_FIELD, field);
            }
            if (map != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_MAP, JSONObject.toJSONString(map));
            }
            if (keyField != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_KEY_FIELD, keyField);
            }
            if (limit != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_LIMIT, limit);
            }
            if (timeout != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_TIMEOUT, timeout);
            }
            if (offset != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_OFFSET, offset);
            }
            if (pollMS != null) {
                rep.saveStepAttribute(transformationId, stepId, ATTR_POLL, pollMS);
            }
            if(dataformate!=null){
                rep.saveStepAttribute(transformationId, stepId, ATTR_DATAFORMATE, dataformate);
            }
            rep.saveStepAttribute(transformationId, stepId, ATTR_LINEDATA, islineData);
            rep.saveStepAttribute(transformationId, stepId, ATTR_STOP_ON_EMPTY_TOPIC, stopOnEmptyTopic);

            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            kafkaProperties.storeToXML(buf, null);
            rep.saveStepAttribute(transformationId, stepId, ATTR_KAFKA, buf.toString());
        } catch (Exception e) {
            throw new KettleException("KafkaConsumerMeta.Exception.saveRep", e);
        }
    }
    public ConsumerRecords<String, String> getRecords(org.apache.kafka.clients.consumer.KafkaConsumer consumer){
        ConsumerRecords<String, String> records = null;
        consumer.assign(Arrays.asList( new TopicPartition(topic, Integer.parseInt(partition))));
        consumer.seek(new TopicPartition(topic, Integer.parseInt(partition)),Long.parseLong(offset));
        int looptime = 0;
        while (looptime<=1){
            try {
                records = consumer.poll(20000L);
            }catch (Exception e){
                e.printStackTrace();
            }
            if(records!=null&&records.count()!=0){
                break;
            }
            looptime+=1;
        }
        allloopTime+=1;
        if(allloopTime>10){
            return null;
        }
        if(records==null || records.isEmpty()){
            offset  = String.valueOf(Integer.parseInt(offset)+1);
            return getRecords(consumer);
        }else{
            return records;
        }
    }
    /**
     * Set default values to the transformation
     */
    public void setDefault() {
        setTopic("");
    }

    public void getFields(RowMetaInterface rowMeta, String origin, RowMetaInterface[] info, StepMeta nextStep,
                          VariableSpace space, Repository repository, IMetaStore metaStore) throws KettleStepException {

        try {


            if(dataformate!=null&&!formateTye[0].equals(dataformate)&&!"".equals(dataformate)){
                if(map.isEmpty()) {
                    Properties pro = getKafkaProperties();
                    Properties p = new Properties();
                    for(Object k:pro.keySet()){
                       p.put(k.toString(),pro.get(k.toString()));
                    }
                    p.put("auto.offset.reset","earliest");
                    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                    p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
                    p.put("partition.assignment.strategy","org.apache.kafka.clients.consumer.RangeAssignor");
                    org.apache.kafka.clients.consumer.KafkaConsumer consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(p);
                    ConsumerRecords<String, String> records =  getRecords(consumer);
                    map = new TreeMap<String, Integer>();
                    if (records != null) {
                        for (ConsumerRecord<String, String> record : records) {
                            try {
                                if (formateTye[1].equals(dataformate)) {
                                    JSONObject jsonObject = null;
                                    try {
                                        jsonObject = JSONObject.parseObject(record.value());
                                    } catch (Exception e) {
                                        offset = String.valueOf(Integer.parseInt(offset) + 1);
                                    }
                                    Object data = null;
                                    Object data2 = null;
                                    JSONObject jb = null;
                                    for (String key : jsonObject.keySet()) {
                                        data = jsonObject.get(key);
                                        if (data instanceof Integer) {
                                            map.put(key, ValueMetaInterface.TYPE_INTEGER);
                                        } else if (data instanceof Long) {
                                            map.put(key, ValueMetaInterface.TYPE_INTEGER);
                                        } else if (data instanceof java.math.BigDecimal) {
                                            map.put(key, ValueMetaInterface.TYPE_BIGNUMBER);
                                        } else if (data instanceof JSONArray) {
                                            map.put(key, ValueMetaInterface.TYPE_STRING);
                                        } else if (data instanceof String) {
                                            map.put(key, ValueMetaInterface.TYPE_STRING);
                                        } else if (data instanceof JSONObject) {
                                            if (islineData) {
                                                jb = (JSONObject) data;
                                                for (String key2 : jb.keySet()) {
                                                    data2 = jb.get(key2);
                                                    if (data2 instanceof Integer) {
                                                        map.put(key + MIDDLE + key2, ValueMetaInterface.TYPE_INTEGER);
                                                    } else if (data2 instanceof Long) {
                                                        map.put(key + MIDDLE + key2, ValueMetaInterface.TYPE_INTEGER);
                                                    } else if (data2 instanceof java.math.BigDecimal) {
                                                        map.put(key + MIDDLE + key2, ValueMetaInterface.TYPE_BIGNUMBER);
                                                    } else if (data2 instanceof String) {
                                                        map.put(key + MIDDLE + key2, ValueMetaInterface.TYPE_STRING);
                                                    } else {
                                                        map.put(key + MIDDLE + key2, ValueMetaInterface.TYPE_STRING);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    if (data != null) {
                                        break;
                                    }
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    } else {
                        throw new RuntimeException("KAFKA数据为空");
                    }
                }

                ValueMetaInterface fieldValueMeta = null;
                for(String key:map.keySet()){
                    fieldValueMeta = ValueMetaFactory.createValueMeta(key, map.get(key));
                    fieldValueMeta.setOrigin(origin);
                    rowMeta.addValueMeta(fieldValueMeta);
                }

                fieldValueMeta = ValueMetaFactory.createValueMeta(KafkaConsumerMeta.CDC_FLAG, ValueMetaInterface.TYPE_INTEGER);
                fieldValueMeta.setOrigin(origin);
                rowMeta.addValueMeta(fieldValueMeta);
                fieldValueMeta = ValueMetaFactory.createValueMeta(KafkaConsumerMeta.CDC_TIMESTAMP, ValueMetaInterface.TYPE_TIMESTAMP);
                fieldValueMeta.setOrigin(origin);
                rowMeta.addValueMeta(fieldValueMeta);

            }else{
                ValueMetaInterface fieldValueMeta = ValueMetaFactory.createValueMeta(getField(), ValueMetaInterface.TYPE_BINARY);
                fieldValueMeta.setOrigin(origin);
                rowMeta.addValueMeta(fieldValueMeta);

                ValueMetaInterface keyFieldValueMeta = ValueMetaFactory.createValueMeta(getKeyField(), ValueMetaInterface.TYPE_BINARY);
                keyFieldValueMeta.setOrigin(origin);
                rowMeta.addValueMeta(keyFieldValueMeta);

                ValueMetaInterface cdcfieldValueMeta = ValueMetaFactory.createValueMeta(KafkaConsumerMeta.CDC_FLAG, ValueMetaInterface.TYPE_INTEGER);
                cdcfieldValueMeta.setOrigin(origin);
                rowMeta.addValueMeta(cdcfieldValueMeta);
                ValueMetaInterface cdctimedValueMeta = ValueMetaFactory.createValueMeta(KafkaConsumerMeta.CDC_TIMESTAMP, ValueMetaInterface.TYPE_TIMESTAMP);
                cdctimedValueMeta.setOrigin(origin);
                rowMeta.addValueMeta(cdctimedValueMeta);
            }
        } catch (KettlePluginException e) {
            throw new KettleStepException("KafkaConsumerMeta.Exception.getFields", e);
        }

    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

}
