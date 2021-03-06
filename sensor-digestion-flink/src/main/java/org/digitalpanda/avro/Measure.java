/**
 * Autogenerated by Avro
 *
 * DO NOT EDIT DIRECTLY
 */
package org.digitalpanda.avro;

import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.specific.SpecificData;

@org.apache.avro.specific.AvroGenerated
public class Measure extends org.apache.avro.specific.SpecificRecordBase implements org.apache.avro.specific.SpecificRecord {
  private static final long serialVersionUID = -410440954091346320L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"record\",\"name\":\"Measure\",\"namespace\":\"org.digitalpanda.avro\",\"fields\":[{\"name\":\"location\",\"type\":\"string\"},{\"name\":\"timeBlockId\",\"type\":\"long\"},{\"name\":\"measureType\",\"type\":{\"type\":\"enum\",\"name\":\"MeasureType\",\"symbols\":[\"TEMPERATURE\",\"HUMIDITY\",\"PRESSURE\"]}},{\"name\":\"bucket\",\"type\":[\"int\",\"null\"],\"default\":0},{\"name\":\"timestamp\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},{\"name\":\"value\",\"type\":\"double\"}]}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }

  private static SpecificData MODEL$ = new SpecificData();
static {
    MODEL$.addLogicalTypeConversion(new org.apache.avro.data.TimeConversions.TimestampMillisConversion());
  }

  private static final BinaryMessageEncoder<Measure> ENCODER =
      new BinaryMessageEncoder<Measure>(MODEL$, SCHEMA$);

  private static final BinaryMessageDecoder<Measure> DECODER =
      new BinaryMessageDecoder<Measure>(MODEL$, SCHEMA$);

  /**
   * Return the BinaryMessageEncoder instance used by this class.
   * @return the message encoder used by this class
   */
  public static BinaryMessageEncoder<Measure> getEncoder() {
    return ENCODER;
  }

  /**
   * Return the BinaryMessageDecoder instance used by this class.
   * @return the message decoder used by this class
   */
  public static BinaryMessageDecoder<Measure> getDecoder() {
    return DECODER;
  }

  /**
   * Create a new BinaryMessageDecoder instance for this class that uses the specified {@link SchemaStore}.
   * @param resolver a {@link SchemaStore} used to find schemas by fingerprint
   * @return a BinaryMessageDecoder instance for this class backed by the given SchemaStore
   */
  public static BinaryMessageDecoder<Measure> createDecoder(SchemaStore resolver) {
    return new BinaryMessageDecoder<Measure>(MODEL$, SCHEMA$, resolver);
  }

  /**
   * Serializes this Measure to a ByteBuffer.
   * @return a buffer holding the serialized data for this instance
   * @throws java.io.IOException if this instance could not be serialized
   */
  public java.nio.ByteBuffer toByteBuffer() throws java.io.IOException {
    return ENCODER.encode(this);
  }

  /**
   * Deserializes a Measure from a ByteBuffer.
   * @param b a byte buffer holding serialized data for an instance of this class
   * @return a Measure instance decoded from the given buffer
   * @throws java.io.IOException if the given bytes could not be deserialized into an instance of this class
   */
  public static Measure fromByteBuffer(
      java.nio.ByteBuffer b) throws java.io.IOException {
    return DECODER.decode(b);
  }

   private java.lang.CharSequence location;
   private long timeBlockId;
   private org.digitalpanda.avro.MeasureType measureType;
   private java.lang.Integer bucket;
   private java.time.Instant timestamp;
   private double value;

  /**
   * Default constructor.  Note that this does not initialize fields
   * to their default values from the schema.  If that is desired then
   * one should use <code>newBuilder()</code>.
   */
  public Measure() {}

  /**
   * All-args constructor.
   * @param location The new value for location
   * @param timeBlockId The new value for timeBlockId
   * @param measureType The new value for measureType
   * @param bucket The new value for bucket
   * @param timestamp The new value for timestamp
   * @param value The new value for value
   */
  public Measure(java.lang.CharSequence location, java.lang.Long timeBlockId, org.digitalpanda.avro.MeasureType measureType, java.lang.Integer bucket, java.time.Instant timestamp, java.lang.Double value) {
    this.location = location;
    this.timeBlockId = timeBlockId;
    this.measureType = measureType;
    this.bucket = bucket;
    this.timestamp = timestamp.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
    this.value = value;
  }

  public org.apache.avro.specific.SpecificData getSpecificData() { return MODEL$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }
  // Used by DatumWriter.  Applications should not call.
  public java.lang.Object get(int field$) {
    switch (field$) {
    case 0: return location;
    case 1: return timeBlockId;
    case 2: return measureType;
    case 3: return bucket;
    case 4: return timestamp;
    case 5: return value;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  private static final org.apache.avro.Conversion<?>[] conversions =
      new org.apache.avro.Conversion<?>[] {
      null,
      null,
      null,
      null,
      new org.apache.avro.data.TimeConversions.TimestampMillisConversion(),
      null,
      null
  };

  @Override
  public org.apache.avro.Conversion<?> getConversion(int field) {
    return conversions[field];
  }

  // Used by DatumReader.  Applications should not call.
  @SuppressWarnings(value="unchecked")
  public void put(int field$, java.lang.Object value$) {
    switch (field$) {
    case 0: location = (java.lang.CharSequence)value$; break;
    case 1: timeBlockId = (java.lang.Long)value$; break;
    case 2: measureType = (org.digitalpanda.avro.MeasureType)value$; break;
    case 3: bucket = (java.lang.Integer)value$; break;
    case 4: timestamp = (java.time.Instant)value$; break;
    case 5: value = (java.lang.Double)value$; break;
    default: throw new org.apache.avro.AvroRuntimeException("Bad index");
    }
  }

  /**
   * Gets the value of the 'location' field.
   * @return The value of the 'location' field.
   */
  public java.lang.CharSequence getLocation() {
    return location;
  }


  /**
   * Sets the value of the 'location' field.
   * @param value the value to set.
   */
  public void setLocation(java.lang.CharSequence value) {
    this.location = value;
  }

  /**
   * Gets the value of the 'timeBlockId' field.
   * @return The value of the 'timeBlockId' field.
   */
  public long getTimeBlockId() {
    return timeBlockId;
  }


  /**
   * Sets the value of the 'timeBlockId' field.
   * @param value the value to set.
   */
  public void setTimeBlockId(long value) {
    this.timeBlockId = value;
  }

  /**
   * Gets the value of the 'measureType' field.
   * @return The value of the 'measureType' field.
   */
  public org.digitalpanda.avro.MeasureType getMeasureType() {
    return measureType;
  }


  /**
   * Sets the value of the 'measureType' field.
   * @param value the value to set.
   */
  public void setMeasureType(org.digitalpanda.avro.MeasureType value) {
    this.measureType = value;
  }

  /**
   * Gets the value of the 'bucket' field.
   * @return The value of the 'bucket' field.
   */
  public java.lang.Integer getBucket() {
    return bucket;
  }


  /**
   * Sets the value of the 'bucket' field.
   * @param value the value to set.
   */
  public void setBucket(java.lang.Integer value) {
    this.bucket = value;
  }

  /**
   * Gets the value of the 'timestamp' field.
   * @return The value of the 'timestamp' field.
   */
  public java.time.Instant getTimestamp() {
    return timestamp;
  }


  /**
   * Sets the value of the 'timestamp' field.
   * @param value the value to set.
   */
  public void setTimestamp(java.time.Instant value) {
    this.timestamp = value.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
  }

  /**
   * Gets the value of the 'value' field.
   * @return The value of the 'value' field.
   */
  public double getValue() {
    return value;
  }


  /**
   * Sets the value of the 'value' field.
   * @param value the value to set.
   */
  public void setValue(double value) {
    this.value = value;
  }

  /**
   * Creates a new Measure RecordBuilder.
   * @return A new Measure RecordBuilder
   */
  public static org.digitalpanda.avro.Measure.Builder newBuilder() {
    return new org.digitalpanda.avro.Measure.Builder();
  }

  /**
   * Creates a new Measure RecordBuilder by copying an existing Builder.
   * @param other The existing builder to copy.
   * @return A new Measure RecordBuilder
   */
  public static org.digitalpanda.avro.Measure.Builder newBuilder(org.digitalpanda.avro.Measure.Builder other) {
    if (other == null) {
      return new org.digitalpanda.avro.Measure.Builder();
    } else {
      return new org.digitalpanda.avro.Measure.Builder(other);
    }
  }

  /**
   * Creates a new Measure RecordBuilder by copying an existing Measure instance.
   * @param other The existing instance to copy.
   * @return A new Measure RecordBuilder
   */
  public static org.digitalpanda.avro.Measure.Builder newBuilder(org.digitalpanda.avro.Measure other) {
    if (other == null) {
      return new org.digitalpanda.avro.Measure.Builder();
    } else {
      return new org.digitalpanda.avro.Measure.Builder(other);
    }
  }

  /**
   * RecordBuilder for Measure instances.
   */
  public static class Builder extends org.apache.avro.specific.SpecificRecordBuilderBase<Measure>
    implements org.apache.avro.data.RecordBuilder<Measure> {

    private java.lang.CharSequence location;
    private long timeBlockId;
    private org.digitalpanda.avro.MeasureType measureType;
    private java.lang.Integer bucket;
    private java.time.Instant timestamp;
    private double value;

    /** Creates a new Builder */
    private Builder() {
      super(SCHEMA$);
    }

    /**
     * Creates a Builder by copying an existing Builder.
     * @param other The existing Builder to copy.
     */
    private Builder(org.digitalpanda.avro.Measure.Builder other) {
      super(other);
      if (isValidValue(fields()[0], other.location)) {
        this.location = data().deepCopy(fields()[0].schema(), other.location);
        fieldSetFlags()[0] = other.fieldSetFlags()[0];
      }
      if (isValidValue(fields()[1], other.timeBlockId)) {
        this.timeBlockId = data().deepCopy(fields()[1].schema(), other.timeBlockId);
        fieldSetFlags()[1] = other.fieldSetFlags()[1];
      }
      if (isValidValue(fields()[2], other.measureType)) {
        this.measureType = data().deepCopy(fields()[2].schema(), other.measureType);
        fieldSetFlags()[2] = other.fieldSetFlags()[2];
      }
      if (isValidValue(fields()[3], other.bucket)) {
        this.bucket = data().deepCopy(fields()[3].schema(), other.bucket);
        fieldSetFlags()[3] = other.fieldSetFlags()[3];
      }
      if (isValidValue(fields()[4], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[4].schema(), other.timestamp);
        fieldSetFlags()[4] = other.fieldSetFlags()[4];
      }
      if (isValidValue(fields()[5], other.value)) {
        this.value = data().deepCopy(fields()[5].schema(), other.value);
        fieldSetFlags()[5] = other.fieldSetFlags()[5];
      }
    }

    /**
     * Creates a Builder by copying an existing Measure instance
     * @param other The existing instance to copy.
     */
    private Builder(org.digitalpanda.avro.Measure other) {
      super(SCHEMA$);
      if (isValidValue(fields()[0], other.location)) {
        this.location = data().deepCopy(fields()[0].schema(), other.location);
        fieldSetFlags()[0] = true;
      }
      if (isValidValue(fields()[1], other.timeBlockId)) {
        this.timeBlockId = data().deepCopy(fields()[1].schema(), other.timeBlockId);
        fieldSetFlags()[1] = true;
      }
      if (isValidValue(fields()[2], other.measureType)) {
        this.measureType = data().deepCopy(fields()[2].schema(), other.measureType);
        fieldSetFlags()[2] = true;
      }
      if (isValidValue(fields()[3], other.bucket)) {
        this.bucket = data().deepCopy(fields()[3].schema(), other.bucket);
        fieldSetFlags()[3] = true;
      }
      if (isValidValue(fields()[4], other.timestamp)) {
        this.timestamp = data().deepCopy(fields()[4].schema(), other.timestamp);
        fieldSetFlags()[4] = true;
      }
      if (isValidValue(fields()[5], other.value)) {
        this.value = data().deepCopy(fields()[5].schema(), other.value);
        fieldSetFlags()[5] = true;
      }
    }

    /**
      * Gets the value of the 'location' field.
      * @return The value.
      */
    public java.lang.CharSequence getLocation() {
      return location;
    }


    /**
      * Sets the value of the 'location' field.
      * @param value The value of 'location'.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder setLocation(java.lang.CharSequence value) {
      validate(fields()[0], value);
      this.location = value;
      fieldSetFlags()[0] = true;
      return this;
    }

    /**
      * Checks whether the 'location' field has been set.
      * @return True if the 'location' field has been set, false otherwise.
      */
    public boolean hasLocation() {
      return fieldSetFlags()[0];
    }


    /**
      * Clears the value of the 'location' field.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder clearLocation() {
      location = null;
      fieldSetFlags()[0] = false;
      return this;
    }

    /**
      * Gets the value of the 'timeBlockId' field.
      * @return The value.
      */
    public long getTimeBlockId() {
      return timeBlockId;
    }


    /**
      * Sets the value of the 'timeBlockId' field.
      * @param value The value of 'timeBlockId'.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder setTimeBlockId(long value) {
      validate(fields()[1], value);
      this.timeBlockId = value;
      fieldSetFlags()[1] = true;
      return this;
    }

    /**
      * Checks whether the 'timeBlockId' field has been set.
      * @return True if the 'timeBlockId' field has been set, false otherwise.
      */
    public boolean hasTimeBlockId() {
      return fieldSetFlags()[1];
    }


    /**
      * Clears the value of the 'timeBlockId' field.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder clearTimeBlockId() {
      fieldSetFlags()[1] = false;
      return this;
    }

    /**
      * Gets the value of the 'measureType' field.
      * @return The value.
      */
    public org.digitalpanda.avro.MeasureType getMeasureType() {
      return measureType;
    }


    /**
      * Sets the value of the 'measureType' field.
      * @param value The value of 'measureType'.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder setMeasureType(org.digitalpanda.avro.MeasureType value) {
      validate(fields()[2], value);
      this.measureType = value;
      fieldSetFlags()[2] = true;
      return this;
    }

    /**
      * Checks whether the 'measureType' field has been set.
      * @return True if the 'measureType' field has been set, false otherwise.
      */
    public boolean hasMeasureType() {
      return fieldSetFlags()[2];
    }


    /**
      * Clears the value of the 'measureType' field.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder clearMeasureType() {
      measureType = null;
      fieldSetFlags()[2] = false;
      return this;
    }

    /**
      * Gets the value of the 'bucket' field.
      * @return The value.
      */
    public java.lang.Integer getBucket() {
      return bucket;
    }


    /**
      * Sets the value of the 'bucket' field.
      * @param value The value of 'bucket'.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder setBucket(java.lang.Integer value) {
      validate(fields()[3], value);
      this.bucket = value;
      fieldSetFlags()[3] = true;
      return this;
    }

    /**
      * Checks whether the 'bucket' field has been set.
      * @return True if the 'bucket' field has been set, false otherwise.
      */
    public boolean hasBucket() {
      return fieldSetFlags()[3];
    }


    /**
      * Clears the value of the 'bucket' field.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder clearBucket() {
      bucket = null;
      fieldSetFlags()[3] = false;
      return this;
    }

    /**
      * Gets the value of the 'timestamp' field.
      * @return The value.
      */
    public java.time.Instant getTimestamp() {
      return timestamp;
    }


    /**
      * Sets the value of the 'timestamp' field.
      * @param value The value of 'timestamp'.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder setTimestamp(java.time.Instant value) {
      validate(fields()[4], value);
      this.timestamp = value.truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
      fieldSetFlags()[4] = true;
      return this;
    }

    /**
      * Checks whether the 'timestamp' field has been set.
      * @return True if the 'timestamp' field has been set, false otherwise.
      */
    public boolean hasTimestamp() {
      return fieldSetFlags()[4];
    }


    /**
      * Clears the value of the 'timestamp' field.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder clearTimestamp() {
      fieldSetFlags()[4] = false;
      return this;
    }

    /**
      * Gets the value of the 'value' field.
      * @return The value.
      */
    public double getValue() {
      return value;
    }


    /**
      * Sets the value of the 'value' field.
      * @param value The value of 'value'.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder setValue(double value) {
      validate(fields()[5], value);
      this.value = value;
      fieldSetFlags()[5] = true;
      return this;
    }

    /**
      * Checks whether the 'value' field has been set.
      * @return True if the 'value' field has been set, false otherwise.
      */
    public boolean hasValue() {
      return fieldSetFlags()[5];
    }


    /**
      * Clears the value of the 'value' field.
      * @return This builder.
      */
    public org.digitalpanda.avro.Measure.Builder clearValue() {
      fieldSetFlags()[5] = false;
      return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Measure build() {
      try {
        Measure record = new Measure();
        record.location = fieldSetFlags()[0] ? this.location : (java.lang.CharSequence) defaultValue(fields()[0]);
        record.timeBlockId = fieldSetFlags()[1] ? this.timeBlockId : (java.lang.Long) defaultValue(fields()[1]);
        record.measureType = fieldSetFlags()[2] ? this.measureType : (org.digitalpanda.avro.MeasureType) defaultValue(fields()[2]);
        record.bucket = fieldSetFlags()[3] ? this.bucket : (java.lang.Integer) defaultValue(fields()[3]);
        record.timestamp = fieldSetFlags()[4] ? this.timestamp : (java.time.Instant) defaultValue(fields()[4]);
        record.value = fieldSetFlags()[5] ? this.value : (java.lang.Double) defaultValue(fields()[5]);
        return record;
      } catch (org.apache.avro.AvroMissingFieldException e) {
        throw e;
      } catch (java.lang.Exception e) {
        throw new org.apache.avro.AvroRuntimeException(e);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumWriter<Measure>
    WRITER$ = (org.apache.avro.io.DatumWriter<Measure>)MODEL$.createDatumWriter(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, SpecificData.getEncoder(out));
  }

  @SuppressWarnings("unchecked")
  private static final org.apache.avro.io.DatumReader<Measure>
    READER$ = (org.apache.avro.io.DatumReader<Measure>)MODEL$.createDatumReader(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, SpecificData.getDecoder(in));
  }

}










