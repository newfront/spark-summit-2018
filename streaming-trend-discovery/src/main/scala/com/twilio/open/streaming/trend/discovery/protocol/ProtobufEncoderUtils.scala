package com.twilio.open.streaming.trend.discovery.protocol

import java.lang.{Iterable => JIterable}
import java.lang.reflect.{Method, Type}
import java.util.{Iterator => JIterator, List => JList, Map => JMap}

import scala.language.existentials
import scala.reflect._

import org.apache.spark.sql.catalyst.{ InternalRow, ScalaReflection }
import org.apache.spark.sql.catalyst.analysis.{ GetColumnByOrdinal, UnresolvedAttribute, UnresolvedExtractValue }
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{ BoundReference, CreateNamedStruct, Expression, IsNull, NonSQLExpression }
import org.apache.spark.sql.catalyst.expressions.codegen.{ CodegenContext, ExprCode }
import org.apache.spark.sql.catalyst.expressions.objects.{ AssertNotNull, ExternalMapToCatalyst, Invoke, MapObjects, NewInstance, StaticInvoke }
import org.apache.spark.sql.catalyst.util.{ ArrayBasedMapData, DateTimeUtils, GenericArrayData }
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.google.common.reflect.TypeToken
import com.google.protobuf.Descriptors.{ Descriptor, FieldDescriptor }
import com.google.protobuf.Message

/**
  * Utilities for protobuf expression encoders.
  * Based on Spark's JavaTypeInference class
  * @see https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/JavaTypeInference.scala
  */
object ProtobufEncoderUtils {

  /**
    * Provide ExpressionEncoder for a given protbuf message type.
    * @tparam T protobuf message type
    * @return ExpressionEncoder for the type
    */
  def protobufMessageEncoder[T <: Message : ClassTag]: ExpressionEncoder[T] = {
    val clazz = classTag[T].runtimeClass
    new ExpressionEncoder[T](
      schema = inferDataType(clazz)._1.asInstanceOf[StructType],
      flat = false,
      serializer = serializerFor(clazz).flatten,
      deserializer = deserializerFor(clazz),
      clsTag = classTag[T]
    )
  }

  private val iterableType = TypeToken.of(classOf[JIterable[_]])
  private val mapType = TypeToken.of(classOf[JMap[_, _]])
  private val listType = TypeToken.of(classOf[JList[_]])
  private val iteratorReturnType = classOf[JIterable[_]].getMethod("iterator").getGenericReturnType
  private val nextReturnType = classOf[JIterator[_]].getMethod("next").getGenericReturnType
  private val keySetReturnType = classOf[JMap[_, _]].getMethod("keySet").getGenericReturnType
  private val valuesReturnType = classOf[JMap[_, _]].getMethod("values").getGenericReturnType

  /**
    * Infers the corresponding SQL data type of a Message class.
    * @return (SQL data type, nullable)
    */
  def inferDataType(dataType: Class[_]): (DataType, Boolean) = {
    inferDataType(TypeToken.of(dataType))
  }

  private def descriptorFor(clazz : Class[_]): Descriptor = {
    val method = clazz.getMethod("getDescriptor")
    method.invoke(clazz).asInstanceOf[Descriptor]
  }


  /**
    * Infers the corresponding SQL data type of a Java type.
    * @param typeToken Java type
    * @return (SQL data type, nullable)
    */
  private def inferDataType(typeToken: TypeToken[_], seenTypeSet: Set[Class[_]] = Set.empty)
  : (DataType, Boolean) = {
    typeToken.getRawType match {
      case c: Class[_] if c == classOf[java.lang.String] => (StringType, true)
      case c: Class[_] if c == classOf[Array[Byte]] => (BinaryType, true)

      case c: Class[_] if c == java.lang.Short.TYPE => (ShortType, true)
      case c: Class[_] if c == java.lang.Integer.TYPE => (IntegerType, true)
      case c: Class[_] if c == java.lang.Long.TYPE => (LongType, true)
      case c: Class[_] if c == java.lang.Double.TYPE => (DoubleType, true)
      case c: Class[_] if c == java.lang.Byte.TYPE => (ByteType, true)
      case c: Class[_] if c == java.lang.Float.TYPE => (FloatType, true)
      case c: Class[_] if c == java.lang.Boolean.TYPE => (BooleanType, true)

      case c: Class[_] if c == classOf[java.lang.Short] => (ShortType, true)
      case c: Class[_] if c == classOf[java.lang.Integer] => (IntegerType, true)
      case c: Class[_] if c == classOf[java.lang.Long] => (LongType, true)
      case c: Class[_] if c == classOf[java.lang.Double] => (DoubleType, true)
      case c: Class[_] if c == classOf[java.lang.Byte] => (ByteType, true)
      case c: Class[_] if c == classOf[java.lang.Float] => (FloatType, true)
      case c: Class[_] if c == classOf[java.lang.Boolean] => (BooleanType, true)

      case c: Class[_] if c == classOf[java.math.BigDecimal] => (DecimalType.SYSTEM_DEFAULT, true)
      case c: Class[_] if c == classOf[java.sql.Date] => (DateType, true)
      case c: Class[_] if c == classOf[java.sql.Timestamp] => (TimestampType, true)

      case _ if typeToken.isArray =>
        val (dataType, nullable) = inferDataType(typeToken.getComponentType, seenTypeSet)
        (ArrayType(dataType, nullable), true)

      case _ if iterableType.isAssignableFrom(typeToken) =>
        val (dataType, nullable) = inferDataType(elementType(typeToken), seenTypeSet)
        (ArrayType(dataType, nullable), true)

      case _ if mapType.isAssignableFrom(typeToken) =>
        val (keyType, valueType) = mapKeyValueType(typeToken)
        val (keyDataType, _) = inferDataType(keyType, seenTypeSet)
        val (valueDataType, nullable) = inferDataType(valueType, seenTypeSet)
        (MapType(keyDataType, valueDataType, nullable), true)

      case other if other.isEnum =>
        (StringType, true)

      case other =>
        if (seenTypeSet.contains(other)) {
          throw new UnsupportedOperationException(
            "Cannot have circular references in bean class, but got the circular reference " +
              s"of class $other")
        }

        val properties = getJavaBeanReadableProperties(other)
        val fields = properties.map { property =>
          val returnType = typeToken.method(getReadMethod(other, property)).getReturnType
          val (dataType, nullable) = inferDataType(returnType, seenTypeSet + other)
          new StructField(property.getName, dataType, nullable)
        }
        (new StructType(fields), true)
    }
  }

  private def getReadMethod(messageType: Class[_], fd: FieldDescriptor): Method = {
    val getterSuffix = if (fd.isRepeated) "_list" else ""
    val getter = underscoreToCamel("get_" + fd.getName + getterSuffix)
    messageType.getDeclaredMethod(getter)
  }

  private def getHasMethod(messageType: Class[_], fd: FieldDescriptor): Method = {
    val name = underscoreToCamel("has_" + fd.getName)
    messageType.getDeclaredMethod(name)
  }

  private def getWriteMethod(messageType: Class[_], fieldType: TypeToken[_], fd: FieldDescriptor): Method = {
    val setterPrefix = if (fd.isRepeated) "add_all_" else "set_"
    val setter = underscoreToCamel(setterPrefix + fd.getName)
    val argType = if (fd.isRepeated) iterableType else fieldType
    messageType.getDeclaredMethod(setter, argType.getRawType)
  }

  private def underscoreToCamel(name: String) = "_([a-z\\d])".r.replaceAllIn(name, {m =>
    m.group(1).toUpperCase()
  })

  private def getJavaBeanReadableProperties(beanClass: Class[_]): Array[FieldDescriptor] = {
    val descriptor = descriptorFor(beanClass)
    descriptor.getFields.toArray(Array[FieldDescriptor]())
  }

  private def getJavaBeanReadableAndWritableProperties(
                                                        beanClass: Class[_]): Array[FieldDescriptor] = {
    getJavaBeanReadableProperties(beanClass)
  }

  private def elementType(typeToken: TypeToken[_]): TypeToken[_] = {
    val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JIterable[_]]]
    val iterableSuperType = typeToken2.getSupertype(classOf[JIterable[_]])
    val iteratorType = iterableSuperType.resolveType(iteratorReturnType)
    iteratorType.resolveType(nextReturnType)
  }

  private def mapKeyValueType(typeToken: TypeToken[_]): (TypeToken[_], TypeToken[_]) = {
    val typeToken2 = typeToken.asInstanceOf[TypeToken[_ <: JMap[_, _]]]
    val mapSuperType = typeToken2.getSupertype(classOf[JMap[_, _]])
    val keyType = elementType(mapSuperType.resolveType(keySetReturnType))
    val valueType = elementType(mapSuperType.resolveType(valuesReturnType))
    keyType -> valueType
  }

  /**
    * Returns the Spark SQL DataType for a given java class.  Where this is not an exact mapping
    * to a native type, an ObjectType is returned.
    *
    * Unlike `inferDataType`, this function doesn't do any massaging of types into the Spark SQL type
    * system.  As a result, ObjectType will be returned for things like boxed Integers.
    */
  private def inferExternalType(cls: Class[_]): DataType = cls match {
    case c if c == java.lang.Boolean.TYPE => BooleanType
    case c if c == java.lang.Byte.TYPE => ByteType
    case c if c == java.lang.Short.TYPE => ShortType
    case c if c == java.lang.Integer.TYPE => IntegerType
    case c if c == java.lang.Long.TYPE => LongType
    case c if c == java.lang.Float.TYPE => FloatType
    case c if c == java.lang.Double.TYPE => DoubleType
    case c if c == classOf[Array[Byte]] => BinaryType
    case _ => ObjectType(cls)
  }

  /**
    * Returns an expression that can be used to deserialize an internal row to an object of java bean
    * `T` with a compatible schema.  Fields of the row will be extracted using UnresolvedAttributes
    * of the same name as the constructor arguments.  Nested classes will have their fields accessed
    * using UnresolvedExtractValue.
    */
  def deserializerFor(beanClass: Class[_]): Expression = {
    deserializerFor(TypeToken.of(beanClass), None)
  }

  private def deserializerFor(typeToken: TypeToken[_], path: Option[Expression]): Expression = {
    /** Returns the current path with a sub-field extracted. */
    def addToPath(part: String): Expression = path
      .map(p => UnresolvedExtractValue(p, expressions.Literal(part)))
      .getOrElse(UnresolvedAttribute(part))

    /** Returns the current path or `GetColumnByOrdinal`. */
    def getPath: Expression = path.getOrElse(GetColumnByOrdinal(0, inferDataType(typeToken)._1))

    typeToken.getRawType match {
      case c if !inferExternalType(c).isInstanceOf[ObjectType] => getPath

      case c if c == classOf[java.lang.Short] ||
        c == classOf[java.lang.Integer] ||
        c == classOf[java.lang.Long] ||
        c == classOf[java.lang.Double] ||
        c == classOf[java.lang.Float] ||
        c == classOf[java.lang.Byte] ||
        c == classOf[java.lang.Boolean] =>
        StaticInvoke(
          c,
          ObjectType(c),
          "valueOf",
          getPath :: Nil,
          propagateNull = false)

      case c if c == classOf[java.sql.Date] =>
        StaticInvoke(
          DateTimeUtils.getClass,
          ObjectType(c),
          "toJavaDate",
          getPath :: Nil,
          propagateNull = false)

      case c if c == classOf[java.sql.Timestamp] =>
        StaticInvoke(
          DateTimeUtils.getClass,
          ObjectType(c),
          "toJavaTimestamp",
          getPath :: Nil,
          propagateNull = false)

      case c if c == classOf[java.lang.String] =>
        Invoke(getPath, "toString", ObjectType(classOf[String]))

      case c if c == classOf[java.math.BigDecimal] =>
        Invoke(getPath, "toJavaBigDecimal", ObjectType(classOf[java.math.BigDecimal]))

      case c if c.isArray =>
        val elementType = c.getComponentType
        val primitiveMethod = elementType match {
          case c if c == java.lang.Boolean.TYPE => Some("toBooleanArray")
          case c if c == java.lang.Byte.TYPE => Some("toByteArray")
          case c if c == java.lang.Short.TYPE => Some("toShortArray")
          case c if c == java.lang.Integer.TYPE => Some("toIntArray")
          case c if c == java.lang.Long.TYPE => Some("toLongArray")
          case c if c == java.lang.Float.TYPE => Some("toFloatArray")
          case c if c == java.lang.Double.TYPE => Some("toDoubleArray")
          case _ => None
        }

        primitiveMethod.map { method =>
          Invoke(getPath, method, ObjectType(c))
        }.getOrElse {
          Invoke(
            MapObjects(
              p => deserializerFor(typeToken.getComponentType, Some(p)),
              getPath,
              inferDataType(elementType)._1),
            "array",
            ObjectType(c))
        }

      case c if listType.isAssignableFrom(typeToken) =>
        val et = elementType(typeToken)
        StaticInvoke(
          classOf[java.util.Arrays],
          ObjectType(classOf[JList[_]]),
          "asList",
          Seq(Invoke(
            MapObjects(
              p => deserializerFor(et, Some(p)),
              getPath,
              inferDataType(et)._1),
            "array",
            ObjectType(classOf[Array[Any]]))),
          false)

      case _ if mapType.isAssignableFrom(typeToken) =>
        val (keyType, valueType) = mapKeyValueType(typeToken)
        val keyDataType = inferDataType(keyType)._1
        val valueDataType = inferDataType(valueType)._1

        val keyData =
          Invoke(
            MapObjects(
              p => deserializerFor(keyType, Some(p)),
              Invoke(getPath, "keyArray", ArrayType(keyDataType)),
              keyDataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        val valueData =
          Invoke(
            MapObjects(
              p => deserializerFor(valueType, Some(p)),
              Invoke(getPath, "valueArray", ArrayType(valueDataType)),
              valueDataType),
            "array",
            ObjectType(classOf[Array[Any]]))

        StaticInvoke(
          ArrayBasedMapData.getClass,
          ObjectType(classOf[JMap[_, _]]),
          "toJavaMap",
          keyData :: valueData :: Nil,
          propagateNull = false)

      case other if other.isEnum =>
        StaticInvoke(
          other,
          ObjectType(other),
          "valueOf",
          Invoke(getPath, "toString", ObjectType(classOf[String]), propagateNull = false) :: Nil,
          propagateNull = true)

      case other =>
        val properties = getJavaBeanReadableAndWritableProperties(other)
        val builderClass = other.getDeclaredClasses.find(_.getSimpleName == "Builder").get
        val setters = properties.map { p =>
          val fieldName = p.getName
          val fieldType = typeToken.method(getReadMethod(other, p)).getReturnType
          val (_, nullable) = inferDataType(fieldType)
          val constructor = deserializerFor(fieldType, Some(addToPath(fieldName)))
          val setter = if (nullable) {
            constructor
          } else {
            AssertNotNull(constructor, Seq("currently no type path record in java"))
          }
          getWriteMethod(builderClass, fieldType, p).getName -> setter
        }.toMap

        val newBuilder = StaticInvoke(other, ObjectType(builderClass), "newBuilder", propagateNull = false)
        val builder = InitializeProtobufBuilder(newBuilder, setters)
        val result = Invoke(builder, "build", ObjectType(other))

        if (path.nonEmpty) {
          expressions.If(
            IsNull(getPath),
            expressions.Literal.create(null, ObjectType(other)),
            result
          )
        } else {
          result
        }
    }
  }

  /**
    * Returns an expression for serializing an object of the given type to an internal row.
    */
  def serializerFor(beanClass: Class[_]): CreateNamedStruct = {
    val inputObject = BoundReference(0, ObjectType(beanClass), nullable = true)
    val nullSafeInput = AssertNotNull(inputObject, Seq("top level input bean"))
    serializerFor(nullSafeInput, TypeToken.of(beanClass)) match {
      case expressions.If(_, _, s: CreateNamedStruct) => s
      case other => CreateNamedStruct(expressions.Literal("value") :: other :: Nil)
    }
  }

  private def serializerFor(inputObject: Expression, typeToken: TypeToken[_]): Expression = {

    def toCatalystArray(input: Expression, elementType: TypeToken[_]): Expression = {
      val (dataType, nullable) = inferDataType(elementType)
      if (ScalaReflection.isNativeType(dataType)) {
        NewInstance(
          classOf[GenericArrayData],
          input :: Nil,
          dataType = ArrayType(dataType, nullable))
      } else {
        MapObjects(serializerFor(_, elementType), input, ObjectType(elementType.getRawType))
      }
    }

    if (!inputObject.dataType.isInstanceOf[ObjectType]) {
      inputObject
    } else {
      typeToken.getRawType match {
        case c if c == classOf[String] =>
          StaticInvoke(
            classOf[UTF8String],
            StringType,
            "fromString",
            inputObject :: Nil,
            propagateNull = false)

        case c if c == classOf[java.sql.Timestamp] =>
          StaticInvoke(
            DateTimeUtils.getClass,
            TimestampType,
            "fromJavaTimestamp",
            inputObject :: Nil,
            propagateNull = false)

        case c if c == classOf[java.sql.Date] =>
          StaticInvoke(
            DateTimeUtils.getClass,
            DateType,
            "fromJavaDate",
            inputObject :: Nil,
            propagateNull = false)

        case c if c == classOf[java.math.BigDecimal] =>
          StaticInvoke(
            Decimal.getClass,
            DecimalType.SYSTEM_DEFAULT,
            "apply",
            inputObject :: Nil,
            propagateNull = false)

        case c if c == classOf[java.lang.Boolean] =>
          Invoke(inputObject, "booleanValue", BooleanType)
        case c if c == classOf[java.lang.Byte] =>
          Invoke(inputObject, "byteValue", ByteType)
        case c if c == classOf[java.lang.Short] =>
          Invoke(inputObject, "shortValue", ShortType)
        case c if c == classOf[java.lang.Integer] =>
          Invoke(inputObject, "intValue", IntegerType)
        case c if c == classOf[java.lang.Long] =>
          Invoke(inputObject, "longValue", LongType)
        case c if c == classOf[java.lang.Float] =>
          Invoke(inputObject, "floatValue", FloatType)
        case c if c == classOf[java.lang.Double] =>
          Invoke(inputObject, "doubleValue", DoubleType)

        case _ if typeToken.isArray =>
          toCatalystArray(inputObject, typeToken.getComponentType)

        case _ if listType.isAssignableFrom(typeToken) =>
          toCatalystArray(inputObject, elementType(typeToken))

        case _ if mapType.isAssignableFrom(typeToken) =>
          val (keyType, valueType) = mapKeyValueType(typeToken)

          ExternalMapToCatalyst(
            inputObject,
            ObjectType(keyType.getRawType),
            serializerFor(_: Expression, keyType),
            ObjectType(valueType.getRawType),
            serializerFor(_: Expression, valueType),
            valueNullable = true
          )

        case other if other.isEnum =>
          StaticInvoke(
            classOf[UTF8String],
            StringType,
            "fromString",
            Invoke(inputObject, "name", ObjectType(classOf[String]), propagateNull = false) :: Nil,
            propagateNull = false)

        case other =>
          val properties = getJavaBeanReadableAndWritableProperties(other)
          val nonNullOutput = CreateNamedStruct(properties.flatMap { p =>
            val fieldName = p.getName
            val getter = getReadMethod(other, p)
            val fieldType = typeToken.method(getter).getReturnType
            val getterDataType = inferExternalType(fieldType.getRawType)
            val fieldValue = Invoke(
              inputObject,
              getter.getName,
              getterDataType,
              propagateNull = false)
            if (p.isRepeated()) {
              expressions.Literal(fieldName) :: serializerFor(fieldValue, fieldType) :: Nil
            } else {
              expressions.Literal(fieldName) :: serializerFor(
                expressions.If(
                  Invoke(
                    inputObject,
                    getHasMethod(other, p).getName,
                    BooleanType,
                    propagateNull = false,
                    returnNullable = false),
                  fieldValue,
                  expressions.Literal.create(null, getterDataType)
                ),
                fieldType) :: Nil
            }
          })

          val nullOutput = expressions.Literal.create(null, nonNullOutput.dataType)
          expressions.If(IsNull(inputObject), nullOutput, nonNullOutput)
      }
    }
  }

  /**
    * Initialize a Builder instance by setting its field values via setters if they're not null.
    * This is just like InitializeJavaBean case class except that it check for null before calling the setter.
    * @see org.apache.spark.sql.catalyst.expressions.objects.InitializeJavaBean
    */
  case class InitializeProtobufBuilder(beanInstance: Expression, setters: Map[String, Expression])
    extends Expression with NonSQLExpression {

    override def nullable: Boolean = beanInstance.nullable
    override def children: Seq[Expression] = beanInstance +: setters.values.toSeq
    override def dataType: DataType = beanInstance.dataType

    override def eval(input: InternalRow): Any =
      throw new UnsupportedOperationException("Only code-generated evaluation is supported.")

    override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
      val instanceGen = beanInstance.genCode(ctx)

      val javaBeanInstance = ctx.freshName("protobufBuilder")
      val beanInstanceJavaType = ctx.javaType(beanInstance.dataType)
      ctx.addMutableState(beanInstanceJavaType, javaBeanInstance, "")

      val initialize = setters.map {
        case (setterMethod, fieldValue) =>
          val fieldGen = fieldValue.genCode(ctx)
          s"""
           ${fieldGen.code}
           if (!${fieldGen.isNull}) {
             ${javaBeanInstance}.$setterMethod(${fieldGen.value});
           }
         """
      }
      val initializeCode = ctx.splitExpressions(ctx.INPUT_ROW, initialize.toSeq)

      val code = s"""
      ${instanceGen.code}
      this.${javaBeanInstance} = ${instanceGen.value};
      if (!${instanceGen.isNull}) {
        $initializeCode
      }
     """
      ev.copy(code = code, isNull = instanceGen.isNull, value = instanceGen.value)
    }
  }

}
