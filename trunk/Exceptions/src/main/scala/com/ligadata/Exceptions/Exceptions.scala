package com.ligadata.Exceptions

class KamanjaException(e: String) extends Exception(e)

case class UnsupportedObjectException(e: String) extends Exception(e)
case class Json4sParsingException(e: String) extends Exception(e)
case class FunctionListParsingException(e: String) extends Exception(e)
case class FunctionParsingException(e: String) extends Exception(e)
case class TypeDefListParsingException(e: String) extends Exception(e)
case class TypeParsingException(e: String) extends Exception(e)
case class TypeDefProcessingException(e: String) extends Exception(e)
case class ConceptListParsingException(e: String) extends Exception(e)
case class ConceptParsingException(e: String) extends Exception(e)
case class MessageDefParsingException(e: String) extends Exception(e)
case class ContainerDefParsingException(e: String) extends Exception(e)
case class ModelDefParsingException(e: String) extends Exception(e)
case class ApiResultParsingException(e: String) extends Exception(e)
case class UnexpectedMetadataAPIException(e: String) extends Exception(e)
case class ObjectNotFoundException(e: String) extends Exception(e)
case class CreateStoreFailedException(e: String) extends Exception(e)
case class UpdateStoreFailedException(e: String) extends Exception(e)

case class LoadAPIConfigException(e: String) extends Exception(e)
case class MissingPropertyException(e: String) extends Exception(e)
case class InvalidPropertyException(e: String) extends Exception(e)
case class KryoSerializationException(e: String) extends Exception(e)
case class InternalErrorException(e: String) extends Exception(e)
case class TranIdNotFoundException(e: String) extends Exception(e)

case class AlreadyExistsException(e: String) extends Throwable(e)
case class ObjectNolongerExistsException(e: String) extends Throwable(e)
case class InvalidArgumentException(e: String) extends Throwable(e)

case class Json4sSerializationException(e: String) extends Exception(e)
case class ZkTransactionParsingException(e: String) extends Exception(e)
case class EngineConfigParsingException(e: String) extends Exception(e)
case class ApiArgListParsingException(e: String) extends Exception(e)
case class JsonException(message: String) extends Exception(message)
case class MessageException(message: String) extends Exception(message)

case class MsgCompilationFailedException(e: String) extends Exception(e)
case class ModelCompilationFailedException(e: String) extends Exception(e)

case class MissingArgumentException(e: String) extends Exception(e)
case class CreateKeySpaceFailedException(e: String) extends Exception(e)
case class KeyNotFoundException(e: String) extends Exception(e)
case class ConnectionFailedException(e: String) extends Exception(e)

case class SerializerManagerException(message: String) extends Exception(message)
case class ProtoBufSerializationException(e: String) extends Throwable(e)

case class KamanjaInvalidOptionsException(message: String) extends KamanjaException(message)



