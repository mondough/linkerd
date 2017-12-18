package io.buoyant.config

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.twitter.concurrent.AsyncStream
import com.twitter.io.{Buf, Reader}
import com.twitter.logging.Logger
import com.twitter.util.{Return, Throw, Try}

import scala.collection.mutable
import scala.util.control.NonFatal

class JsonStreamParser(mapper: ObjectMapper with ScalaObjectMapper) {

  private[this] val log = Logger.get

  def read[T: TypeReference](buf: Buf): Try[T] = {
    val Buf.ByteArray.Owned(bytes, begin, end) = Buf.ByteArray.coerce(buf)
    Try(mapper.readValue[T](bytes, begin, end - begin, implicitly[TypeReference[T]]))
  }

  def writeBuf[T: TypeReference](t: T) = Buf.ByteArray.Owned(mapper.writeValueAsBytes(t))

  /*
   * JSON Streaming
   */
  object EndOfStream extends Throwable

  private[this] object Incomplete {
    val unexpectedEOI = "end-of-input"

    def unapply(jpe: JsonProcessingException): Boolean =
      jpe.getMessage match {
        case null => false
        case msg => msg.contains(unexpectedEOI)
      }
  }

  /**
   * Given a chunk of bytes, read the next object from the stream if it can be read, and return the remaining unread
   * buffer.
   */
  def readChunked[T: TypeReference](chunk: Buf): (Option[T], Buf) = {
    val Buf.ByteArray.Owned(bytes, begin, end) = Buf.ByteArray.coerce(chunk)
    val parser = mapper.getFactory.createParser(bytes, begin, end - begin)
    val Buf.Utf8(s) = chunk
    log.info("trying json read on: %s", s)

    val (obj, offsetAfter) = try {
      val v = Option(parser.readValueAs[T](implicitly[TypeReference[T]]))
      val o = parser.getCurrentLocation.getByteOffset.toInt
      (v, o)
    } catch {
      case Incomplete() => (None, 0)
    } finally {
      parser.close()
    }

    log.info("json read object: %s", obj)
    val leftover = chunk.slice(offsetAfter, chunk.length)
    (obj, leftover)
  }

  private def fromReaderJson(r: Reader, chunkSize: Int = Int.MaxValue): AsyncStream[Option[Buf]] = {
    log.trace("json reading chunk of %d bytes", chunkSize)
    val read = r.read(chunkSize).respond {
      case Return(Some(Buf.Utf8(chunk))) =>
        log.trace("json read chunk: %s", chunk)
      case Return(None) | Throw(_: Reader.ReaderDiscarded) =>
        log.trace("json read eoc")
      case Throw(e) =>
        log.warning(e, "json read error")
    }.handle {
      case NonFatal(e) => None
    }

    AsyncStream.fromFuture(read).flatMap {
      //Fake None element to get around scanLeft being one behind bug
      //Tracked in https://github.com/twitter/util/issues/195
      //Can be removed once 195 is fixed
      case Some(buf) => Some(buf) +:: None +:: fromReaderJson(r, chunkSize)
      case None => AsyncStream.empty[Option[Buf]]
    }
  }

  def readStream[T: TypeReference](reader: Reader, bufsize: Int = 8 * 1024): AsyncStream[T] = {
    fromReaderJson(reader, bufsize)
      .scanLeft[(Seq[T], Buf)]((Nil, Buf.Empty)) {
        case ((_, lastLeftover), Some(chunk)) =>
          var b = lastLeftover.concat(chunk)
          val objs = mutable.Buffer.empty[T]
          var reading = true
          while (reading) {
            val (o, leftover) = readChunked(b)
            objs ++= o
            b = leftover
            // If no object was returned, any leftover buffer cannot be parsed by itself.
            // It will be prepended to the next chunk's buffer instead.
            reading = o.isDefined
          }
          (objs, b)
        case ((_, lastLeftover), None) =>
          (Nil, lastLeftover)
      }
      .flatMap {
        case (v, _) => AsyncStream.fromSeq(v)
      }
  }
}
