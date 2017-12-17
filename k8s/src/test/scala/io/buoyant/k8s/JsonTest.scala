package io.buoyant.k8s

import com.fasterxml.jackson.core.`type`.TypeReference
import com.twitter.io.{Buf, Reader}
import com.twitter.util._
import org.scalatest.FunSuite

private case class TestType(name: String, value: Option[TestType] = None)
private case class OtherType(huh: String, name: Option[String] = None)
private case class TypeWithSeq(name: String, values: Seq[TestType])

class JsonTest extends FunSuite {

  implicit private[this] val testTypeRef = new TypeReference[TestType] {}
  implicit private[this] val otherTypeRef = new TypeReference[OtherType] {}
  implicit private[this] val typeWithSeqRef = new TypeReference[TypeWithSeq] {}

  test("chunked: message") {
    val obj = TestType("foo", Some(TestType("bar")))
    val (decoded, rest) = Json.readChunked[TestType](Json.writeBuf(obj))
    assert(decoded == Some(obj))
    assert(rest.isEmpty)
  }

  test("chunked: message + newline") {
    val obj = TestType("foo", Some(TestType("bar")))
    val (decoded, rest) = Json.readChunked[TestType](Json.writeBuf(obj).concat(Buf.Utf8("\n")))
    assert(decoded == Some(obj))
    assert(rest == Buf.Utf8("\n"))
  }

  test("chunked: ignore unknown properties") {
    val obj = OtherType("foo", Some("bar"))
    val (decoded, rest) = Json.readChunked[TestType](Json.writeBuf(obj))
    assert(decoded === Some(TestType("bar")))
  }

  test("chunked: 2 full objects and a partial object") {
    val objs = Seq(
      TestType("foo", Some(TestType("bar"))),
      TestType("bah", Some(TestType("bat"))),
      TestType("baf", Some(TestType("bal")))
    )
    val readable = Json.writeBuf(objs(0)).concat(Json.writeBuf(objs(1)))
    val buf = readable.concat(Json.writeBuf(objs(2)))
    val (decoded0, rest0) = Json.readChunked[TestType](buf.slice(0, buf.length - 7))
    assert(decoded0 == Some(objs(0)))
    val (decoded1, rest1) = Json.readChunked[TestType](rest0)
    assert(decoded1 == Some(objs(1)))
    assert(rest1 == buf.slice(readable.length, buf.length - 7))
  }

  // Failures inside arrays throw JsonMappingExceptions rather than JsonParseExceptions.
  test("chunked: object containing a collection") {
    val obj0 = TypeWithSeq("hello", Nil)
    val obj1 = TypeWithSeq("foo", Seq(TestType("foo", Some(TestType("bar"))), TestType("baz", Some(TestType("binky")))))
    val readable = Json.writeBuf(obj0)
    val buf = readable.concat(Json.writeBuf(obj1))
    val toChop = 10
    val (decoded, rest) = Json.readChunked[TypeWithSeq](buf.slice(0, buf.length - toChop))
    assert(decoded == Some(obj0))
    assert(rest == buf.slice(readable.length, buf.length - toChop))
  }

  test("stream: messages") {
    val rw = Reader.writable()
    val objs = Seq(
      TestType("foo", Some(TestType("bar"))),
      TestType("bah", Some(TestType("bat"))),
      TestType("baf", Some(TestType("bal")))
    )
    val whole = objs.foldLeft(Buf.Empty) { (b, o) =>
      b.concat(Json.writeBuf(o)).concat(Buf.Utf8("\n"))
    }
    val sz = whole.length / objs.length
    val chunks = Seq(
      whole.slice(0, sz - 2),
      whole.slice(sz - 2, 2 * sz + 2),
      whole.slice(2 * sz + 2, whole.length)
    )

    val decoded = Json.readStream[TestType](rw).toSeq()
    for (c <- chunks)
      Await.result(rw.write(c))
    Await.result(rw.close())

    assert(Await.result(decoded) == objs)
  }

  test("stream: messages exceeding buffer size") {
    val bufsize = 8 * 1024
    // Make the encoded object so large that it will not fit in a single buffered read
    val obj = TestType("foo" * 10000, Some(TestType("inner")))
    val buf = Json.writeBuf(obj)
    assert(buf.length > bufsize)
    val stream = Json.readStream[TestType](Reader.fromBuf(buf), bufsize)
    val decoded = Await.result(stream.toSeq())
    assert(decoded == Seq(obj))
  }
}
