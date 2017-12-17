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
    val inObj = TestType("foo", Some(TestType("bar")))
    val (outObj, rest) = Json.readChunked[TestType](Json.writeBuf(inObj))
    assert(outObj.contains(inObj))
    assert(rest.isEmpty)
  }

  test("chunked: message + newline") {
    val inObj = TestType("foo", Some(TestType("bar")))
    val (outObj, rest) = Json.readChunked[TestType](Json.writeBuf(inObj).concat(Buf.Utf8("\n")))
    assert(outObj.contains(inObj))
    assert(rest == Buf.Utf8("\n"))
  }

  test("chunked: ignore unknown properties") {
    val (outObj, rest) = Json.readChunked[TestType](Json.writeBuf(OtherType("foo", Some("bar"))))
    assert(outObj.contains(TestType("bar")))
  }

  test("chunked: 2 full objects and a partial object") {
    val inObjs = Seq(
      TestType("foo", Some(TestType("bar"))),
      TestType("bah", Some(TestType("bat"))),
      TestType("baf", Some(TestType("bal")))
    )
    val readable = Json.writeBuf(inObjs(0)).concat(Json.writeBuf(inObjs(1)))
    val buf = readable.concat(Json.writeBuf(inObjs(2)))
    val (outObj1, rest1) = Json.readChunked[TestType](buf.slice(0, buf.length - 7))
    assert(outObj1.contains(inObjs(0)))
    val (objOut2, rest2) = Json.readChunked[TestType](rest1)
    assert(objOut2.contains(inObjs(1)))
    assert(rest2 == buf.slice(readable.length, buf.length - 7))
  }

  // Failures inside arrays throw JsonMappingExceptions rather than JsonParseExceptions.
  test("chunked: object containing a collection") {
    val obj0 = TypeWithSeq("hello", Nil)
    val obj1 = TypeWithSeq("foo", Seq(TestType("foo", Some(TestType("bar"))), TestType("baz", Some(TestType("binky")))))
    val readable = Json.writeBuf(obj0)
    val buf = readable.concat(Json.writeBuf(obj1))
    val toChop = 10
    val (obj, rest) = Json.readChunked[TypeWithSeq](buf.slice(0, buf.length - toChop))
    assert(obj.contains(obj0))
    assert(rest == buf.slice(readable.length, buf.length - toChop))
  }

  test("stream: messages") {
    val rw = Reader.writable()
    val objs = Seq(
      // Force the first object to be so large that it will not fit in a single buffered read
      TestType("foo", Some(TestType("bar" * 2000))),
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

    val stream = Json.readStream[TestType](rw)
    for (c <- chunks) Await.result(rw.write(c))
    Await.result(rw.close())

    val decoded = Await.result(stream.toSeq())
    assert(decoded === objs)
  }
}
