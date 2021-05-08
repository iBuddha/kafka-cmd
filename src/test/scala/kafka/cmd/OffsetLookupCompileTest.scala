package kafka.cmd

import kafka.cmd.common.request.FindPartitionOffsetRequest
import kafka.cmd.repl.OffsetLookupCompiler
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatest.{BeforeAndAfter}

/**
  * Created by xhuang on 25/04/2017.
  */
class OffsetLookupCompileTest extends AnyFlatSpecLike with Matchers with BeforeAndAfter {
  "OffsetLookupCompiler" should "compile successfully " in {
    var request = OffsetLookupCompiler.compile("select offset from 1@clicks where time = 5h-ago", 1)
    request shouldBe a [FindPartitionOffsetRequest]
    request =  OffsetLookupCompiler.compile("select offset from 1@clicks where time=5h-ago", 1)
    request shouldBe a [FindPartitionOffsetRequest]
  }
}
