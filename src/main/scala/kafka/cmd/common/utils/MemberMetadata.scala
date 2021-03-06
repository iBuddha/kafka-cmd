package kafka.cmd.common.utils

import java.util

import kafka.coordinator.JoinGroupResult
import kafka.utils.nonthreadsafe



case class MemberSummary(memberId: String,
                         clientId: String,
                         clientHost: String,
                         metadata: Array[Byte],
                         assignment: Array[Byte])

/**
  * Member metadata contains the following metadata:
  *
  * Heartbeat metadata:
  * 1. negotiated heartbeat session timeout
  * 2. timestamp of the latest heartbeat
  *
  * Protocol metadata:
  * 1. the list of supported protocols (ordered by preference)
  * 2. the metadata associated with each protocol
  *
  * In addition, it also contains the following state information:
  *
  * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
  *                                 its rebalance callback will be kept in the metadata if the
  *                                 member has sent the join group request
  * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
  *                            is kept in metadata until the leader provides the group assignment
  *                            and the group transitions to stable
  */
@nonthreadsafe
class MemberMetadata(val memberId: String,
                                          val groupId: String,
                                          val clientId: String,
                                          val clientHost: String,
                                          val rebalanceTimeoutMs: Int,
                                          val sessionTimeoutMs: Int,
                                          val protocolType: String,
                                          var supportedProtocols: List[(String, Array[Byte])]) {

  var assignment: Array[Byte] = Array.empty[Byte]
  var awaitingJoinCallback: JoinGroupResult => Unit = null
  var awaitingSyncCallback: (Array[Byte], Short) => Unit = null
  var latestHeartbeat: Long = -1
  var isLeaving: Boolean = false

  def protocols = supportedProtocols.map(_._1).toSet

  /**
    * Get metadata corresponding to the provided protocol.
    */
  def metadata(protocol: String): Array[Byte] = {
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  /**
    * Check if the provided protocol metadata matches the currently stored metadata.
    */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    if (protocols.size != this.supportedProtocols.size)
      return false

    for (i <- 0 until protocols.size) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, metadata(protocol), assignment)
  }

  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
    * Vote for one of the potential group protocols. This takes into account the protocol preference as
    * indicated by the order of supported protocols and returns the first one also contained in the set
    */
  def vote(candidates: Set[String]): String = {
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString = {
    "[%s,%s,%s,%d]".format(memberId, clientId, clientHost, sessionTimeoutMs)
  }
}
