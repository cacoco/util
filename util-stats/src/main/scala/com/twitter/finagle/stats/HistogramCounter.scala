package com.twitter.finagle.stats

import com.twitter.conversions.DurationOps._
import com.twitter.util.Closable
import com.twitter.util.Duration
import com.twitter.util.Future
import com.twitter.util.Time
import com.twitter.util.Timer
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.LongAdder
import scala.collection.JavaConverters._

private[twitter] sealed abstract class StatsFrequency(val frequency: Duration) {
  def suffix: String
}

private[twitter] object StatsFrequency {
  case object HundredMilliSecondly extends StatsFrequency(100.millis) {
    override def suffix = "hundredMilliSecondly"
  }
}

/**
 * Class for creating [[HistogramCounter]]s. It is expected that there be one [[HistogramCounterFactory]]
 * per process -- otherwise we will schedule multiple timer tasks for aggregating the counter into
 * a stat, and there can be multiple aggregations for a single stat which may produce unexpected
 * results.
 */
private[twitter] class HistogramCounterFactory(timer: Timer, nowMs: () => Long) extends Closable {

  @volatile private[this] var closed = false

  private[this] val frequencyToStats: Map[
    StatsFrequency,
    ConcurrentHashMap[Stat, HistogramCounter]
  ] = Map(
    StatsFrequency.HundredMilliSecondly -> new ConcurrentHashMap[Stat, HistogramCounter]
  )

  frequencyToStats.map {
    case (statsFrequency, statToCounter) =>
      timer.doLater(statsFrequency.frequency)(recordStatsForCounters(statsFrequency, statToCounter))
  }

  def apply(
    name: Seq[String],
    frequency: StatsFrequency,
    statsReceiver: StatsReceiver
  ): HistogramCounter = {
    val stat = statsReceiver.stat(normalizeName(name) :+ frequency.suffix: _*)
    val histogramCounter = new HistogramCounter(stat, nowMs, frequency.frequency.inMillis)
    val existing = frequencyToStats(frequency).putIfAbsent(stat, histogramCounter)
    if (existing == null) {
      histogramCounter
    } else {
      existing
    }
  }

  override def close(deadline: Time): Future[Unit] = {
    closed = true
    Future.Done
  }

  private[this] def recordStatsForCounters(
    statsFrequency: StatsFrequency,
    statToCounter: ConcurrentHashMap[Stat, HistogramCounter]
  ): Unit = {
    statToCounter.values().asScala.foreach { counter =>
      counter.recordAndReset()
    }
    if (!closed) {
      timer.doLater(statsFrequency.frequency)(recordStatsForCounters(statsFrequency, statToCounter))
    }
  }

  private[this] def normalizeName(name: Seq[String]): Seq[String] = {
    if (name.forall(!_.contains("/"))) {
      name
    } else {
      name.map(_.split("/")).flatten
    }
  }
}

private[stats] class HistogramCounter(stat: Stat, nowMs: () => Long, windowSizeMs: Long) {
  private[this] val counter: LongAdder = new LongAdder
  @volatile private[this] var lastRecordAndResetMs = nowMs()

  private[stats] def recordAndReset(): Unit = {
    val count = counter.sumThenReset()
    val now = nowMs()
    val elapsed = Math.max(0, now - lastRecordAndResetMs)
    val elapsedWindows = elapsed.toFloat / windowSizeMs
    stat.add(count / elapsedWindows)
    lastRecordAndResetMs = now
  }

  def incr(delta: Long): Unit = {
    counter.add(delta)
  }

  def incr(): Unit = {
    counter.increment()
  }
}
