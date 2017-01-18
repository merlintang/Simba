package org.apache.spark.sql.simba

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.annotation.InterfaceStability
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.ui.SQLListener
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.internal.StaticSQLConf._
import org.apache.spark.sql.simba.index.IndexType
import org.apache.spark.sql.{SparkSession => SParkSparkSession, _}
import org.apache.spark.sql.{Dataset => SparkDataSet}
import org.apache.spark.util.Utils

import scala.reflect.ClassTag
import scala.util.control.NonFatal
/**
  * Created by mtang on 1/9/17.
  */
class SparkSession private[simba]
(@transient override val sparkContext: SparkContext,
 @transient protected[simba] val indexManager: IndexManager
)extends SParkSparkSession (sparkContext)
{

  self=>

  def this(sparkContext: SparkContext) = {
    this(sparkContext, new IndexManager)
  }

  /**
    * State isolated across sessions, including SQL configurations, temporary tables, registered
    * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
    */
  @transient
  private[sql] override  lazy val sessionState: SimbaSessionState = {
    new SimbaSessionState(this,indexManager)
  }


  def hasIndex(tableName: String, indexName: String): Boolean = {
    indexManager.lookupIndexedData(table(tableName), indexName).nonEmpty
  }

  def indexTable(tableName: String, indexType: IndexType,
                 indexName: String, column: Array[String]): Unit = {
    import SimbaImplicits._
    val table = _simbaContext.table(tableName)
    assert(table != null, "Table not found")
    val attrs = table.queryExecution.analyzed.output
    val columnKeys = column.map(attr => {
      var ans: Attribute = null
      for (i <- attrs.indices)
        if (attrs(i).name.equals(attr)) ans = attrs(i)
      assert(ans != null, "Attribute not found")
      ans
    }).toList
    indexManager.createIndexQuery(_simbaContext.table(tableName), indexType,
      indexName, columnKeys, Some(tableName))
  }

  def showIndex(tableName: String): Unit = indexManager.showQuery(this, tableName)

  def persistIndex(indexName: String, fileName: String): Unit =
    indexManager.persistIndex(this, indexName, fileName)

  def loadIndex(indexName: String, fileName: String): Unit =
    indexManager.loadIndex(this, indexName, fileName)

  def dropIndexTableByName(tableName: String, indexName: String): Unit = {
    indexManager.dropIndexByNameQuery(table(tableName), indexName)
  }

  def clearIndex(): Unit = indexManager.clearIndex()

  object SimbaImplicits extends Serializable {
    protected[simba] def _simbaContext: SparkSession = self

    implicit def datasetToSimbaDataSet[T : Encoder](df: SparkDataSet[T]): Dataset[T] = Dataset(self, df.queryExecution.logical)
  }

}

@InterfaceStability.Stable
object SparkSession {

  /**
    * Builder for [[SparkSession]].
    */
  @InterfaceStability.Stable
  class Builder extends Logging {

    private[this] val options = new scala.collection.mutable.HashMap[String, String]

    private[this] var userSuppliedContext: Option[SparkContext] = None

    private[spark] def sparkContext(sparkContext: SparkContext): Builder = synchronized {
      userSuppliedContext = Option(sparkContext)
      this
    }

    /**
      * Sets a name for the application, which will be shown in the Spark web UI.
      * If no application name is set, a randomly generated name will be used.
      *
      * @since 2.0.0
      */
    def appName(name: String): Builder = config("spark.app.name", name)

    /**
      * Sets a config option. Options set using this method are automatically propagated to
      * both `SparkConf` and SparkSession's own configuration.
      *
      * @since 2.0.0
      */
    def config(key: String, value: String): Builder = synchronized {
      options += key -> value
      this
    }

    /**
      * Sets a config option. Options set using this method are automatically propagated to
      * both `SparkConf` and SparkSession's own configuration.
      *
      * @since 2.0.0
      */
    def config(key: String, value: Long): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
      * Sets a config option. Options set using this method are automatically propagated to
      * both `SparkConf` and SparkSession's own configuration.
      *
      * @since 2.0.0
      */
    def config(key: String, value: Double): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
      * Sets a config option. Options set using this method are automatically propagated to
      * both `SparkConf` and SparkSession's own configuration.
      *
      * @since 2.0.0
      */
    def config(key: String, value: Boolean): Builder = synchronized {
      options += key -> value.toString
      this
    }

    /**
      * Sets a list of config options based on the given `SparkConf`.
      *
      * @since 2.0.0
      */
    def config(conf: SparkConf): Builder = synchronized {
      conf.getAll.foreach { case (k, v) => options += k -> v }
      this
    }

    /**
      * Sets the Spark master URL to connect to, such as "local" to run locally, "local[4]" to
      * run locally with 4 cores, or "spark://master:7077" to run on a Spark standalone cluster.
      *
      * @since 2.0.0
      */
    def master(master: String): Builder = config("spark.master", master)

    /**
      * Enables Hive support, including connectivity to a persistent Hive metastore, support for
      * Hive serdes, and Hive user-defined functions.
      *
      * @since 2.0.0
      */
    def enableHiveSupport(): Builder = synchronized {
      if (hiveClassesArePresent) {
        config(CATALOG_IMPLEMENTATION.key, "hive")
      } else {
        throw new IllegalArgumentException(
          "Unable to instantiate SparkSession with Hive support because " +
            "Hive classes are not found.")
      }
    }

    /**
      * Gets an existing [[SparkSession]] or, if there is no existing one, creates a new
      * one based on the options set in this builder.
      *
      * This method first checks whether there is a valid thread-local SparkSession,
      * and if yes, return that one. It then checks whether there is a valid global
      * default SparkSession, and if yes, return that one. If no valid global default
      * SparkSession exists, the method creates a new SparkSession and assigns the
      * newly created SparkSession as the global default.
      *
      * In case an existing SparkSession is returned, the config options specified in
      * this builder will be applied to the existing SparkSession.
      *
      * @since 2.0.0
      */
    def getOrCreate(): SparkSession = synchronized {
      // Get the session from current thread's active session.
      var session = activeThreadSession.get()
      if ((session ne null) && !session.sparkContext.isStopped) {
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        if (options.nonEmpty) {
          logWarning("Using an existing SparkSession; some configuration may not take effect.")
        }
        return session
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = defaultSession.get()
        if ((session ne null) && !session.sparkContext.isStopped) {
          options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
          if (options.nonEmpty) {
            logWarning("Using an existing SparkSession; some configuration may not take effect.")
          }
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          val randomAppName = java.util.UUID.randomUUID().toString
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(randomAppName)
          }
          val sc = SparkContext.getOrCreate(sparkConf)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession
          options.foreach { case (k, v) => sc.conf.set(k, v) }
          if (!sc.conf.contains("spark.app.name")) {
            sc.conf.setAppName(randomAppName)
          }
          sc
        }
        session = new SparkSession(sparkContext)
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        defaultSession.set(session)

        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            defaultSession.set(null)
            sqlListener.set(null)
          }
        })
      }

      return session
    }
  }

  /**
    * Creates a [[SparkSession.Builder]] for constructing a [[SparkSession]].
    *
    * @since 2.0.0
    */
  def builder(): Builder = new Builder

  /**
    * Changes the SparkSession that will be returned in this thread and its children when
    * SparkSession.getOrCreate() is called. This can be used to ensure that a given thread receives
    * a SparkSession with an isolated session, instead of the global (first created) context.
    *
    * @since 2.0.0
    */
  def setActiveSession(session: SparkSession): Unit = {
    activeThreadSession.set(session)
  }

  /**
    * Clears the active SparkSession for current thread. Subsequent calls to getOrCreate will
    * return the first created context instead of a thread-local override.
    *
    * @since 2.0.0
    */
  def clearActiveSession(): Unit = {
    activeThreadSession.remove()
  }

  /**
    * Sets the default SparkSession that is returned by the builder.
    *
    * @since 2.0.0
    */
  def setDefaultSession(session: SparkSession): Unit = {
    defaultSession.set(session)
  }

  /**
    * Clears the default SparkSession that is returned by the builder.
    *
    * @since 2.0.0
    */
  def clearDefaultSession(): Unit = {
    defaultSession.set(null)
  }

  private[sql] def getActiveSession: Option[SparkSession] = Option(activeThreadSession.get)

  private[sql] def getDefaultSession: Option[SparkSession] = Option(defaultSession.get)

  /** A global SQL listener used for the SQL UI. */
  private[sql] val sqlListener = new AtomicReference[SQLListener]()

  ////////////////////////////////////////////////////////////////////////////////////////
  // Private methods from now on
  ////////////////////////////////////////////////////////////////////////////////////////

  /** The active SparkSession for the current thread. */
  private val activeThreadSession = new InheritableThreadLocal[SparkSession]

  /** Reference to the root SparkSession. */
  private val defaultSession = new AtomicReference[SparkSession]

  private val HIVE_SESSION_STATE_CLASS_NAME = "org.apache.spark.sql.hive.HiveSessionState"

  private def sessionStateClassName(conf: SparkConf): String = {
    conf.get(CATALOG_IMPLEMENTATION) match {
      case "hive" => HIVE_SESSION_STATE_CLASS_NAME
      case "in-memory" => classOf[SessionState].getCanonicalName
    }
  }

  /**
    * Helper method to create an instance of [[T]] using a single-arg constructor that
    * accepts an [[Arg]].
    */
  private def reflect[T, Arg <: AnyRef](
                                         className: String,
                                         ctorArg: Arg)(implicit ctorArgTag: ClassTag[Arg]): T = {
    try {
      val clazz = Utils.classForName(className)
      val ctor = clazz.getDeclaredConstructor(ctorArgTag.runtimeClass)
      ctor.newInstance(ctorArg).asInstanceOf[T]
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(s"Error while instantiating '$className':", e)
    }
  }

  /**
    * @return true if Hive classes can be loaded, otherwise false.
    */
  private[spark] def hiveClassesArePresent: Boolean = {
    try {
      Utils.classForName(HIVE_SESSION_STATE_CLASS_NAME)
      Utils.classForName("org.apache.hadoop.hive.conf.HiveConf")
      true
    } catch {
      case _: ClassNotFoundException | _: NoClassDefFoundError => false
    }
  }


}
