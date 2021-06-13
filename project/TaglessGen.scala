import sbt._
import sbt.Keys._

import java.lang.reflect._
import java.util.concurrent.CompletableFuture
import java.util.function.Consumer
import scala.reflect.ClassTag

object TaglessGen {

  lazy val taglessGenClasses =
    settingKey[List[Class[_]]]("classes for which tagless-final algebras should be generated")
  lazy val taglessGenDir     = settingKey[File]("directory where tagless-final algebras go")
  lazy val taglessGenPackage = settingKey[String]("package where tagless-final algebras go")
  lazy val taglessGenRenames =
    settingKey[Map[Class[_], String]]("map of imports that must be renamed")
  lazy val taglessGen = taskKey[Seq[File]]("generate tagless-final algebras")
  lazy val taglessAwsService = taskKey[String](
    "What aws service generate algebras, must match the package name  ex [s3, sqs, sns, kinesis]"
  )

  lazy val taglessGenSettings = Seq(
    taglessGenClasses := Nil,
    taglessGenDir     := (Compile / sourceManaged).value,
    taglessGenPackage := "aws.tagless",
    taglessGenRenames := Map(classOf[java.sql.Array] -> "SqlArray"),
    taglessGen :=
      new TaglessGen(
        taglessGenClasses.value,
        taglessGenPackage.value,
        taglessGenRenames.value,
        state.value.log,
        taglessAwsService.value
      ).gen(taglessGenDir.value)
  )

}

class TaglessGen(
  managed: List[Class[_]],
  pkg: String,
  renames: Map[Class[_], String],
  log: Logger,
  awsService: String
) {

  // These Java classes will have non-Java names in our generated code
  val ClassBoolean = classOf[Boolean]
  val ClassByte    = classOf[Byte]
  val ClassShort   = classOf[Short]
  val ClassInt     = classOf[Int]
  val ClassLong    = classOf[Long]
  val ClassFloat   = classOf[Float]
  val ClassDouble  = classOf[Double]
  val ClassObject  = classOf[Object]
  val ClassVoid    = Void.TYPE

  def tparams(t: Type): List[String] =
    t match {
      case t: GenericArrayType  => tparams(t.getGenericComponentType)
      case t: ParameterizedType => t.getActualTypeArguments.toList.flatMap(tparams)
      case t: TypeVariable[_]   => List(t.toString)
      case _                    => Nil
    }

  def toScalaType(t: Type): String =
    t match {
      case t: GenericArrayType => s"Array[${toScalaType(t.getGenericComponentType)}]"
      case t: ParameterizedType =>
        s"${toScalaType(t.getRawType)}${t.getActualTypeArguments.map(toScalaType).mkString("[", ", ", "]")}"
      case t: WildcardType =>
        t.getUpperBounds.toList.filterNot(_ == classOf[Object]) match {
          case (c: Class[_]) :: Nil => s"_ <: ${c.getName}"
          case Nil                  => "_"
          case cs                   => sys.error("unhandled upper bounds: " + cs.toList)
        }
      case t: TypeVariable[_]       => t.toString
      case ClassVoid                => "Unit"
      case ClassBoolean             => "Boolean"
      case ClassByte                => "Byte"
      case ClassShort               => "Short"
      case ClassInt                 => "Int"
      case ClassLong                => "Long"
      case ClassFloat               => "Float"
      case ClassDouble              => "Double"
      case ClassObject              => "AnyRef"
      case x: Class[_] if x.isArray => s"Array[${toScalaType(x.getComponentType)}]"
      case x: Class[_]              => renames.getOrElse(x, x.getSimpleName)
    }

  // Each constructor for our algebra maps to an underlying method, and an index is provided to
  // disambiguate in cases of overloading.
  case class Ctor(method: Method, index: Int) {

    // The method name, unchanged
    def mname: String =
      method.getName

    // The case class constructor name, capitalized and with an index when needed
    def cname: String = {
      val s = mname(0).toUpper +: mname.drop(1)
      (if (index == 0) s else s"$s$index")
    }

    // Constructor parameter type names
    def cparams: List[String] =
      method.getGenericParameterTypes.toList.map(toScalaType)

    def ctparams: String = {
      val ss = (method.getGenericParameterTypes.toList.flatMap(tparams) ++ tparams(
        method.getGenericReturnType
      )).toSet
      if (ss.isEmpty) "" else ss.mkString("[", ", ", "]")
    }

    // Constructor arguments, a .. z zipped with the right type
    def cargs: List[String] =
      "abcdefghijklmnopqrstuvwxyz".toList.zip(cparams).map {
        case (n, t) => s"$n: $t"
      }

    // Return type name
    def ret: String =
      method.getGenericReturnType match {
        case t: ParameterizedType if t.getRawType == classOf[CompletableFuture[_]] =>
          toScalaType(t.getActualTypeArguments.toList.head)
        case t => toScalaType(t)
      }
    def isRetFuture: Boolean =
      method.getGenericReturnType match {
        case t: ParameterizedType if t.getRawType == classOf[CompletableFuture[_]] =>
          true
        case t => false
      }
    // Case class/object declaration
    def ctor(opname: String): String =
      ((cparams match {
        case Nil => s"|case object $cname"
        case ps  => s"|final case class $cname$ctparams(${cargs.mkString(", ")})"
      }) + s""" extends $opname[$ret] {
        |      def visit[F[_]](v: Visitor[F]) = v.$mname${if (args.isEmpty) "" else s"($args)"}
        |    }""").trim.stripMargin

    // Argument list: a, b, c, ... up to the proper arity
    def args: String =
      "abcdefghijklmnopqrstuvwxyz".toList.take(cparams.length).mkString(", ")

    // Pattern to match the constructor
    def pat: String =
      cparams match {
        case Nil => s"object $cname"
        case ps  => s"class  $cname(${cargs.mkString(", ")})"
      }

    // Case clause mapping this constructor to the corresponding primitive action
    def prim(sname: String): String =
      (if (cargs.isEmpty)
         s"case $cname => primitive(_.$mname)"
       else
         s"case $cname($args) => primitive(_.$mname($args))")

    // Smart constructor
    def lifted(ioname: String): String =
      if (cargs.isEmpty) {
        s"val $mname: $ioname[$ret] = FF.liftF($cname)"
      } else {
        s"def $mname$ctparams(${cargs.mkString(", ")}): $ioname[$ret] = FF.liftF($cname($args))"
      }

    def visitor: String =
      if (cargs.isEmpty) s"|      def $mname: F[$ret]"
      else s"|      def $mname$ctparams(${cargs.mkString(", ")}): F[$ret]"

    def stub: String =
      if (cargs.isEmpty) s"""|      def $mname: F[$ret] = sys.error("Not implemented: $mname")"""
      else
        s"""|      def $mname$ctparams(${cargs.mkString(", ")}): F[$ret] = sys.error("Not implemented: $mname$ctparams(${cparams
          .mkString(", ")})")"""

    def kleisliImpl: String = {
      val ep = if (isRetFuture) "eff" else "primitive"
      if (cargs.isEmpty) s"|    override def $mname = $ep(_.$mname)"
      else
        s"|    override def $mname$ctparams(${cargs.mkString(", ")}) = $ep(_.$mname($args))"
    }

    def kleisliLensImpl: String = {
      val ep = if (isRetFuture) "eff1" else "primitive1"
      if (cargs.isEmpty) s"|    override def $mname = Kleisli(e => $ep(f(e).$mname))"
      else
        s"|    override def $mname$ctparams(${cargs.mkString(", ")}) = Kleisli(e => $ep(f(e).$mname($args)))"
    }

    def fImpl: String = {
      val ep = if (isRetFuture) "eff1" else "primitive1"
      if (cargs.isEmpty) s"|    override def $mname = $ep(client.$mname)"
      else
        s"|    override def $mname$ctparams(${cargs.mkString(", ")}) = $ep(client.$mname($args))"
    }

  }

  // This class, plus any superclasses and interfaces, "all the way up"
  def closure(c: Class[_]): List[Class[_]] =
    (c :: (Option(c.getSuperclass).toList ++ c.getInterfaces.toList).flatMap(closure)).distinct
      .filterNot(_.getName == "java.lang.AutoCloseable") // not available in jdk1.6
      .filterNot(_.getName == "java.lang.Object")        // we don't want .equals, etc.

  implicit class MethodOps(m: Method) {
    def isStatic: Boolean =
      (m.getModifiers & Modifier.STATIC) != 0
  }

  // All non-deprecated methods for this class and any superclasses/interfaces
  def methods(c: Class[_]): List[Method] =
    closure(c)
      .flatMap(_.getDeclaredMethods.toList)
      .distinct
      .filterNot(_.isStatic)
      .filter(_.getAnnotation(classOf[Deprecated]) == null)
      .filterNot(_.getParameterTypes.contains(classOf[Consumer[_]]))

  // Ctor values for all methods in of A plus superclasses, interfaces, etc.
  def ctors[A](implicit ev: ClassTag[A]): List[Ctor] =
    methods(ev.runtimeClass)
      .groupBy(_.getName)
      .toList
      .flatMap {
        case (n, ms) =>
          ms.toList
            .sortBy(_.getGenericParameterTypes.map(toScalaType).mkString(","))
            .zipWithIndex
            .map {
              case (m, i) => Ctor(m, i)
            }
      }
      .sortBy(c => (c.mname, c.index))

  // Fully qualified rename, if any
  def renameImport(c: Class[_]): String = {
    val sn = c.getSimpleName
    val an = renames.getOrElse(c, sn)
    if (sn == an) s"import ${c.getName}"
    else s"import ${c.getPackage.getName}.{ $sn => $an }"
  }

  // All types referenced by all methods on A, superclasses, interfaces, etc.
  def imports[A](implicit ev: ClassTag[A]): List[String] = {
    def extractGenericTypes(m: Method): List[Class[_]] = m.getGenericReturnType match {
      case t: ParameterizedType if t.getRawType == classOf[CompletableFuture[_]] =>
        m.getReturnType :: Nil //t.getActualTypeArguments.toList.map(tt => Class.forName(tt.getTypeName))
      case _ => m.getReturnType :: Nil
    }
    (renameImport(ev.runtimeClass) :: ctors
      .map(_.method)
      .flatMap(m => m.getReturnType :: m.getParameterTypes.toList)
      .map(t => if (t.isArray) t.getComponentType else t)
      .filterNot(t => t.isPrimitive || t == classOf[Object])
      .map(c => renameImport(c))).distinct.sorted
  }

  // The algebra module for A
  def module[A](implicit ev: ClassTag[A]): String = {
    val oname  = ev.runtimeClass.getSimpleName // original name, without name mapping
    val sname  = toScalaType(ev.runtimeClass)
    val opname = s"${oname}Op"
    val ioname = s"${oname}IO"
    val mname  = oname.toLowerCase
    s"""
    |package $pkg
    |
    |import software.amazon.awssdk.services.$awsService.model._
    |
    |${imports[A].mkString("\n")}
    |
    |
    |    trait $opname[F[_]] {
    |      // $sname
         ${ctors[A].map(_.visitor).mkString("\n    ")}
    |
    |
    |}
    |""".trim.stripMargin
  }

  // Import for the IO type for a carrer type, with renaming
  def ioImport(c: Class[_]): String = {
    val sn = c.getSimpleName
    s"import ${sn.toLowerCase}.${sn}IO"
  }

  def kleisliInterp[A](implicit ev: ClassTag[A]): String = {
    val oname = ev.runtimeClass.getSimpleName // original name, without name mapping
    val sname = toScalaType(ev.runtimeClass)
    s"""
       |  trait ${oname}Interpreter extends ${oname}Op[Kleisli[M, $sname, *]] {
       |
       |    // domain-specific operations are implemented in terms of `primitive`
       |${ctors[A].map(_.kleisliImpl).mkString("\n")}
       |      def lens[E](f: E => $oname): ${oname}Op[Kleisli[M, E, *]] =
       |          new ${oname}Op[Kleisli[M, E, *]] {
       |${ctors[A].map(_.kleisliLensImpl).mkString("\n")}
       |      }
       |  }
       |""".trim.stripMargin
  }

  def fInterp[A](implicit ev: ClassTag[A]): String = {
    val oname = ev.runtimeClass.getSimpleName // original name, without name mapping
    s"""
       |  def ${oname}Resource(builder : ${oname}Builder) : Resource[M, $oname] = Resource.fromAutoCloseable(asyncM.delay(builder.build()))
       |  def ${oname}OpResource(builder: ${oname}Builder) =${oname}Resource(builder).map(create)
       |  def create(client : $oname) : ${oname}Op[M] = new ${oname}Op[M] {
       |
       |    // domain-specific operations are implemented in terms of `primitive`
       |${ctors[A].map(_.fImpl).mkString("\n")}
       |
       |  }
       |""".trim.stripMargin
  }

  def interpreterDef(c: Class[_]): String = {
    val oname  = c.getSimpleName // original name, without name mapping
    val sname  = toScalaType(c)
    val opname = s"${oname}Op"
    val ioname = s"${oname}IO"
    val mname  = oname.toLowerCase
    s"lazy val ${oname}Interpreter: ${oname}Interpreter = new ${oname}Interpreter { }"
  }

  // template for a kleisli interpreter
  def kleisliInterpreter: String =
    s"""
      |package $pkg
      |
      |// Library imports
      |import cats.data.Kleisli
      |import cats.effect.{ Async, Blocker, ContextShift,  Resource }
      |${managed
      .map(_.getSimpleName)
      .map(oname => s"import software.amazon.awssdk.services.$awsService.${oname}Builder")
      .mkString("\n")}
      |import software.amazon.awssdk.services.$awsService.model._
      |
      |import java.util.concurrent.CompletionException
      |
      |// Types referenced
      |${managed.map(ClassTag(_)).flatMap(imports(_)).distinct.sorted.mkString("\n")}
      |
      |
      |object Interpreter {
      |
      |  @deprecated("Use Interpreter[M]. blocker is not needed anymore", "3.2.0")
      |  def apply[M[_]](b: Blocker)(
      |    implicit am: Async[M],
      |             cs: ContextShift[M]
      |  ): Interpreter[M] =
      |    new Interpreter[M] {
      |      val asyncM = am
      |      val contextShiftM = cs
      |    }
      |    
      |  def apply[M[_]](
      |    implicit am: Async[M],
      |             cs: ContextShift[M]
      |  ): Interpreter[M] =
      |    new Interpreter[M] {
      |      val asyncM = am
      |      val contextShiftM = cs
      |    }
      |
      |}
      |
      |// Family of interpreters into Kleisli arrows for some monad M.
      |trait Interpreter[M[_]] { outer =>
      |
      |  implicit val asyncM: Async[M]
      |
      |  // to support shifting blocking operations to another pool.
      |  val contextShiftM: ContextShift[M]
      |
      | ${managed.map(interpreterDef).mkString("\n  ")}
      |  // Some methods are common to all interpreters and can be overridden to change behavior globally.
      | 
      |  def primitive[J, A](f: J => A): Kleisli[M, J, A] = Kleisli(a => primitive1(f(a)))
      |
      |  def primitive1[J, A](f: =>A): M[A] = asyncM.delay(f)
      |
      |  def eff[J, A](fut: J => CompletableFuture[A]): Kleisli[M, J, A] = Kleisli(a => eff1(fut(a)))
      |
      |  def eff1[J, A](fut: =>CompletableFuture[A]): M[A] =
      |    asyncM.guarantee(
      |      asyncM
      |        .async[A] { cb =>
      |          fut.handle[Unit] { (a, x) =>
      |            if (a == null)
      |              x match {
      |                case t: CompletionException => cb(Left(t.getCause))
      |                case t                      => cb(Left(t))
      |              }
      |            else
      |              cb(Right(a))
      |          }
      |          ()
      |        }
      |    )(contextShiftM.shift)
      |
      |
      |  // Interpreters
      |${managed.map(ClassTag(_)).map(kleisliInterp(_)).mkString("\n")}
      |${managed.map(ClassTag(_)).map(fInterp(_)).mkString("\n")}
      |
      |}
      |""".trim.stripMargin

  def gen(base: File): Seq[java.io.File] = {
    import java.io._
    log.info("Generating tagless algebras into " + base)
    val fs = managed.map { c =>
      base.mkdirs
      val mod  = module(ClassTag(c))
      val file = new File(base, s"${c.getSimpleName}Op.scala")
      val pw   = new PrintWriter(file)
      pw.println(mod)
      pw.close()
      log.info(s"${c.getName} -> ${file.getName}")
      file
    }
    val ki = {
      val file = new File(base, s"Interpreter.scala")
      val pw   = new PrintWriter(file)
      pw.println(kleisliInterpreter)
      pw.close()
      log.info(s"... -> ${file.getName}")
      file
    }
    ki :: fs
  }

}
