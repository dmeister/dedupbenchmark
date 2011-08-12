import sbt._
import java.io.File

class DedupTrProject(info: ProjectInfo) extends DefaultProject(info)
{  
  override def dependencyPath = "lib"
  override def mainSourceRoots = ("src")
  override def mainClass = Some("de.pc2.dedup.traffic.runner.OnlineTrafficRunner")
}
