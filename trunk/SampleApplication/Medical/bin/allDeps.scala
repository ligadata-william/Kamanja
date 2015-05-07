#!/bin/bash
exec scala "$0" "$@"
!#

/***************************************************************************************
 * allDeps.scala processes a list of sbt projects, creating a discrete set of their
 * jar dependencies for the repo in the current working directory.
 *
 **************************************************************************************/

import scala.collection.mutable._
import scala.collection.immutable.Seq
import scala.io.Source
import sys.process._
import java.util.regex.Pattern
import java.util.regex.Matcher


object allDeps extends App {
  
    def usage : String = {
"""
allDeps.scala 

        Produce a list of all jar dependencies from the repo.

        allDeps.scala [--excludeProjects <list of comma delimited projects not to consider>]
      
"""
    }


    override def main (args : Array[String]) {


        //println("original args:")
        //args.foreach(arg => print(arg + " "))
        //println

        var projects : ArrayBuffer[String] = ArrayBuffer[String]()

        val arglist = args.toList
        type OptionMap = Map[Symbol, String]
        //println(arglist)
        def nextOption(map: OptionMap, list: List[String]): OptionMap = {
            list match {
              case Nil => map
              case "--excludeProjects" :: value :: tail =>
                nextOption(map ++ Map('excludeProjects -> value), tail)
              case _ =>
                projects += list.head
                nextOption(map, list.tail)
            }
        }
      
        val options = nextOption(Map(), arglist)

        val excludedProjects : String = if (options.contains('excludeProjects)) options.apply('excludeProjects) else null
        val projectsToConsider : Array[String] = if (excludedProjects != null) {
            println(s"excluded projects = $excludedProjects")
            projects.filter(proj => (! excludedProjects.contains(proj))).toSeq.sorted.toArray
        } else {
            projects.toSeq.sorted.toArray
        }

        
        
        //println(s"projects to be processed : ${projectsToConsider.mkString(",")}")

        val pwd : String = Process(s"pwd").!!.trim
  		  if (pwd == null) {
  			   println(s"Error: Unable to obtain the value of the present working directory")
  			   return
  		  } else {
            if (pwd.split('/').last != "trunk")
              println("Please run this script from a repo trunk directory")
        }

        var allDepJars : scala.collection.mutable.Set[String] = scala.collection.mutable.Set[String]()
        projectsToConsider.foreach(arg => {
            println(s"processing project $arg...")

            val depJarsCmd = s"sbtProjDependencies.scala --sbtDeps ${'"'}`sbt 'show $arg/fullClasspath' | grep 'List(Attributed'`${'"'} --emitCp 1"
            println(s"...cmd=$depJarsCmd")
            val depJarsCmdSeq : Seq[String] = Seq("bash", "-c", depJarsCmd)
            val depJarsStr : String = Process(depJarsCmdSeq).!!.trim
            if (depJarsStr != null && depJarsStr.size > 0) {
                val projDepJars : Array[String] = depJarsStr.split(':')
                projDepJars.foreach(dep => {
                    if (! dep.contains("target/streams/$global/assemblyOption"))
                        allDepJars.add(dep)
                })
            }

        })

        println(s"Dependency jars for repo found at $pwd")
        allDepJars.foreach(jarPath => println("cp " + jarPath + " $systemlib"))
	}
}


allDeps.main(args)

