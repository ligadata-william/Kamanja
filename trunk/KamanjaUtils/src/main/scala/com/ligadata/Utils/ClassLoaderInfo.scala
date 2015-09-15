/*
 * Copyright 2015 ligaDATA
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ligadata.Utils

import scala.collection.mutable.TreeSet
import scala.reflect.runtime.{ universe => ru }
import java.net.{ URL, URLClassLoader }
import org.apache.log4j.Logger

/*
 * Kamanja custom ClassLoader. We can use this as Parent first (default, which is default for java also) and Parent Last.
 *   So, if same class loaded multiple times in the class loaders hierarchy, 
 *     with parent first it gets the first loaded class.
 *     with parent last it gets the most recent loaded class.
 */
class KamanjaClassLoader(val systemClassLoader: URLClassLoader, val parent: KamanjaClassLoader, val currentClassClassLoader: ClassLoader, val parentLast: Boolean)
  extends URLClassLoader(if (systemClassLoader != null) systemClassLoader.getURLs() else Array[URL](), if (parentLast == false && parent != null) parent else currentClassClassLoader) {
  private val LOG = Logger.getLogger(getClass)

  override def addURL(url: URL) {
    LOG.debug("Adding URL:" + url.getPath + " to default class loader")
    super.addURL(url)
  }

  protected override def loadClass(className: String, resolve: Boolean): Class[_] = this.synchronized {
    LOG.debug("Trying to load class:" + className + ", resolve:" + resolve)

    if (parentLast) {
      try {
        return super.loadClass(className, resolve) // First try in local
      } catch {
        case e: ClassNotFoundException => {
          if (parent != null)
            return parent.loadClass(className) // If not found, go to Parent
          else
            throw e
        }
      }
    } else {
      return super.loadClass(className, resolve)
    }
  }
}

/*
 * KamanjaLoaderInfo is just wrapper for ClassLoader to maintain already loaded jars.
 */
class KamanjaLoaderInfo(val parent: KamanjaLoaderInfo = null, val useParentloadedJars: Boolean = false, val parentLast: Boolean = false) {
  // Parent class loader
  val parentKamanLoader: KamanjaClassLoader = if (parent != null) parent.loader else null

  // Class Loader
  val loader = new KamanjaClassLoader(ClassLoader.getSystemClassLoader().asInstanceOf[URLClassLoader], parentKamanLoader, getClass().getClassLoader(), parentLast)

  // Loaded jars
  val loadedJars: TreeSet[String] = if (useParentloadedJars && parent != null) parent.loadedJars else new TreeSet[String]

  // Get a mirror for reflection
  val mirror: reflect.runtime.universe.Mirror = ru.runtimeMirror(loader)
}

