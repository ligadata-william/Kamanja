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

package com.ligadata.utils.clean

import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.ExponentialBackoffRetry

/**
 * Created by will on 11/4/15.
 */
object CleanZookeeper {
  private lazy val logger = org.apache.log4j.Logger.getLogger(this.getClass)
  private lazy val retryPolicy = new ExponentialBackoffRetry(1000, 3)

  def deletePath(zkInfo: ZooKeeperInfo): Unit = {
    val zkc = CuratorFrameworkFactory.newClient(zkInfo.connStr, 6000, 6000, retryPolicy)
    try {
      zkc.start()
      //if(zkc.checkExists().forPath(zkInfo.nodeBasePath) != null) {
      //  logger.warn(s"CLEAN-UTIL: Failed to find zookeeper path '${zkInfo.nodeBasePath}'. Skipping delete...")
      //}
      //else {
        logger.info("Deleting Zookeeper node " + zkInfo.nodeBasePath)
        zkc.delete().deletingChildrenIfNeeded.forPath(zkInfo.nodeBasePath)
        if (zkc.checkExists().forPath(zkInfo.nodeBasePath) != null) {
          throw new Exception("CLEAN-UTIL: Failed to delete zookeeper path " + zkInfo.nodeBasePath)
        }
      //}
    }
    finally {
      zkc.close()
    }
  }
}
