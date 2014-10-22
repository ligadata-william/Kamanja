package com.ligadata.ZooKeeper

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

object CreateClient
{
    def  createSimple(connectionString:String) :  CuratorFramework = {
        // these are reasonable arguments for the ExponentialBackoffRetry. The first
        // retry will wait 1 second - the second will wait up to 2 seconds - the
        // third will wait up to 4 seconds.
        val retryPolicy = new ExponentialBackoffRetry(1000, 3)

        // The simplest way to get a CuratorFramework instance. This will use default values.
        // The only required arguments are the connection string and the retry policy
        val curatorZookeeperClient = CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
	curatorZookeeperClient.start
	curatorZookeeperClient.getZookeeperClient.blockUntilConnectedOrTimedOut
	curatorZookeeperClient
    }

    def  createWithOptions(connectionString:String, 
			   retryPolicy:RetryPolicy, 
			   connectionTimeoutMs:Int, 
			   sessionTimeoutMs:Int) : CuratorFramework = {
        // using the CuratorFrameworkFactory.builder() gives fine grained control
        // over creation options. See the CuratorFrameworkFactory.Builder javadoc
        // details
        CuratorFrameworkFactory.builder()
            .connectString(connectionString)
            .retryPolicy(retryPolicy)
            .connectionTimeoutMs(connectionTimeoutMs)
            .sessionTimeoutMs(sessionTimeoutMs)
            .build();
    }
}
