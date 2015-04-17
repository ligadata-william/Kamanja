*******************************************
Install the FataFatClusterInstall installer 
*******************************************

The current trunk/SampleApplication/ClusterInstall scripts should be copied to a directory on your PATH.  Verify they are executable and if not perform the appropriate 'chmod' command on them.  

The NodeInfoExtract-1.0 application, used by the FataFatClusterInstall.sh script, also mus be on your PATH.  It can be found in the trunk/Utils/NodeInfoExtract/target/scala-2.10 directory.

*******************************************
Pre-requisites before using the installer 
*******************************************

See <some wiki> for some details regarding what software is expected to be available and configured appropriately.

***********************************
Install a New Cluster (from source)
***********************************

1) Run the install script without arguments to see what is required.  For example,

	rich@pepper:~/github/Fatafat/trunk$ FataFatClusterInstall.sh

	Problem: Incorrect number of arguments

	Usage if building from source:
	      FataFatClusterInstall.sh --ClusterId <cluster name identifer> 
	                               --MetadataAPIConfig  <metadataAPICfgPath>  
	                               --KafkaInstallPath <kafka location>
	                               [ --NodeConfigPath <engine config path> ]
	                               [ --WorkingDir <alt working dir>  ]
	Usage if deploying tarball:
	      FataFatClusterInstall.sh --ClusterId <cluster name identifer> 
	                               --MetadataAPIConfig  <metadataAPICfgPath>  
	                               --TarballPath <tarball path>
	                               [ --NodeConfigPath <engine config path> ]
	                               [ --WorkingDir <alt working dir>  ]

	  NOTES: Only tar'd gzip files are supported for the tarballs at the moment.
	         If the NodeConfigPath is not supplied, the MetadataAPIConfig will be assumed to already have the cluster information
	         in it.  The working directory, by default, is /tmp.  If such a public location is abhorrent, chose a private one.  It
	         must be an existing directory and readable by this script, however
	         If both the KafkaInstallPath and the TarballPath are specified, the script fails.
	         If neither the KafkaInstallPath or TarballPath  is supplied, the script will fail. 
	         A ClusterId is a required argument.  It will use only the nodes identified with that cluster id.

	         In addition, the NodeInfoExtract application that is used by this installer to fetch cluster node configuration  
	         information must be on the PATH.  It is found in the trunk/Utils/NodeInfoExtract/target/scala-2.10/ 

That is the message you will get.  As you can see, there were insufficient arguments submitted.  What will be required for building the cluster from source will be a ClusterId of one of the cluster's defined in the metadata cache OR if that has not yet been accomplished, one can specifty the cluster configuration json file to use that describes the cluster.  In the latter case (when cluster metadata not present), the NodeConfigPath file's cluster identifier MUST be the same as the one you mention on the command line via the ClusterId parameter value.

Since this is a source build, the kafka installation must be specified.  This is required by the easyinstaller script that is used to build a single instance of the installation on the local (build) machine.  Unless you are particularly paranoid, the WorkingDir parameter may be omitted as it defaults to /tmp, available on all linux boxes.

Important: the build must be run from the trunk of the local repo that is to be built and deployed with the installer.

2) About the MetadataAPIConfig file

For the installation, the metadata configuration identifies some very important parameters needed by the installer. The primary one is the installation directory where all installations will be placed on ALL machines in the cluster node specification used.  It also identifies the location of the metadata cache that contains node configuration information for the cluster being installed.  

Note that when the NodeConfigPath is not specified, the installer expects to find a Cluster declaration in the metadata located with the MetadataSchemaName, MetadataStoreType, and MetadataLocation values found in this file.  For example, if the cluster to be installed is going to use a metadata cache based upon Cassandra, the parameters in the file would look like:

	MetadataSchemaName=metadata
	MetadataStoreType=cassandra
	MetadataLocation=localhost

Clearly the location could be specified as something other than localhost.  You can use an ip4 address there that locates the cassandra data store.  For Cassandra installations that run on multiple clusters, just choose one of the ips for now.  We may allow multiple ip addresses for the Metadata location in the future.

The easy installer project has several sample template configuration files, including one that uses HashMap, appropriate for simple test clusters, and one that uses Cassandra where the metadata is assumed to be on the localhost.  See trunk/SampleApplication//EasyInstall/template/config "properties" files ... ClusterCfgMetadataAPIConfig_ Cassandra _ Template.properties and ClusterCfgMetadataAPIConfig_Template.properties for examples.  You could take a copy of one of these and fill it with the appropriate location information for your MetadataAPIConfig parameter.

3) About the NodeConfigPath file

If the MetadataAPIConfig can supply all of that information, why would we need a file that explicitly describes the node configuration (i.e., the NodeConfigPath value)?  Flexibility mostly.  It is possible to bootstrap a cluster, including the metadata cache content itself.  It is not necessary to have the cluster definition or anything else in a pre-existing metadata cache if desired.

There is an example in trunk/SampleApplication//EasyInstall/template/config/ClusterConfig _ Template.json.  Like the metadata configuration file, a copy of this could be made and configured for a NodeConfigPath parameter.  If this is done, the substutions done for {InstallDirectory} in particular need to match your ROOT_DIR parameter in the MetadataAPIConfig.  There are other {variables} that need to have legitimate values for the nodes participating in your installation as well, like {ScalaInstallDirectory} and {JavaInstallDirectory}.  

4) About the KafkaInstallPath parameter

When building from source, the location of your local build machine kafka installation is required because the cluster installer uses the easyInstallFatafat.sh script which needs it to configure CreateQueues.sh, WatchOutputQueue.sh, WatchStatusQueue.sh and WatchInputQueue.sh.  

Note: This points out that when multiple machines are to be used for the cluster installation, ALL machines must have the Kafka installed in the same location.  

5) About WorkingDir

This defaults to /tmp.  Unless you are particularly paranoid that others with access to the machines to receive the installation will foul your installation by tampering with the files in this public location, it does not need to be specified.

6) An example invocation of the FataFatClusterInstall.sh:

	rich@pepper:~/github/Fatafat/trunk$ FataFatClusterInstall.sh --ClusterId ligadata1 --MetadataAPIConfig ClusterCfgMetadataAPIConfigCass.properties --KafkaInstallPath ~/tarballs/kafka/2.10/kafka_2.10-0.8.1.1 --NodeConfigPath ClusterConfig.json


************************************
Install a New Cluster (from tarball)
************************************

1) Let's focus in on the parameters (snipped from the prior section) that deal with tarball installation:

	Usage if deploying tarball:
	      FataFatClusterInstall.sh --ClusterId <cluster name identifer> 
	                               --MetadataAPIConfig  <metadataAPICfgPath>  
	                               --TarballPath <tarball path>
	                               [ --NodeConfigPath <engine config path> ]
	                               [ --WorkingDir <alt working dir>  ]

All of the parameter descriptions common with the "source" installation in the previous section are sufficiently explained there.  Refer to those descriptions.  The only new one is the TarballPath.  It will be considered in the next several points.

2) Why use a tarball installation?

Depending on the nature of the organization deploying Fatafat clusters, the team that designs and creates the Fatafat cluster may not necessarily be the one that deploys the cluster.  Similarly, the team that deploys the cluster does not have permissions and/or knowledge to build the cluster software from source.  In these cases, a tarball installation is recommended.

3) The basic steps to create the tarball are:

	a) Run the easyInstallFatafat.sh script on the build machine.  This will create a local installation at the specified <install dir> location.  See the easyInstallFatafat.sh documentation for more details.

	b) Currently only tar'd and gzip'd tarballs are supported.  To create the "tgz" archive for your <install dir> content, change directory to <install dir>'s parent directory and issue 'tar cvzf someTarbBallName.tar.gz <install dir>'.  This tar.gz archive's file path will be the file to specify for the TarballPath value.

	c) In practice, you will want to supply some meaningful information in the tarball name like the release tag from your repo's build, a timestamp, etc.

4) Once created, invoke your FataFatClusterInstall.sh command.  For example,

	rich@pepper:~/github/Fatafat/trunk$ FataFatClusterInstall.sh --ClusterId ligadata1 --MetadataAPIConfig ClusterCfgMetadataAPIConfigCass.properties --TarballPath ~/fatafatBuilds/someTarbBallName.tar.gz --NodeConfigPath ClusterConfig.json

******************
What can go wrong?  
******************

1) Short answer: lots.  Read on for an enumeration of some of the most common issues.

2) Your cluster configuration must refer to valid ip addresses that are reachable from the build/deploy machine.  The paths in the configuration files need to be legitimateand paths.

3) The script uses scp and ssh liberally to make the installation.  The script expects a "passphrase-less" access to the machines in the target cluster declaration, including the local node if it is to participate in the installation.  If this is not the case, there will be errors when the tarball that is created from source (or supplied on the command line) is copied to the password protected node.  See <some wiki page> for how to correct this and what is expected for Fatafat installations in terms of ssh configuration and access.

4) If you expect to have success with the FataFat cluster install script, it is important to use the same directory topology for your critical components.  These include kafka, zookeeper, cassandra, hbase, scala, and java to name a few.

5) Parameters supplied to the FataFatClusterInstall.sh need to be cogent.  There are roughly 15 semantic checks currently done on the parameters to the script.  If any problems are detected, the script stops and a meaningful message is written to the console.  They are for the most part self-explanatory, describing with some precision what the problem is.

6) "I tried to run the cluster after installing it, but an error was thrown."  It is important to realize that the installer does not configure your cluster installation or the applications that are to run on it.  That work must be done after (or if using HBase or Cassandra, before or after) the cluster is installed.  The FataFatClusterInstall.sh's job is to push the installation to the appropriate machines based upon the cluster configuration given to it via parameters.  Nothing more is done.

7) Installing the correct versions of the software needed to build and run the Fatafat cluster is exceedingly important.  For example, certain libraries upon which Fatafat depends prevent us from simply using the latest versions of the Scala compiler.  Always check <some wiki page> for what the requirements are for the version you wish to install.

8) The NodeConfigPath is both your friend and your enemy.  If you supply one, it had better NOT be defined in your metadata cache already.  This is principally a problem only when metadata is maintained in Cassandra or HBase.  There will be errors and the installation will fail.  It is intended principally for testing builds and doing development work (typically with a TreeMap or HashMap metadata store... see <some wiki page> for more information about that).  A mature Fatafat installation will likely already have the cluster declaration installed in the cache before the cluster installation takes place, making the NodeConfigPath unnecessary.

9) This list will be augmented with additional issues as this software is experienced by others and they report them.

10) The script can't find NodeInfoExtract-1.0.  What should I do?  Be sure that this application is on your PATH.  See the "Install the FataFatClusterInstall installer" section above for more details.