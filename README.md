Fatafat
===
###Overview

Fatafat is an online learning engine platform that provides near real-time processing of streaming data, and produces opportunities or threats based on your business rule sets and historical data.   The resulting alerts are pushed into a messaging queue which can be consumed via downstream applications such as SMS portals, mobile applications, dashboards, business action solutions or automated expert action systems.   

###What Fatafat does

Fatafat is an advanced PMML scoring engine.  Through a combination of result scoring and rule set strategy, Fatafat can provide high speed processing of streaming data, using your rule set, and produce actionable threats and/or opportunities.  It does these things:

By using its enhanced and extended PMML modeling capabilities, Fatafat provides a streamlined method for designing and implementing your business questions and rulesets in PMML models.  Incoming messages are consumed by the engine, which uses advanced custom complex data types in various combinations to process messages against multiple models and rule sets to produce insight.   
	
Fatafat allows you to quickly create complex rule sets, and programmatically define the input data and output required, without writing new PMML.  You define Input, Ruleset, and Output.  We provide the data transformation, rule set engine and output mechanism. 

Fatafat simplifies the complexity of PMML development, and provides layering of algorithms, rules and models that can consume other models at runtime so that your results are more meaningful. You can then programmatically handle those results, speeding along both actions and decisions according to your business needs.

###Use Cases

Fatafat is helping one financial partner reduce overnight global system transaction analysis from over 10 minutes to under a few seconds, resulting in millions saved each period. 

Medical applications are using Fatafat to create a system that analyzes physician notes, test results and patient input to identify certain combinations of information that may alert health care practitioners to new or changed conditions.

###How to get Started

	1. Download latest Fatafat binaries from here:
	
		http://www.ligadata.com/releases/bin/fatafat_install_v101.zip

	2. Unzip the downloaded file
	
		a. On Mac OS just double click the fatafat_install_v101.zip file if it’s not already unzipped.
		b. On Windows use ‘winrar’ or ‘winzip’ to unzip it. 
		c. On other Linux based OS, use command “unzip fatafat_install_v101.zip -d destination_folder”. 
	   	[Use “sudo apt-get install unzip” if unzip not found]


	3. Get started with setup instructions found in InstallDirectory/documentation/Fatafat1_0SetupRunGuide.pdf

	4. Ask questions, explore, and help improve Fatafat community at:
	
		https://groups.google.com/a/ligadata.com/forum/?hl=en#!forum/fatafat_userforum
		[You may need to ask to join the group if you’re not already a member]

###To Install Fatafat from Source Code
- Clone Fatafat/trunk
- Please see the Setup and Running guide at ~\LigaData\Fatafat\trunk\Documentation for installation details, and brief instructions to run the engine.
