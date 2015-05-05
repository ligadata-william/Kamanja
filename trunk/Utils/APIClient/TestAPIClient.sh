# Add objects
# Cluster Config
APIClient -l DEBUG -t PUT -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/config/ClusterConfig.json -u https://localhost:8081/api/UploadConfig 
# Containers
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/container/CoughCodes.json -u https://localhost:8081/api/Container
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/container/DyspnoeaCodes.json -u https://localhost:8081/api/Container
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/container/EnvCodes.json -u https://localhost:8081/api/Container
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/container/SmokeCodes.json -u https://localhost:8081/api/Container
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/container/SputumCodes.json -u https://localhost:8081/api/Container
# Messages
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/message/inpatientclaim.json -u https://localhost:8081/api/Message
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/message/outpatientclaim.json -u https://localhost:8081/api/Message
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/message/hl7.json -u https://localhost:8081/api/Message
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/message/beneficiary.json -u https://localhost:8081/api/Message
# Models
APIClient -l DEBUG -t POST -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -f /media/home2/installFatafat/input/application1/metadata/model/COPDv1.xml -u https://localhost:8081/api/Model
#
# Fetch Objects
# Leader
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/leader
# config objects
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/config/nodes
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/config/adapters
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/config/clusters
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/config/all
# Container Objects
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.coughcodes.000000000001000000
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.dyspnoeacodes.000000000001000000
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.envcodes.000000000001000000
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.smokecodes.000000000001000000
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.sputumcodes..000000000001000000
# Message Objects
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/message/system.inpatientclaim.000000000001000000
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/message/system.outpatientclaim.000000000001000000
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/message/system.hl7.000000000001000000
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/message/system.beneficiary.000000000001000000
# Model objects
APIClient -l DEBUG -t GET -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/model/system.copdriskassessment.000000000001000000
# Load a Jar file
APIClient -l DEBUG -t PUT -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/UploadJars?name=shire-core -f /media/home2/Downloads/shiro-core-1.1.0.jar
# Delete Objects

# Model objects
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/model/system.copdriskassessment.000000000001000000
# Message Objects
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/message/system.beneficiary.000000000001000000
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/message/system.hl7.000000000001000000
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/message/system.inpatientclaim.000000000001000000
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/message/system.outpatientclaim.000000000001000000
# Container Objects
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.coughcodes.000000000001000000
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.dyspnoeacodes.000000000001000000
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.envcodes.000000000001000000
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.smokecodes.000000000001000000
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/container/system.sputumcodes..000000000001000000
# config objects
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/config/nodes
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/config/adapters
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/config/clusters
APIClient -l DEBUG -t DELETE -i lonestarr -p vespa -r goodguy -n localhost:8081,localhost:8082 -u https://localhost:8081/api/config/all
