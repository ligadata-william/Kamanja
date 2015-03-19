It is currently unnecessary to run the core udf json file through the metadata api.  These are currently in the metadata bootstrap.
Should that change, and function defs are no longer added there, then the PmmlUdfs core udf metadata should be regenerated and loaded like
the other user defined function libs. 
