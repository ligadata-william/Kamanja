
WHAT IS HERE?

This folder contains message test file for the Barclay's Alchemy model.  A subdirectory is also here that contains
data for producing the EnvContext container map data needed for the model.  Read on for more information.

DETAILS

The following csv files contain messages for the BankPoc testing.  The file names give clue as to whether they will
produce an "Alert" or "NoAlert".  The "OneCustomer*" files have only one customer ... useful for debugging.  At this time,
only the following files are usable.  

	OneCustomerTransactionsAlerts_LBOD123EB2.csv 
	OneCustomerTransactionsAlerts_OD123EB12.csv
	OneCustomerTransactionsAlerts_OD123.csv

This is fluid and changing, but during development, it was discovered that the content of the original test data was not
correct.  The other .csv files remain to be changed to data content similar to that found in the files above.


There are corresponding OnLEPManager config files that can be used to invoke the .gz counterparts of the files mentioned
above.  They are:

	BankPoc_LBOD123EB2.cfg
	BankPoc_OD123EB12.cfg
	BankPoc_OD123.cfg

There are other config files, but until the data files to which they refer is corrected, they are not usable.  To ease 
managing the data creation, there are SPREADSHEET versions (.xls) of these files.  The spreadsheet version tabulates
the running balance using the current debit/credit entry... the data can then be copied to the csv version after manipulation
It is important for the data to have this integrity.  The alchemy model depends on the computation of the "previous" balance.


There is the original data and modified data (respectively) in spreadsheet form:

	transactionsOriginal.xlsx
	transactions.xlsx

This data HAS NOT BEEN CONVERTED as of yet to the correct values (similar to that used by
OneCustomerTransactionsAlerts_LBOD123EB2.csv for example).

Note on how to construct the containers EnvContext maps for the BankPoc (also in the installOnLEP script above):

	ConstructingAlchemyContainers.txt

The subdirectory EnvContextContainerData contains the "dimensional" data that matches the message data.  These
csv files are consumed by the AlchemyKVInit application to produce (currently) MapDb kv store files.  There are
files for 

	AlertParameters
	AlertHistory
	CustomerPreferences
	TukTier

The kvDimensionalData.xls contains the working spreadsheet with tabs that produced each of the csv files.



