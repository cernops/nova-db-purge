Nova DB Purge
=============

What is it?
-----------
It's a small tool that purges nova database per date.
It creates a file with all instances removed to be used for child cells purge.


How to use it?
--------------
To see the available options run:

python nova-db-purge -h

There 5 optional arguments: <br />
--date DATE      Remove deleted instances until this date
--file FILE      Remove deleted instances defined in the file
--cell CELL      Remove instances that belong to cell
--dryrun         Don't delete instances
--config CONFIG  Configuration file


Examples
--------

Remove all instances deleted before "2015-02-01 00:00:00". <br />
In a cell environment should be used in the parent cell. <br />
python cern-db-purge --date "2015-02-01 00:00:00" --config nova.conf


Remove all instances that belong to cell "child_cell_01" that have a reference in the file "delete_these_instances.txt".  <br />
In a cell environment should be used in the child cells.  <br />
python cern-db-purge --file "delete_these_instances.txt" --cell 'top_cell!child_cell_01' --config nova.conf


Nova versions supported
-----------------------
We use it in Icehouse.


Bugs and Disclaimer
-------------------
Bugs? Oh, almost certainly.

This tool was written to be used in the CERN Cloud Infrastructure and 
it has been tested only in our environment.

Since it updates nova DB use it with extremely caution.
