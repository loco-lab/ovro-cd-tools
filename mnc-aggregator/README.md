# mnc-aggregator
This package helps define interfaces to convert distributed etcd monitor points
into summary points for influx ingestions.

## Prerequisites
You must manually install the following packages before being able to use this repository

- setuptools_scm
- lwa_f (lwa f engine control software -- yes you need this to use mnc_python at all)
- casperfpga (a requirement of lwa_f)

all other packages can be automatically installed and built by running `pip install .`


## Adding new Monitor Points
This code makes heavy use of python's AbstractBaseClass system as an interface. New monitor points need to subclass `mnc_aggregator.interface.MonitorAggregator` class. The only method that should need to be overwritten is `aggregate_monitor_points`.

This method must provide the logic to read all MonitorPoints associated with an sybsystem and condense them into a single summary point. The method returns a list of AggregateMonitorPoints one for each `tag` in a system (e.g. one per snap, or one per X-Enginge pipelinehost).

After creating a new class in `mnc_aggregator.subsystems` it must be added to the list of classes in `mnc_aggregator.cli` called `MonitorClasses`. The command line tool uses this list to iterate through subsystems and create summary stats.