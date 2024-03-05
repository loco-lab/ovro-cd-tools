# mnc-aggregator
This package helps define interfaces to convert distributed etcd monitor points
into summary points for influx ingestions.

# prerequisites
You must manually install the following packages before being able to use this repository

- setuptools_scm
- lwa_f (lwa f engine control software -- yes you need this to use mnc_python at all)
- casperfpga (a requirement of lwa_f)

all other packages can be automatically installed and built by running `pip install .`