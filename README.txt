Run maven package
take the dependencies jar file from target and copy over to an empty dir
copy over the config files too and modify conf files accordingly
zip up the contents of the dir and upload to beanstalk
The manifest file in the jar file will kick off SimulationDriver which sets up the connection and reading of config files.