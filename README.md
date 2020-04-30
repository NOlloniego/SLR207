# SLR207

For the sequential program:

Run it by passing as argument the text file to work with.

For the distributed program:

1) Choose a workig folder (for exemple /tmp/nolloniego) and copy the Slave.jar file into that folder. This will be the name of the folder to be created in the distant machines.

2) Run the Clean programm. Arguments: File with all the machines (If it desnt exist, a file with that name will be created), username for ssh connection and working folder.

3) Run the Deploy program. Arguments: File with all the machines, username for ssh connection and working folder.

4) Run the Master program. Arguments: File with all the machines, text to work with, working folder, username for ssh connection and amount of splits to generate.
