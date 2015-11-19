#!/bin/bash
sshpass -p PutPasswordHere ssh -o "StrictHostKeyChecking no" username@user.palmetto.clemson.edu << ENDHERE
	cd ~/info/
	sh sample.sh $1;
	exit
ENDHERE
