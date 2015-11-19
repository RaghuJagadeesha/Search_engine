#!/bin/bash
sshpass -p PutPasswordHere ssh -o "StrictHostKeyChecking no" username@user.palmetto.clemson.edu << ENDHERE
	sh ranked.sh $1;
	exit
ENDHERE
