#!/bin/bash

ARCHIVE=$DATA_PATH/expert-groups
EXGROUPS=$ARCHIVE/groups-`date +%Y%m%d`.xml

mkdir -p $ARCHIVE
curl -o $EXGROUPS "http://ec.europa.eu/transparency/regexpert/openXMLDirect.cfm"

echo "Uploading to S3..."
aws s3 cp $EXGROUPS s3://archive.pudo.org/expert-groups/groups-`date +%Y%m%d`.xml
aws s3 cp $EXGROUPS s3://archive.pudo.org/expert-groups/groups-latest.xml

python scraper.py $EXGROUPS

rm $EXGROUPS

