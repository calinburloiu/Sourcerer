#!/bin/bash

# =====================================================================
# Sourcerer: An infrastructure for large-scale source code analysis.
# Copyright (C) by contributors. See CONTRIBUTORS.txt for full list.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
# ===================================================================== 
# @author Sushil Bajracharya (bajracharya@gmail.com)

# run this from solr-server-pass(1|2) folder

if [ $# -ne 6 ] ; then
    echo 'This script requires 6 aruments: cluster_root_path pass_number java_home cluster_q_name solr_port project_ids'
    # echo 'Run this script from solr-server_pass(1|2)/'
    exit 0
fi

ROOT=$1
PASS=$2
JAVA_HOME=$3
Q=$4
PORT=$5
PROJ_IDS=$6

SOLR=$ROOT/solrbin/solr-server-pass$PASS

INDEXROOT=$ROOT/indexroot/pass$PASS
JOBSDIR=$ROOT/jobs/pass$PASS
SOLRLOGS=$ROOT/solrlogs/pass$PASS
JETTYLOGS=$ROOT/jettylogs/pass$PASS

RANGE=$PROJ_IDS

JETTYXML=$JOBSDIR"/jetty_"$RANGE".xml"
OUT=$JOBSDIR/$RANGE".stdout"
ERR=$JOBSDIR/$RANGE".stderr"

INDEXDIR=$INDEXROOT/$RANGE
mkdir $INDEXDIR

SOLRLOGDIR=$SOLRLOGS/$RANGE
mkdir $SOLRLOGDIR

JETTYLOGDIR=$JETTYLOGS/$RANGE
mkdir $JETTYLOGDIR

LPROP=$JOBSDIR/$RANGE".logging.properties"

sed "s#!SOLR_LOG_FOLDER!#$SOLRLOGDIR#g" $SOLR/logging.properties > $LPROP

SERVERID=$RANGE"_"$PASS
sed "s#!SERVER_ID!#$SERVERID#g" $SOLR/etc/jetty.xml > $JETTYXML

# make a copy of own Solr home
SOLR_HOME=$JOBSDIR"/"$RANGE"_solrhome"
mkdir $SOLR_HOME
cp -r $SOLR'/installation/solr/'* $SOLR_HOME'/'

OUT=$(echo $OUT|sed 's/,/_/g')
ERR=$(echo $ERR|sed 's/,/_/g')

qsub -v PATH -b y -o $OUT -e $ERR -q $Q -l mem_free=3G $ROOT/runindex.sh $INDEXDIR $LPROP $SOLR $PROJ_IDS $ROOT $PASS $JETTYLOGDIR $JAVA_HOME $JETTYXML $PORT > $JOBSDIR/$RANGE".qsub.out"