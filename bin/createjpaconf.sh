#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

SCRIPT_DIR=$(dirname $0)
CURRENT_DIR=$(pwd)
DBTYPE=
USERNAME=
PASSWORD=
DBURL=

function usage()
{
    echo >&2 \
	  "usage: $0 [-ddbtype] [-uusername] [-ppassword] [-lurl]"
    exit 1
}

while getopts :d:u:p:l: opt
do
    case "$opt" in
      d)  DBTYPE="$OPTARG";;
      u)  USERNAME="$OPTARG";;
      p)  PASSWORD="$OPTARG";;
      l)  DBURL="$OPTARG";;
      \?) #unknown flag
          usage;;
    esac
done

# check all arguments are given:
[ -z "$DBTYPE" ] && usage
[ -z "$USERNAME" ] && usage
[ -z "$DBURL" ] && usage

DriverClassName=
Url=

if [ "$DBTYPE" == "oracle" ]; then
  DriverClassName=oracle.jdbc.driver.OracleDriver
  Url=jdbc:oracle:thin:@${DBURL}
  DB_ISOLATION="read-committed"
elif [ "$DBTYPE" == "mysql" ]; then
  DriverClassName=com.mysql.jdbc.Driver
  Url=jdbc:mysql://${DBURL}
  DB_ISOLATION="repeatable-read"
else
  DriverClassName=org.hsqldb.jdbcDriver
  Url="jdbc:hsqldb:${DBURL};create=true"
  DB_ISOLATION="read-committed"
fi

CONNECTSTRING="DriverClassName=${DriverClassName},Url=${Url},Username=${USERNAME},Password=${PASSWORD},MaxActive=100"

#create persistence.xml
mkdir ${SCRIPT_DIR}/tmp
cat << EOF-persistence.xml > ${SCRIPT_DIR}/tmp/persistence.xml
<?xml version="1.0" encoding="UTF-8"?>
<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->
<persistence xmlns="http://java.sun.com/xml/ns/persistence"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    version="1.0">

    <!--
        We need to enumerate each persistent class first in the persistence.xml
        See: http://issues.apache.org/jira/browse/OPENJPA-78
    -->
    <persistence-unit name="none" transaction-type="RESOURCE_LOCAL">
        <!--
          <mapping-file>oozie/schema.xml</mapping-file>
        -->
    </persistence-unit>

    <!--
        A persistence unit is a set of listed persistent entities as well
        the configuration of an EntityManagerFactory. We configure each
        example in a separate persistence-unit.
    -->
    <persistence-unit name="oozie" transaction-type="RESOURCE_LOCAL">
        <!--
            The default provider can be OpenJPA, or some other product.
            This element is optional if OpenJPA is the only JPA provider
            in the current classloading environment, but can be specified
            in cases where there are multiple JPA implementations available.
        -->
        <!--
        <provider>
            org.apache.openjpa.persistence.PersistenceProviderImpl
        </provider>
        -->

        <!-- We must enumerate each entity in the persistence unit -->
        <class>org.apache.oozie.WorkflowActionBean</class>
        <class>org.apache.oozie.WorkflowJobBean</class>
        <class>org.apache.oozie.CoordinatorJobBean</class>
        <class>org.apache.oozie.CoordinatorActionBean</class>
        <class>org.apache.oozie.SLAEventBean</class>
        <class>org.apache.oozie.client.rest.JsonWorkflowJob</class>
        <class>org.apache.oozie.client.rest.JsonWorkflowAction</class>
        <class>org.apache.oozie.client.rest.JsonCoordinatorJob</class>
        <class>org.apache.oozie.client.rest.JsonCoordinatorAction</class>
        <class>org.apache.oozie.client.rest.JsonSLAEvent</class>

        <properties>
            <!--
                We can configure the default OpenJPA properties here. They
                happen to be commented out here since the provided examples
                all specify the values via System properties.
            -->
            <property name="openjpa.ConnectionDriverName" value="org.apache.oozie.util.db.InstrumentedBasicDataSource"/>

            <property name="openjpa.ConnectionProperties" value="$CONNECTSTRING"/>
            <property name="openjpa.MetaDataFactory" value="jpa(Types=org.apache.oozie.WorkflowActionBean;
                org.apache.oozie.WorkflowJobBean;
                org.apache.oozie.CoordinatorJobBean;
                org.apache.oozie.CoordinatorActionBean;
                org.apache.oozie.SLAEventBean;
                org.apache.oozie.client.rest.JsonSLAEvent;
                org.apache.oozie.client.rest.JsonWorkflowJob;
                org.apache.oozie.client.rest.JsonWorkflowAction;
                org.apache.oozie.client.rest.JsonCoordinatorJob;
                org.apache.oozie.client.rest.JsonCoordinatorAction)"></property>

            <property name="openjpa.jdbc.SynchronizeMappings" value="buildSchema(ForeignKeys=true)"/>
            <property name="openjpa.DetachState" value="fetch-groups(DetachedStateField=true)"/>
            <!--property name="openjpa.FlushBeforeQueries" value="false" /-->
            <property name="openjpa.LockManager" value="pessimistic"/>
            <property name="openjpa.ReadLockLevel" value="read"/>
            <property name="openjpa.WriteLockLevel" value="write"/>
            <property name="openjpa.jdbc.TransactionIsolation" value="$DB_ISOLATION"/>
            <property name="openjpa.jdbc.DBDictionary" value="UseGetBytesForBlobs=true"/>
            <property name="openjpa.jdbc.DBDictionary" value="UseSetBytesForBlobs=true"/>
            <property name="openjpa.jdbc.DBDictionary" value="BlobBufferSize=500000"/>
            <property name="openjpa.jdbc.DBDictionary" value="batchLimit=50"/>
            <!-- property name="openjpa.Log" value="File=/tmp/sql.log, DefaultLevel=TRACE, SQL=TRACE"/-->
            <!-- property name="openjpa.DynamicEnhancementAgent" value="false"/-->
            <property name="openjpa.RuntimeUnenhancedClasses" value="supported"/>
        </properties>
    </persistence-unit>

</persistence>
EOF-persistence.xml

cd ${SCRIPT_DIR}
cp tmp/persistence.xml ../webapp/src/main/resources/META-INF/

#oracle
if [ "$DBTYPE" == "oracle" ]; then
  cp ../webapp/src/main/resources/META-INF/orm.xml.oracle ../webapp/src/main/resources/META-INF/orm.xml
#mysql
elif [ "$DBTYPE" == "mysql" ]; then
  cp ../webapp/src/main/resources/META-INF/orm.xml.mysql ../webapp/src/main/resources/META-INF/orm.xml
#hsql
else
  cp ../webapp/src/main/resources/META-INF/orm.xml.hsql ../webapp/src/main/resources/META-INF/orm.xml
fi


rm -fr tmp
