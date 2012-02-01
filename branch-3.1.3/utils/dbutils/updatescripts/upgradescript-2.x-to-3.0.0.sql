-- 
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--   
--     http://www.apache.org/licenses/LICENSE-2.0
--   
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
-- 

# please replace "OOZIEDB" to your schema name

UPDATE OOZIEDB.COORD_JOBS
SET STATUS = 'RUNNING',PENDING = 1
WHERE ID IN 
(
SELECT JOB_ID FROM COORD_ACTIONS WHERE JOB_ID IN (
SELECT ID FROM COORD_JOBS WHERE STATUS = 'SUCCEEDED') AND  (STATUS != 'FAILED' AND
STATUS != 'SUCCEEDED' AND STATUS != 'KILLED' AND STATUS != 'TIMEDOUT') 
);

UPDATE OOZIEDB.COORD_JOBS SET STATUS = 'RUNNING' WHERE STATUS = 'PREMATER';

UPDATE OOZIEDB.COORD_ACTIONS 
SET STATUS = 'SUSPENDED'
WHERE ID IN(
SELECT A.ID FROM COORD_ACTIONS A, WF_JOBS B WHERE A.EXTERNAL_ID = B.ID
AND B.STATUS = 'SUSPENDED' AND A.STATUS = 'RUNNING'
);
