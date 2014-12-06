<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
<%@ page import="java.lang.management.ThreadInfo"%>
<%@ page import="org.apache.oozie.servlet.JVMInfo.JThreadInfo"%>
<%@page contentType="text/html; charset=UTF-8"%>

<jsp:useBean id="jvmInfo" class="org.apache.oozie.servlet.JVMInfo" scope="request">
    <jsp:setProperty name="jvmInfo" property="threadSortOrder" value='<%=request.getParameter("threadsort")%>' />
    <jsp:setProperty name="jvmInfo" property="cpuMonitorTime" value='<%=request.getParameter("cpuwatch")%>' />
</jsp:useBean>

<HTML>
<HEAD>
<TITLE>Oozie JVM Info</TITLE>
</HEAD>
<BODY>
    <h2>Memory Information:</h2>
    <ul>
        <li>Heap Memory: <br /> &nbsp;&nbsp;&nbsp;&nbsp;<%=jvmInfo.getHeapMemoryUsage()%></li>
        <li>NonHeap Memory: <br /> &nbsp;&nbsp;&nbsp;&nbsp; <%=jvmInfo.getNonHeapMemoryUsage()%></li>
        <li>ClassLoading: <br /> &nbsp;&nbsp;&nbsp;&nbsp; <%=jvmInfo.getClassLoadingInfo()%></li>
        <li>Threads: <br /> &nbsp;&nbsp;&nbsp;&nbsp; <%=jvmInfo.getThreadInfo()%></li>
    </ul>

    <h2>Thread Summary:</h2>
    <table cellpadding="5" cellspacing="5">
        <tr>
            <th>Thread</th>
            <th>State</th>
            <th>CPU Time (ns)</th>
            <th>User Time (ns)</th>
        </tr>
        <%
            for (JThreadInfo jthread : jvmInfo.getThreads()) {
                ThreadInfo thread = jthread.getThreadInfo();
        %>
        <tr>
            <td><a href="#<%=thread.getThreadId()%>"><%=thread.getThreadName()%></a></td>
            <td><%=thread.getThreadState()%></td>
            <td><%=jthread.getCpuTime()%></td>
            <td><%=jthread.getUserTime()%></td>
        </tr>
        <%
            }
        %>
    </table>

    <h2>Stack Trace of JVM:</h2>
    <%
        for (JThreadInfo jthread : jvmInfo.getThreads()) {
            ThreadInfo thread = jthread.getThreadInfo();
    %>
    <h4>
        <a name="<%=thread.getThreadId()%>"><%=thread.getThreadName()%>&nbsp;tid=<%=thread.getThreadId()%>&nbsp;
            <%=thread.getThreadState()%></a>
    </h4>
    <pre>
        <table>
        <%
            StackTraceElement[] traceElements = thread.getStackTrace();
                for (StackTraceElement traceElement : traceElements) {
        %>
            <tr>
                <td>&nbsp;&nbsp;&nbsp;</td>
                <td> at <%=traceElement%> </td>
            </tr>
        <%
             }
         %>
        </table>
        </pre>
    <%
        }
    %>
</BODY>
</HTML>