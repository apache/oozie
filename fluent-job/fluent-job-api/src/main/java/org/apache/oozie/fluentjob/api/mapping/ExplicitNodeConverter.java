/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.oozie.fluentjob.api.mapping;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.oozie.fluentjob.api.action.DistcpAction;
import org.apache.oozie.fluentjob.api.action.EmailAction;
import org.apache.oozie.fluentjob.api.action.FSAction;
import org.apache.oozie.fluentjob.api.action.GitAction;
import org.apache.oozie.fluentjob.api.action.Hive2Action;
import org.apache.oozie.fluentjob.api.action.HiveAction;
import org.apache.oozie.fluentjob.api.action.JavaAction;
import org.apache.oozie.fluentjob.api.action.MapReduceAction;
import org.apache.oozie.fluentjob.api.action.Node;
import org.apache.oozie.fluentjob.api.action.PigAction;
import org.apache.oozie.fluentjob.api.action.ShellAction;
import org.apache.oozie.fluentjob.api.action.SparkAction;
import org.apache.oozie.fluentjob.api.action.SqoopAction;
import org.apache.oozie.fluentjob.api.action.SshAction;
import org.apache.oozie.fluentjob.api.action.SubWorkflowAction;
import org.apache.oozie.fluentjob.api.generated.workflow.ACTION;
import org.apache.oozie.fluentjob.api.generated.workflow.ACTIONTRANSITION;
import org.apache.oozie.fluentjob.api.generated.workflow.FS;
import org.apache.oozie.fluentjob.api.generated.workflow.JAVA;
import org.apache.oozie.fluentjob.api.generated.workflow.MAPREDUCE;
import org.apache.oozie.fluentjob.api.generated.workflow.ObjectFactory;
import org.apache.oozie.fluentjob.api.generated.workflow.PIG;
import org.apache.oozie.fluentjob.api.generated.workflow.SUBWORKFLOW;
import org.apache.oozie.fluentjob.api.dag.DecisionJoin;
import org.apache.oozie.fluentjob.api.dag.ExplicitNode;
import org.apache.oozie.fluentjob.api.dag.NodeBase;
import org.apache.oozie.fluentjob.api.workflow.Credential;
import org.dozer.DozerConverter;
import org.dozer.Mapper;
import org.dozer.MapperAware;
import com.google.common.base.Preconditions;

import javax.xml.bind.JAXBElement;
import java.util.Map;

/**
 * A {@link DozerConverter} converting from {@link ExplicitNode} to JAXB {@link ACTION}.
 * <p>
 * Being the main entry point to Dozer framework, this class finds out the type of the actual
 * {@code ExplicitNode}, delegates to further {@code DozerConverter}s based on the type,
 * and connects the resulting JAXB objects based on the transitions between {@code ExplicitNode}s.
 */
public class ExplicitNodeConverter extends DozerConverter<ExplicitNode, ACTION> implements MapperAware {
    private static final ObjectFactory WORKFLOW_OBJECT_FACTORY = new ObjectFactory();

    private static final Map<Class<? extends Node>, Class<? extends Object>> ACTION_CLASSES = initActionClasses();

    private static Map<Class<? extends Node>, Class<? extends Object>> initActionClasses() {
        final ImmutableMap.Builder<Class<? extends Node>, Class<? extends Object>> builder = new ImmutableMap.Builder<>();

        builder.put(MapReduceAction.class, MAPREDUCE.class)
                .put(SubWorkflowAction.class, SUBWORKFLOW.class)
                .put(FSAction.class, FS.class)
                .put(EmailAction.class, org.apache.oozie.fluentjob.api.generated.action.email.ACTION.class)
                .put(DistcpAction.class, org.apache.oozie.fluentjob.api.generated.action.distcp.ACTION.class)
                .put(HiveAction.class, org.apache.oozie.fluentjob.api.generated.action.hive.ACTION.class)
                .put(GitAction.class, org.apache.oozie.fluentjob.api.generated.action.git.ACTION.class)
                .put(Hive2Action.class, org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION.class)
                .put(JavaAction.class, JAVA.class)
                .put(PigAction.class, PIG.class)
                .put(ShellAction.class, org.apache.oozie.fluentjob.api.generated.action.shell.ACTION.class)
                .put(SparkAction.class, org.apache.oozie.fluentjob.api.generated.action.spark.ACTION.class)
                .put(SqoopAction.class, org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION.class)
                .put(SshAction.class, org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION.class);

        return builder.build();
    }

    private Mapper mapper;

    public ExplicitNodeConverter() {
        super(ExplicitNode.class, ACTION.class);
    }

    @Override
    public ACTION convertTo(final ExplicitNode source, ACTION destination) {
        destination = ensureDestination(destination);

        mapAttributes(source, destination);

        mapTransitions(source, destination);

        mapActionContent(source, destination);

        return destination;
    }

    private ACTION ensureDestination(ACTION destination) {
        if (destination == null) {
            destination = WORKFLOW_OBJECT_FACTORY.createACTION();
        }
        return destination;
    }

    @Override
    public ExplicitNode convertFrom(final ACTION source, final ExplicitNode destination) {
        throw new UnsupportedOperationException("This mapping is not bidirectional.");
    }

    private Mapper checkAndGetMapper() {
        Preconditions.checkNotNull(mapper, "mapper should be set");
        return mapper;
    }

    @Override
    public void setMapper(final Mapper mapper) {
        this.mapper = mapper;
    }

    private void mapAttributes(final ExplicitNode source, final ACTION destination) {
        destination.setName(source.getName());

        final StringBuilder credBuilder = new StringBuilder();
        for (final Credential credential : source.getRealNode().getCredentials()) {
            if (credBuilder.length() > 0) {
                credBuilder.append(",");
            }
            credBuilder.append(credential.getName());
        }
        if (!Strings.isNullOrEmpty(credBuilder.toString())) {
            destination.setCred(credBuilder.toString());
        }

        final Integer retryInterval = source.getRealNode().getRetryInterval();
        if (retryInterval != null) {
            destination.setRetryInterval(retryInterval.toString());
        }

        final Integer retryMax = source.getRealNode().getRetryMax();
        if (retryMax != null) {
            destination.setRetryMax(retryMax.toString());
        }

        if (!Strings.isNullOrEmpty(source.getRealNode().getRetryPolicy())) {
            destination.setRetryPolicy(source.getRealNode().getRetryPolicy());
        }
    }

    private void mapTransitions(final ExplicitNode source, final ACTION destination) {
        // Error transitions are handled at the level of converting the Graph object to a WORKFLOWAPP object.
        final ACTIONTRANSITION ok = WORKFLOW_OBJECT_FACTORY.createACTIONTRANSITION();
        final NodeBase child = findNonDecisionNodeDescendant(source);

        ok.setTo(child == null ? "" : child.getName());

        destination.setOk(ok);
    }

    private NodeBase findNonDecisionNodeDescendant(final ExplicitNode source) {
        if (source.getChild() instanceof DecisionJoin) {
            return ((DecisionJoin) source.getChild()).getFirstNonDecisionJoinDescendant();
        }
        return source.getChild();
    }

    private void mapActionContent(final ExplicitNode source, final ACTION destination) {
        final Node realNode = source.getRealNode();

        Object actionTypeObject = null;
        if (ACTION_CLASSES.containsKey(realNode.getClass())) {
            final Class<? extends Object> mappedClass = ACTION_CLASSES.get(realNode.getClass());
            actionTypeObject = checkAndGetMapper().map(realNode, mappedClass);
        }

        Preconditions.checkNotNull(actionTypeObject, "actionTypeObject");

        if (actionTypeObject instanceof MAPREDUCE) {
            destination.setMapReduce((MAPREDUCE) actionTypeObject);
        }
        else if (actionTypeObject instanceof PIG) {
            destination.setPig((PIG) actionTypeObject);
        }
        else if (actionTypeObject instanceof SUBWORKFLOW) {
            destination.setSubWorkflow((SUBWORKFLOW) actionTypeObject);
        }
        else if (actionTypeObject instanceof FS) {
            destination.setFs((FS) actionTypeObject);
        }
        else if (actionTypeObject instanceof JAVA) {
            destination.setJava((JAVA) actionTypeObject);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.email.ACTION) {
            setEmail((org.apache.oozie.fluentjob.api.generated.action.email.ACTION) actionTypeObject, destination);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.distcp.ACTION) {
            setDistcp((org.apache.oozie.fluentjob.api.generated.action.distcp.ACTION) actionTypeObject, destination);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.git.ACTION) {
            setGit((org.apache.oozie.fluentjob.api.generated.action.git.ACTION) actionTypeObject, destination);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.hive.ACTION) {
            setHive((org.apache.oozie.fluentjob.api.generated.action.hive.ACTION) actionTypeObject, destination);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION) {
            setHive2((org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION) actionTypeObject, destination);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.shell.ACTION) {
            setShell((org.apache.oozie.fluentjob.api.generated.action.shell.ACTION) actionTypeObject, destination);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.spark.ACTION) {
            setSpark((org.apache.oozie.fluentjob.api.generated.action.spark.ACTION) actionTypeObject, destination);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION) {
            setSqoop((org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION) actionTypeObject, destination);
        }
        else if (actionTypeObject instanceof org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION) {
            setSsh((org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION) actionTypeObject, destination);
        }
    }

    private void setEmail(final org.apache.oozie.fluentjob.api.generated.action.email.ACTION source, final ACTION destination) {
        final JAXBElement<?> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.email.ObjectFactory().createEmail(source);
        destination.setOther(jaxbElement);
    }

    private void setDistcp(final org.apache.oozie.fluentjob.api.generated.action.distcp.ACTION source, final ACTION destination) {
        final JAXBElement<org.apache.oozie.fluentjob.api.generated.action.distcp.ACTION> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.distcp.ObjectFactory().createDistcp(source);

        destination.setOther(jaxbElement);
    }

    private void setGit(final org.apache.oozie.fluentjob.api.generated.action.git.ACTION source, final ACTION destination) {
        final JAXBElement<org.apache.oozie.fluentjob.api.generated.action.git.ACTION> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.git.ObjectFactory().createGit(source);

        destination.setOther(jaxbElement);
    }

    private void setHive(final org.apache.oozie.fluentjob.api.generated.action.hive.ACTION source, final ACTION destination) {
        final JAXBElement<org.apache.oozie.fluentjob.api.generated.action.hive.ACTION> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.hive.ObjectFactory().createHive(source);

        destination.setOther(jaxbElement);
    }

    private void setHive2(final org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION source, final ACTION destination) {
        final JAXBElement<org.apache.oozie.fluentjob.api.generated.action.hive2.ACTION> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.hive2.ObjectFactory().createHive2(source);

        destination.setOther(jaxbElement);
    }

    private void setJava(final JAVA source, final ACTION destination) {
        final JAXBElement<JAVA> jaxbElement =
                new ObjectFactory().createJava(source);

        destination.setOther(jaxbElement);
    }

    private void setPig(final PIG source, final ACTION destination) {
        final JAXBElement<PIG> jaxbElement =
                new ObjectFactory().createPig(source);

        destination.setOther(jaxbElement);
    }

    private void setShell(final org.apache.oozie.fluentjob.api.generated.action.shell.ACTION source, final ACTION destination) {
        final JAXBElement<org.apache.oozie.fluentjob.api.generated.action.shell.ACTION> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.shell.ObjectFactory().createShell(source);

        destination.setOther(jaxbElement);
    }

    private void setSpark(final org.apache.oozie.fluentjob.api.generated.action.spark.ACTION source, final ACTION destination) {
        final JAXBElement<org.apache.oozie.fluentjob.api.generated.action.spark.ACTION> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.spark.ObjectFactory().createSpark(source);

        destination.setOther(jaxbElement);
    }

    private void setSqoop(final org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION source, final ACTION destination) {
        final JAXBElement<org.apache.oozie.fluentjob.api.generated.action.sqoop.ACTION> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.sqoop.ObjectFactory().createSqoop(source);

        destination.setOther(jaxbElement);
    }

    private void setSsh(final org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION source, final ACTION destination) {
        final JAXBElement<org.apache.oozie.fluentjob.api.generated.action.ssh.ACTION> jaxbElement =
                new org.apache.oozie.fluentjob.api.generated.action.ssh.ObjectFactory().createSsh(source);

        destination.setOther(jaxbElement);
    }
}
