import React from 'react';
import { Layout, Breadcrumb } from 'antd';
import WorkflowRunning from './WorkflowRunning';
import WorkflowCompleted from "./WorkflowCompleted";

const { Content } = Layout;

class WorkflowList extends React.Component {
  render() {
    return (
      <Content style={{ margin: '24px 16px 0' }}>
        <Breadcrumb style={{ margin: '16px 0' }}>
          <Breadcrumb.Item>Workflows</Breadcrumb.Item>
          <Breadcrumb.Item>Running</Breadcrumb.Item>
        </Breadcrumb>
        <WorkflowRunning/>
        <Breadcrumb style={{ margin: '16px 0' }}>
          <Breadcrumb.Item>Workflows</Breadcrumb.Item>
          <Breadcrumb.Item>Completed</Breadcrumb.Item>
        </Breadcrumb>
        <WorkflowCompleted/>
      </Content>
    );
  }
}

export default WorkflowList;
