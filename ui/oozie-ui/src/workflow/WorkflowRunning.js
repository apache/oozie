import React from 'react';
import { connect } from "react-redux";
import { fetchRunningWorkflows } from "../actions/oozie/workflows";
import { getRunningPagination, getRunningWorkflows, isRunningFetching } from "./reducer";
import { Icon, Table, Badge, Divider, Button } from 'antd';

class WorkflowRunning extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      collapsed: false,
      selectedRows: []
    };
  };

  onSelectChange = (selectedRows) => {
    this.setState({ selectedRows });
  };

  componentDidMount() {
    const { pagination } = this.props;

    this.props.fetchWorkflows(pagination.current, pagination.size);

    this.interval = setInterval(() => this.props.fetchWorkflows(pagination.current, pagination.size), 30000);
  };

  componentWillUnmount() {
    clearInterval(this.interval);
  };

  handleTableChange = (page) => {
    this.props.fetchWorkflows(page.current, page.pageSize);
  };

  render() {
    const { selectedRows } = this.state;
    const { workflows, isFetching, pagination } = this.props;

    const rowSelection = {
      selectedRows,
      onChange: this.onSelectChange
    };

    const hasSelected = selectedRows.length > 0;
    const selected = workflows.filter(a => selectedRows.includes(a.id));
    const hasSuspended = selected.filter(a => a.status === "SUSPENDED").length > 0;
    const hasRunning = selected.filter(a => a.status === "RUNNING").length > 0;

    const columns = [
      {
        title: 'Submitted',
        dataIndex: 'startTime',
        key: 'submission'
      },
      {
        title: 'Status',
        dataIndex: 'status',
        key: 'status',
        width: 150,
        render: (text) => <span><Badge status={text === "RUNNING" ? "processing" : "warning"} text={text}/></span>
      },
      {
        title: 'Name',
        dataIndex: 'appName',
        key: 'name'
      },
      {
        title: 'User',
        dataIndex: 'user',
        key: 'user',
        width: 70,
      },
      {
        title: 'Last Modified',
        dataIndex: 'lastModTime',
        key: 'lastModified'
      },
      {
        title: 'ID',
        dataIndex: 'id',
        key: 'id',
        render: (text) => <span><a href={`#workflows/${text}`}>{text}</a></span>
      },
      {
        title: 'Parent',
        dataIndex: 'parentId',
        key: 'parent',
        width: 80,
        align: 'center',
        render: (text) => <span><a href={`#coordinators/${text}`}><Icon type="folder" style={{ fontSize: 16 }}/></a></span>
      },
      {
        title: 'Action',
        dataIndex: 'status',
        key: 'action',
        width: 100,
        align: 'center',
        render: (status, row) => <span className="workflow-op">
          <a href={status === "RUNNING" ? `#workflows/${row.id}/suspend` : `#workflows/${row.id}/resume`}>
            <Icon type={status === "RUNNING" ? "pause-circle" : "play-circle"}
                  style={{ fontSize: 16, color: status === "RUNNING" ? '#faad14' : '#08c' }}/></a>

          <Divider type="vertical"/>
          <a href={`#workflows/${row.id}/kill`}><Icon type="close-circle" style={{ fontSize: 16, color: '#f5222d' }}/></a>
        </span>
      }
    ];

    return (
      <div style={{ padding: 24, background: '#fff', minHeight: 360 }}>
        <div style={{ marginBottom: 16 }}>
          <Button.Group size="large">
            <Button type="primary" disabled={!hasSelected || hasRunning} icon="play-circle">Resume</Button>
            <Button type="primary" disabled={!hasSelected || hasSuspended} icon="pause-circle">Suspend</Button>
            <Button type="danger" disabled={!hasSelected} icon="close-circle">Kill</Button>
          </Button.Group>
          <span style={{ marginLeft: 8 }}>{hasSelected ? `Selected ${selectedRows.length} items` : ''}</span>
        </div>
        <Table rowKey={wf => wf.id} dataSource={workflows}
               pagination={pagination} columns={columns}
               rowSelection={rowSelection} loading={isFetching}
               onChange={this.handleTableChange} />
      </div>
    );
  }
}

export default connect(
  state => ({
    workflows: getRunningWorkflows(state),
    isFetching: isRunningFetching(state),
    pagination: getRunningPagination(state)
  }),
  dispatch => ({
    fetchWorkflows: (current, size) => dispatch(fetchRunningWorkflows(current, size))
  }),
)(WorkflowRunning);
