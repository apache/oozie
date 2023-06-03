import React from 'react';
import { connect } from 'react-redux';
import { fetchCompletedWorkflows } from '../actions/oozie/workflows';
import { getCompletedPagination, getCompletedWorkflows, isCompletedFetching } from "./reducer";
import { Icon, Table, Badge, Button } from 'antd';
import { readableDuration } from '../utils/time';

class WorkflowCompleted extends React.Component {
  constructor(props) {
    super(props);

    this.state = {
      selectedRows: [],
    };
  };

  onSelectChange = (selectedRows) => {
    this.setState({ selectedRows });
  };

  componentDidMount() {
    this.props.fetchWorkflows();
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

    const columns = [
      {
        title: 'Completed',
        dataIndex: 'endTime',
        key: 'completion'
      },
      {
        title: 'Status',
        dataIndex: 'status',
        key: 'status',
        width: 150,
        render: (text) => <span><Badge status={text === "SUCCEEDED" ? "success" : "error"} text={text}/></span>
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
        title: 'Duration',
        dataIndex: 'endTime',
        key: 'duration',
        render: (endTime, wf) => `${readableDuration(Date.parse(endTime) - Date.parse(wf.startTime))}`
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
        render: (parentId) => <span><a href={`#coordinators/${parentId}`}><Icon type="folder" style={{ fontSize: 16 }}/></a></span>
      },
      {
        title: 'Action',
        dataIndex: 'status',
        key: 'action',
        align: 'center',
        render: (status, wf) => <span className="workflow-op">
          <a href={`#workflows/${wf.id}/rerun`}><Icon type="reload" style={{ fontSize: 16, color: '#52c41a' }}/></a>
        </span>
      }
    ];

    return (
      <div style={{ padding: 24, background: '#fff', minHeight: 360 }}>
        <div style={{ marginBottom: 16 }}>
          <Button.Group size="large">
            <Button type="primary" disabled={!hasSelected} icon="play-circle">Rerun</Button>
          </Button.Group>
          <span style={{ marginLeft: 8 }}>{hasSelected ? `Selected ${selectedRows.length} items` : ''}</span>
        </div>
        <Table rowKey={wf => wf.id} dataSource={workflows}
               pagination={pagination} columns={columns}
               rowSelection={rowSelection} loading={isFetching} onChange={this.handleTableChange} />
      </div>
    );
  }
}

export default connect(
  state => ({
    workflows: getCompletedWorkflows(state),
    isFetching: isCompletedFetching(state),
    pagination: getCompletedPagination(state)
  }),
  dispatch => ({
    fetchWorkflows: (current, size) => dispatch(fetchCompletedWorkflows(current, size))
  }),
)(WorkflowCompleted);
