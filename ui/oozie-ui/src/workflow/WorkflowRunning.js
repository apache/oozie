import React from 'react';
import { Icon, Table, Badge, Progress, Divider, Button } from 'antd';

class WorkflowRunning extends React.Component {
  state = {
    collapsed: false,
    selectedRows: [],
    dataSource: []
  };

  onSelectChange = (selectedRows) => {
    this.setState({ selectedRows });
  };

  componentDidMount = () => {
    var dataSource = [];
    for (var i = 0; i < 5; i++) {
      dataSource.push({
        key: i,
        submission: 'Fri, 08 Jun 2018 22:30:00',
        status: Math.random() > 0.4 ? 'RUNNING' : 'SUSPENDED',
        name: 'ROI-Calculation-' + i,
        progress: Math.round(Math.random() * 100),
        submitter: 'john.white',
        lastModified: '2h:30m:32s',
        id: '00000' + i + '-2192983989178749-oozie-oozi-W',
        parent: '00000' + i + '-2192983989178749-oozie-oozi-C',
      });
    }

    this.setState({ dataSource: dataSource});
  };

  render() {
    const { selectedRows, dataSource } = this.state;

    const rowSelection = {
      selectedRows,
      onChange: this.onSelectChange
    };

    const hasSelected = selectedRows.length > 0;
    const selected = dataSource.filter(a => selectedRows.includes(a.key));
    const hasSuspended = selected.filter(a => a.status === "SUSPENDED").length > 0;
    const hasRunning = selected.filter(a => a.status === "RUNNING").length > 0;

    const columns = [
      {
        title: 'Submitted',
        dataIndex: 'submission',
        key: 'submission'
      },
      {
        title: 'Status',
        dataIndex: 'status',
        key: 'status',
        render: (text) => <span><Badge status={text === "RUNNING" ? "processing" : "warning"} text={text}/></span>
      },
      {
        title: 'Name',
        dataIndex: 'name',
        key: 'name'
      },
      {
        title: 'Progress',
        dataIndex: 'progress',
        key: 'progress',
        render: (percent) => <span><Progress size="small" status="active" percent={percent} /></span>
      },
      {
        title: 'Submitter',
        dataIndex: 'submitter',
        key: 'submitter'
      },
      {
        title: 'Last Modified',
        dataIndex: 'lastModified',
        key: 'lastModified'
      },
      {
        title: 'ID',
        dataIndex: 'id',
        key: 'id',
        render: (text) => <span><a href={`#workflows/${text}`}>{text}</a></span>
      },
      {
        title: 'Coordinator',
        dataIndex: 'parent',
        key: 'parent',
        align: 'center',
        render: (text) => <span><a href={`#coordinators/${text}`}><Icon type="folder" style={{ fontSize: 16 }}/></a></span>
      },
      {
        title: 'Action',
        dataIndex: 'status',
        key: 'action',
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
        <Table dataSource={dataSource} columns={columns} rowSelection={rowSelection} />
      </div>
    );
  }
}

export default WorkflowRunning;
