import React from 'react';
import { Icon, Table, Badge, Button } from 'antd';

class WorkflowCompleted extends React.Component {
  state = {
    selectedRows: [],
    dataSource: []
  };

  onSelectChange = (selectedRows) => {
    this.setState({ selectedRows });
  };

  componentDidMount = () => {
    var dataSource = [];
    for (var i = 0; i < 102; i++) {
      dataSource.push({
        key: i,
        completion: 'Fri, 05 Jun 2017 22:30:00',
        status: Math.random() > 0.4 ? 'SUCCEEDED' : 'KILLED',
        name: 'Monthly-Calculation-' + i,
        duration: '1h:30m:25s',
        submitter: 'peter.brown',
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

    const columns = [
      {
        title: 'Completed',
        dataIndex: 'completion',
        key: 'completion'
      },
      {
        title: 'Status',
        dataIndex: 'status',
        key: 'status',
        render: (text) => <span><Badge status={text === "SUCCEEDED" ? "success" : "error"} text={text}/></span>
      },
      {
        title: 'Name',
        dataIndex: 'name',
        key: 'name'
      },
      {
        title: 'Submitter',
        dataIndex: 'submitter',
        key: 'submitter'
      },
      {
        title: 'Duration',
        dataIndex: 'duration',
        key: 'duration'
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
          <a href={`#workflows/${row.id}/rerun`}><Icon type="reload" style={{ fontSize: 16, color: '#52c41a' }}/></a>
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
        <Table dataSource={dataSource} columns={columns} rowSelection={rowSelection} />
      </div>
    );
  }
}

export default WorkflowCompleted;
