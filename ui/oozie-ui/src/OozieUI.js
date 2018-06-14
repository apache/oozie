import React, { Component } from 'react';
import { Layout, Menu, Icon } from 'antd';
import WorkflowList from './workflows/WorkflowList';
import './OozieUI.css';

const { Header, Footer, Sider } = Layout;

class OozieUI extends Component {
  state = {
    collapsed: false,
  };

  toggle = () => {
    this.setState({
      collapsed: !this.state.collapsed,
    });
  };

  render() {
    return (
      <div className="App">
        <Layout>
          <Sider trigger={null} collapsible collapsed={this.state.collapsed}
                 style={{ overflow: 'auto', height: '100vh', position: 'fixed', left:0 }}>
            <div className="logo" />
            <Menu theme="dark" mode="inline" defaultSelectedKeys={['2']}>
              <Menu.Item key="2">
                <Icon type="file-word" />
                <span>Workflows</span>
              </Menu.Item>
              <Menu.Item key="1">
                <Icon type="folder" />
                <span>Coordinators</span>
              </Menu.Item>
              <Menu.Item key="3">
                <Icon type="book" />
                <span>Bundles</span>
              </Menu.Item>
            </Menu>
          </Sider>
          <Layout style={{ marginLeft: this.state.collapsed ? 80 : 200 }}>
            <Header style={{ background: '#fff', padding: 0 }}>
              <Icon
                className="trigger"
                type={this.state.collapsed ? 'menu-unfold' : 'menu-fold'}
                onClick={this.toggle}
              />
            </Header>
            <WorkflowList />
            <Footer style={{ textAlign: 'center' }}>
              Apache Oozie © 2018
            </Footer>
          </Layout>
        </Layout>
      </div>
    );
  }
}

export default OozieUI;
