import React, { Component } from 'react';
import { Layout, Menu, Icon, Breadcrumb } from 'antd';
import './App.css';

const { Header, Footer, Sider, Content } = Layout;

class App extends Component {
  state = {
    collapsed: false
  };

  toggle = () => {
    this.setState({
      collapsed: !this.state.collapsed,
    });
  }

  render() {
    return (
      <div className="App">
        <Layout>
          <Sider trigger={null} collapsible collapsed={this.state.collapsed}
                 style={{ overflow: 'auto', height: '100vh', position: 'fixed', left:0 }}>
            <div className="logo" />
            <Menu theme="dark" mode="inline" defaultSelectedKeys={['1']}>
              <Menu.Item key="1">
                <Icon type="user" />
                <span>Nav 1</span>
              </Menu.Item>
              <Menu.Item key="2">
                <Icon type="video-camera" />
                <span>Nav 2</span>
              </Menu.Item>
              <Menu.Item key="3">
                <Icon type="upload" />
                <span>Nav 3</span>
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
            <Content style={{ margin: '24px 16px 0' }}>
              <Breadcrumb style={{ margin: '16px 0' }}>
                <Breadcrumb.Item>User</Breadcrumb.Item>
                <Breadcrumb.Item>Bill</Breadcrumb.Item>
              </Breadcrumb>
              <div style={{ padding: 24, background: '#fff', minHeight: 360 }}>
                content
              </div>
            </Content>
            <Footer style={{ textAlign: 'center' }}>
              Apache Oozie © 2018
            </Footer>
          </Layout>
        </Layout>
      </div>
    );
  }
}

export default App;
