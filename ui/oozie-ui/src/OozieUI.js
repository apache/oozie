import React from 'react';
import { Layout, Menu, Icon } from 'antd';
import Workflow from './workflow/Workflow';
import './OozieUI.css';

const { Footer, Sider } = Layout;

const collapsed_logos = {
  true: "/img/logo.png",
  false: "/img/logo-full.png"
};

class OozieUI extends React.Component {
  state = {
    collapsed: false,
    logo: collapsed_logos[false]
  };

  toggle = () => {
    const collapsed = !this.state.collapsed;
    this.setState({
      collapsed: collapsed,
      logo: collapsed_logos[collapsed]
    });
  };

  render() {
    const { logo } = this.state;

    return (
      <div className="App">
        <Layout>
          <Sider collapsible
                 collapsed={this.state.collapsed}
                 theme="light"
                 onCollapse={this.toggle}>
            <div className="logo" align="center"><img id="logo" src={logo} alt="Apache Oozie" height="40px"/></div>
            <Menu mode="inline" style={{ minHeight: '100vh' }} defaultSelectedKeys={['2']}>
              <Menu.Item key="1">
                <Icon type="dashboard" />
                <span>Dashboard</span>
              </Menu.Item>
              <Menu.Item key="2">
                <Icon type="file-word" />
                <span>Workflows</span>
              </Menu.Item>
              <Menu.Item key="3">
                <Icon type="folder" />
                <span>Coordinators</span>
              </Menu.Item>
              <Menu.Item key="4">
                <Icon type="book" />
                <span>Bundles</span>
              </Menu.Item>
            </Menu>
          </Sider>
          <Layout>
            <Workflow />
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
