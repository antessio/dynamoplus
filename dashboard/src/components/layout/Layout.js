import React from "react";
import  { Layout, Icon } from 'antd';

import './Layout.css'

import Logo from '../logo/Logo'
import Menu from '../menu/Menu'

class MainLayout extends React.Component {
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
      <Layout style={{minHeight: "100vh"}}>
        <Layout.Sider trigger={null} collapsible collapsed={this.state.collapsed}>
            <Logo />
            <Menu />
        </Layout.Sider>
        <Layout>
          <Layout.Header style={{ background: '#fff', padding: 0 }}>
            <Icon
              className="trigger"
              type={this.state.collapsed ? 'menu-unfold' : 'menu-fold'}
              onClick={this.toggle}
            />
          </Layout.Header>
          <Layout.Content
            style={{
              margin: '24px 16px',
              padding: 24,
              background: '#fff'
            }}
          >
            {this.props.children}
          </Layout.Content>
        </Layout>
      </Layout>
    );
  }
}

export default MainLayout;