import React from 'react'
import { Card, Icon,Avatar, Badge, Collapse, Row, Col,Descriptions } from 'antd';



import { Link } from "react-router-dom";
const { Panel } = Collapse;
export default (props)=>{
    const document = props.document
    const isActive=Object.keys(document).filter(k=>k==='active').map(k=>document[k]).join(",")
    const color = isActive=="true"?"green":"red";
    return (    
    <Card 
    actions={[
        <Link to={"/documents/"+document.name}><Icon type="read" key="read" /></Link>,
        <Icon type="edit" key="edit" />,
        <Icon type="search" key="query" />,
      ]}
        >
    
        <Row>
            <Col xs={4} >
            <Badge dot style={{ backgroundColor: color, color: "white", boxShadow: '0 0 0 1px #d9d9d9 inset' }}>
                <Avatar style={{ backgroundColor: "gray", verticalAlign: 'middle' }} size="large">
                    {document.name[0]}
                </Avatar>
            </Badge>
            </Col>
            <Col xs={20}>
                <span>{document.name}</span>
            </Col>
        </Row>
    
    <br/>
    <Descriptions
    column={{ xxl: 1, xl: 1, lg: 1, md: 1, sm: 1, xs: 1 }}>
        <Descriptions.Item label="Created at ">{document.creation_date_time}</Descriptions.Item>
        <Descriptions.Item label="Key">{document.idKey}</Descriptions.Item>
        <Descriptions.Item label="Sort key ">{document.orderingKey}</Descriptions.Item>
    </Descriptions>
    <Link to={"/indexes/"+document.name}>
            <Icon type="search" />
            <span >Indexes</span>
    </Link>
    <Collapse bordered={false} defaultActiveKey={[]}>
        <Panel header="Show definition" key="1"> 
        <code>{JSON.stringify(document, null, 2)}</code>
        </Panel>
    </Collapse>
    </Card>    )
}