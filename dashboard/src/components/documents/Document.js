import React from 'react'
import { Card, Icon,Avatar, Badge, Collapse } from 'antd';

const { Panel } = Collapse;


const { Meta } = Card;

export default (props)=>{
    const document = props.document
    return (    
    <Card 
    actions={[
        <Icon type="setting" key="setting" />,
        <Icon type="edit" key="edit" />,
        <Icon type="ellipsis" key="ellipsis" />,
      ]}
        >
    <Meta title={document.name}/>
    <br/>
    <Badge dot color={document.active==true?"green":"red"}>
        <Avatar style={{ backgroundColor: "gray", verticalAlign: 'middle' }} size="large">
            {document.name[0]}
        </Avatar>
        <Collapse bordered={false} defaultActiveKey={[]}>
            <Panel header="Show definition" key="1"> 
            <code>{JSON.stringify(document, null, 2)}</code>
            </Panel>
        </Collapse>
    </Badge>
    </Card>    )
}