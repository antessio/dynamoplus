import React from 'react'
// import { Card, Icon,Avatar, Badge, Collapse, Row, Col,Descriptions } from 'antd';
import {Tree, Icon } from 'antd';
import { Link } from "react-router-dom";


const { TreeNode } = Tree;


// import { Link } from "react-router-dom";
// const { Panel } = Collapse;
export default (props)=>{
    const documents = props.documents
    const collection = props.collection
    // const isActive=Object.keys(document).filter(k=>k==='active').map(k=>document[k]).join(",")
    // const color = isActive=="true"?"green":"red";
    return (
        <Tree
    showIcon
    switcherIcon={<Icon type="database" />}>
        {documents && documents.map(
            d=><TreeNode key={d[collection.idKey]} 
                    icon={<Icon type="container" />} 
                    title={d[collection.idKey]}>
                    {Object.keys(d).map(k=>renderDocumentFields(k,d[k]))}
                </TreeNode>

        )}
    </Tree>
     )
}
const renderDocumentFields=(fieldKey,fieldValue)=>{
    if (Array.isArray(fieldValue)){
        return <TreeNode 
        key={fieldKey}
        icon={<Icon type="switcher"/>}
        title={fieldKey}>
        {fieldValue.map((subItem,i)=><TreeNode 
            key={fieldKey+"_"+i} 
                    icon={<Icon type="switcher" />} 
                    title={i}>
                        {renderDocumentFields(fieldKey,subItem)}
                    </TreeNode>)}
        </TreeNode>
    }else if(typeof fieldValue == "object"){
            return (<TreeNode key={fieldKey}
            icon={<Icon type="switcher" />}
            title={fieldKey}>
                {
                    Object.keys(fieldValue).map(k=>
                        renderDocumentFields(k,fieldValue[k])
                    )
                    }
            </TreeNode>
            )
            
    }else if(fieldValue){
    return (<TreeNode 
            key={fieldKey} 
                    icon={<Icon type="select" />} 
                    title={fieldKey+" : "+fieldValue}>
                </TreeNode>)
    }else{
        return null;
    }
}